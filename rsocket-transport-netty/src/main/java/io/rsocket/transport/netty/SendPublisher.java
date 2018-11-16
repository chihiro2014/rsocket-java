package io.rsocket.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

class SendPublisher extends Flux<Frame> {

  private static final AtomicLongFieldUpdater<SendPublisher> REQUESTED_UPSTREAM =
      AtomicLongFieldUpdater.newUpdater(SendPublisher.class, "requestedUpstream");

  private static final AtomicLongFieldUpdater<SendPublisher> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(SendPublisher.class, "requested");

  private static final int MAX_SIZE = Queues.SMALL_BUFFER_SIZE;
  private static final int REFILL_SIZE = MAX_SIZE / 2;
  private final Publisher<Frame> source;
  private final Channel channel;
  private final Scheduler scheduler;
  private final Queue<ByteBuf> queue;
  private final AtomicBoolean terminated = new AtomicBoolean();
  private final AtomicBoolean completed = new AtomicBoolean();
  private final AtomicInteger WIP = new AtomicInteger();

  @SuppressWarnings("unused")
  private volatile long requested;

  @SuppressWarnings("unused")
  private volatile long requestedUpstream;

  public SendPublisher(Publisher<Frame> source, Channel channel, Scheduler scheduler) {
    this.source = source;
    this.channel = channel;
    this.scheduler = scheduler;
    this.queue = Queues.<ByteBuf>small().get();
  }

  @Override
  public void subscribe(CoreSubscriber<? super Frame> destination) {
    InnerSubscriber innerSubscriber = new InnerSubscriber(destination);
    InnerSubscription innerSubscription = new InnerSubscription(innerSubscriber);
    destination.onSubscribe(innerSubscription);
    source.subscribe(innerSubscriber);
  }

  private class InnerSubscriber implements Subscriber<Frame> {
    final CoreSubscriber<? super Frame> destination;
    volatile Subscription s;
    private AtomicBoolean pendingFlush = new AtomicBoolean();

    public InnerSubscriber(CoreSubscriber<? super Frame> destination) {
      this.destination = destination;
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.s = s;
      Operators.addCap(REQUESTED_UPSTREAM, SendPublisher.this, MAX_SIZE);
      s.request(MAX_SIZE);
      tryDrain();
    }

    @Override
    public void onNext(Frame t) {
      if (!queue.offer(t.content().retain())) {
        throw new IllegalStateException("missing back pressure");
      }
      tryDrain();
    }

    @Override
    public void onError(Throwable t) {
      if (terminated.compareAndSet(false, true)) {
        s.cancel();
        destination.onError(t);
      }
    }

    @Override
    public void onComplete() {
      if (completed.compareAndSet(false, true)) {
        tryDrain();
      }
    }

    private void tryDrain() {
      if (WIP.getAndIncrement() == 0) {
        try {
          drain();
        } catch (Throwable t) {
          onError(t);
        }
      }
    }

    private void tryRequestMoreUpstream() {
      long ru = requestedUpstream;
      if (ru <= REFILL_SIZE) {
        long u = MAX_SIZE - ru;
        if (s != null) {
          Operators.addCap(REQUESTED_UPSTREAM, SendPublisher.this, u);
          s.request(u);
        }
      }
    }

    private void flush() {
      try {
        channel.flush();
        pendingFlush.set(false);
        if (completed.get() && queue.isEmpty() && !terminated.get() && !pendingFlush.get()) {
          destination.onComplete();
        }
      } catch (Throwable t) {
        onError(t);
      }
    }

    private void drain() {
      boolean scheduleFlush;
      for (; ; ) {
        scheduleFlush = false;
        WIP.decrementAndGet();

        long r = Math.min(requested, requestedUpstream);
        while (!queue.isEmpty() && r-- > 0) {
          ByteBuf poll = queue.poll();
          if (poll != null) {
            if (requested != Long.MAX_VALUE) {
              REQUESTED.decrementAndGet(SendPublisher.this);
            }
            REQUESTED_UPSTREAM.decrementAndGet(SendPublisher.this);

            try {
              if (channel.isWritable()) {
                channel.write(poll);
                scheduleFlush = true;
              } else {
                channel.writeAndFlush(poll);
              }
            } finally {
              ReferenceCountUtil.release(poll);
            }
          }
        }

        tryRequestMoreUpstream();

        if (scheduleFlush) {
          pendingFlush.compareAndSet(false, true);
          scheduler.schedule(this::flush);
        }

        if (completed.get() && queue.isEmpty() && !terminated.get() && !pendingFlush.get()) {
          System.out.println("HERE<><><><><><><><><>>>>>>>");
         // destination.onComplete();
        }

        if (WIP.get() == 0 || terminated.get()) {
          break;
        }
      }
    }
  }

  private class InnerSubscription implements Subscription {
    private final InnerSubscriber innerSubscriber;

    public InnerSubscription(InnerSubscriber innerSubscriber) {
      this.innerSubscriber = innerSubscriber;
    }

    @Override
    public void request(long n) {
      Operators.addCap(SendPublisher.REQUESTED, SendPublisher.this, n);
      innerSubscriber.tryDrain();
    }

    @Override
    public void cancel() {
      terminated.set(true);
    }
  }
}
