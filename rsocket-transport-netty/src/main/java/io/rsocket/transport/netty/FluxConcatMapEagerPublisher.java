package io.rsocket.transport.netty;

import io.netty.util.internal.shaded.org.jctools.queues.SpscLinkedQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

public class FluxConcatMapEagerPublisher<T, R> extends Flux<R> {
  final Publisher<T> source;

  final Function<? super T, ? extends Publisher<? extends R>> mapper;

  final int maxConcurrency;

  final int prefetch;

  final ErrorMode errorMode;

  public FluxConcatMapEagerPublisher(
      Publisher<T> source,
      Function<? super T, ? extends Publisher<? extends R>> mapper,
      int maxConcurrency,
      int prefetch,
      ErrorMode errorMode) {
    this.source = source;
    this.mapper = mapper;
    this.maxConcurrency = maxConcurrency;
    this.prefetch = prefetch;
    this.errorMode = errorMode;
  }

  @Override
  public void subscribe(CoreSubscriber<? super R> actual) {
    source.subscribe(
        new ConcatMapEagerDelayErrorSubscriber<T, R>(
            actual, mapper, maxConcurrency, prefetch, errorMode));
  }

  public enum ErrorMode {
    /** Report the error immediately, cancelling the active inner source. */
    IMMEDIATE,
    /** Report error after an inner source terminated. */
    BOUNDARY,
    /** Report the error after all sources terminated. */
    END
  }

  static final class ConcatMapEagerDelayErrorSubscriber<T, R> extends AtomicInteger
      implements CoreSubscriber<T>, Subscription, InnerQueuedSubscriberSupport<R> {

    static final AtomicReferenceFieldUpdater<ConcatMapEagerDelayErrorSubscriber, Throwable> ERROR =
        AtomicReferenceFieldUpdater.newUpdater(
            ConcatMapEagerDelayErrorSubscriber.class, Throwable.class, "error");
    private static final long serialVersionUID = -4255299542215038287L;
    final Subscriber<? super R> actual;
    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    final int maxConcurrency;
    final int prefetch;
    final AtomicLong requested;
    final SpscLinkedQueue<InnerQueuedSubscriber<R>> subscribers;
    final ErrorMode errorMode;
    volatile Throwable error;
    Subscription s;
    volatile boolean cancelled;

    volatile boolean done;

    volatile InnerQueuedSubscriber<R> current;

    ConcatMapEagerDelayErrorSubscriber(
        Subscriber<? super R> actual,
        Function<? super T, ? extends Publisher<? extends R>> mapper,
        int maxConcurrency,
        int prefetch,
        ErrorMode errorMode) {
      this.actual = actual;
      this.mapper = mapper;
      this.maxConcurrency = maxConcurrency;
      this.prefetch = prefetch;
      this.subscribers = new SpscLinkedQueue<>();
      this.requested = new AtomicLong();
      this.errorMode = errorMode;
    }

    public static long add(AtomicLong requested, long n) {
      for (; ; ) {
        long r = requested.get();
        if (r == Long.MAX_VALUE) {
          return Long.MAX_VALUE;
        }
        long u = addCap(r, n);
        if (requested.compareAndSet(r, u)) {
          return r;
        }
      }
    }

    /**
     * Adds two long values and caps the sum at Long.MAX_VALUE.
     *
     * @param a the first value
     * @param b the second value
     * @return the sum capped at Long.MAX_VALUE
     */
    public static long addCap(long a, long b) {
      long u = a + b;
      if (u < 0L) {
        return Long.MAX_VALUE;
      }
      return u;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;

        actual.onSubscribe(this);

        s.request(maxConcurrency == Integer.MAX_VALUE ? Long.MAX_VALUE : maxConcurrency);
      }
    }

    @Override
    public void onNext(T t) {
      Publisher<? extends R> p;

      try {
        p = Objects.requireNonNull(mapper.apply(t), "The mapper returned a null Publisher");
      } catch (Throwable ex) {
        Exceptions.throwIfFatal(ex);
        s.cancel();
        onError(ex);
        return;
      }
  
      InnerQueuedSubscriber<R> inner = new InnerQueuedSubscriber<R>(this, prefetch);

      if (cancelled) {
        return;
      }

      subscribers.offer(inner);

      p.subscribe(inner);

      if (cancelled) {
        inner.cancel();
        drainAndCancel();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (Exceptions.addThrowable(ERROR, this, t)) {
        done = true;
        drain();
      } else {
        throw Exceptions.bubble(t);
      }
    }

    @Override
    public void onComplete() {
      done = true;
      drain();
    }

    @Override
    public void cancel() {
      if (cancelled) {
        return;
      }
      cancelled = true;
      s.cancel();

      drainAndCancel();
    }

    void drainAndCancel() {
      if (getAndIncrement() == 0) {
        do {
          cancelAll();
        } while (decrementAndGet() != 0);
      }
    }

    void cancelAll() {
      InnerQueuedSubscriber<R> inner;

      while ((inner = subscribers.poll()) != null) {
        inner.cancel();
      }
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        add(requested, n);
        drain();
      }
    }

    @Override
    public void innerNext(InnerQueuedSubscriber<R> inner, R value) {
      if (inner.queue().offer(value)) {
        drain();
      } else {
        inner.cancel();
        innerError(inner, new Throwable("missing back pressure"));
      }
    }

    @Override
    public void innerError(InnerQueuedSubscriber<R> inner, Throwable e) {
      if (Exceptions.addThrowable(ERROR, this, e)) {
        inner.setDone();
        if (errorMode != ErrorMode.END) {
          s.cancel();
        }
        drain();
      } else {
        throw Exceptions.bubble(e);
      }
    }

    @Override
    public void innerComplete(InnerQueuedSubscriber<R> inner) {
      inner.setDone();
      drain();
    }

    @Override
    public void drain() {
      if (getAndIncrement() != 0) {
        return;
      }

      int missed = 1;
      InnerQueuedSubscriber<R> inner = current;
      Subscriber<? super R> a = actual;
      ErrorMode em = errorMode;

      for (; ; ) {
        long r = requested.get();
        long e = 0L;

        if (inner == null) {

          if (em != ErrorMode.END) {
            Throwable ex = error;
            if (ex != null) {
              cancelAll();

              a.onError(error);
              return;
            }
          }

          boolean outerDone = done;

          inner = subscribers.poll();

          if (outerDone && inner == null) {
            Throwable ex = error;
            if (ex != null) {
              a.onError(ex);
            } else {
              a.onComplete();
            }
            return;
          }

          if (inner != null) {
            current = inner;
          }
        }

        boolean continueNextSource = false;

        if (inner != null) {
          Queue<R> q = inner.queue();
          if (q != null) {
            while (e != r) {
              if (cancelled) {
                cancelAll();
                return;
              }

              if (em == ErrorMode.IMMEDIATE) {
                Throwable ex = error;
                if (ex != null) {
                  current = null;
                  inner.cancel();
                  cancelAll();

                  a.onError(error);
                  return;
                }
              }

              boolean d = inner.isDone();

              R v;

              try {
                v = q.poll();
              } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
                current = null;
                inner.cancel();
                cancelAll();
                a.onError(ex);
                return;
              }

              boolean empty = v == null;

              if (d && empty) {
                inner = null;
                current = null;
                s.request(1);
                continueNextSource = true;
                break;
              }

              if (empty) {
                break;
              }

              a.onNext(v);

              e++;

              inner.requestOne();
            }

            if (e == r) {
              if (cancelled) {
                cancelAll();
                return;
              }

              if (em == ErrorMode.IMMEDIATE) {
                Throwable ex = error;
                if (ex != null) {
                  current = null;
                  inner.cancel();
                  cancelAll();

                  a.onError(error);
                  return;
                }
              }

              boolean d = inner.isDone();

              boolean empty = q.isEmpty();

              if (d && empty) {
                inner = null;
                current = null;
                s.request(1);
                continueNextSource = true;
              }
            }
          }
        }

        if (e != 0L && r != Long.MAX_VALUE) {
          requested.addAndGet(-e);
        }

        if (continueNextSource) {
          continue;
        }

        missed = addAndGet(-missed);
        if (missed == 0) {
          break;
        }
      }
    }
  }
}
