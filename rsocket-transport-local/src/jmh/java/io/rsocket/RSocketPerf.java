/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket;

import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.DefaultPayload;
import org.jctools.maps.NonBlockingHashMapLong;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

@BenchmarkMode(Mode.Throughput)
@Fork(
  value = 1,
  jvmArgsAppend = {"-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-Dio.netty.leakDetection.level=DISABLED"}
)
@Warmup(iterations = 10)
@Measurement(iterations = 1000)
@State(Scope.Thread)
public class RSocketPerf {

  @Benchmark
  public void putRemove(Input input) {
      long i = input.supplier.nextStreamId();
      input.map.put(i, input.client);
      input.map.remove(i);
  }

  @Benchmark
  public void requestResponseHello(Input input) {
    Payload block = input.client.requestResponse(Input.HELLO_PAYLOAD).block();
    input.bh.consume(block);
  }

  @Benchmark
  public void requestStreamHello1000(Input input) {
    try {
      input.client.requestStream(Input.HELLO_PAYLOAD).subscribe(input.bh::consume);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  @Benchmark
  public void fireAndForgetHello(Input input) {
    // this is synchronous so we don't need to use a CountdownLatch to wait
    input.client.fireAndForget(Input.HELLO_PAYLOAD).subscribe(input.bh::consume);
  }

  @State(Scope.Benchmark)
  public static class Input {
    static final ByteBuffer HELLO = ByteBuffer.wrap("HELLO".getBytes(StandardCharsets.UTF_8));
    static final Payload HELLO_PAYLOAD = DefaultPayload.create(HELLO);

    public Blackhole bh;
    public RSocket client;
    public ConcurrentHashMap<Long, RSocket> map = new ConcurrentHashMap<>();
    public StreamIdSupplier supplier = StreamIdSupplier.clientSupplier();

    @Setup
    public void setup(Blackhole bh) {
      final LocalServerTransport tranport = LocalServerTransport.create("test-local-server");

      RSocketFactory.receive()
          .acceptor(
              (setup, sendingSocket) -> {
                RSocket rSocket =
                    new RSocket() {
                      @Override
                      public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.empty();
                      }

                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return Mono.just(HELLO_PAYLOAD);
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return Flux.range(1, 1_000).flatMap(i -> requestResponse(payload));
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return Flux.empty();
                      }

                      @Override
                      public Mono<Void> metadataPush(Payload payload) {
                        return Mono.empty();
                      }

                      @Override
                      public void dispose() {}

                      @Override
                      public Mono<Void> onClose() {
                        return Mono.empty();
                      }
                    };

                return Mono.just(rSocket);
              })
          .transport(tranport)
          .start()
          .block();

      client = RSocketFactory.connect().transport(tranport.clientTransport()).start().block();

      this.bh = bh;
    }
  }
}
