/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.transport.netty;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.internal.UnboundedProcessor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;

import java.util.Objects;

/** An implementation of {@link DuplexConnection} that connects via TCP. */
public final class TcpDuplexConnection implements DuplexConnection {
  
  private final Connection connection;
  private final NettyOutbound outbound;
  
  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} to for managing the server
   */
  public TcpDuplexConnection(Connection connection) {
    this.connection = Objects.requireNonNull(connection, "connection must not be null");
    this.outbound = connection.outbound();
  }
  
  @Override
  public void dispose() {
    connection.dispose();
  }
  
  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }
  
  @Override
  public Mono<Void> onClose() {
    return connection.onDispose();
  }
  
  @Override
  public Flux<Frame> receive() {
    return connection.inbound().receive().map(buf -> Frame.from(buf.retain()));
  }
  
  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    if (frames instanceof UnboundedProcessor) {
      return Flux.from(frames)
                 .map(Frame::content)
                 .flatMapSequential(outbound::sendObject, 256, Integer.MAX_VALUE)
                 .then();
    } else {
      return Flux.from(frames)
                 .map(Frame::content)
                 .concatMap(outbound::sendObject, 256)
                 .then();
    }
  }
  
  @Override
  public Mono<Void> sendOne(Frame frame) {
    return outbound.sendObject(frame.content()).then();
  }
  
}
