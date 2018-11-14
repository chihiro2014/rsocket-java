/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import io.rsocket.Frame;
import io.rsocket.RSocketFactory;
import io.rsocket.test.PingHandler;
import io.rsocket.transport.netty.server.TcpServerTransport;
import reactor.netty.tcp.TcpServer;

public final class TcpPongServer {

  public static void main(String... args) {
//    SSLContext context = SSLContext.getInstance("TLSv1.3");
//    SSLContext.setDefault(context);
//    SecureRandom random = new SecureRandom();
//    SelfSignedCertificate ssc = new SelfSignedCertificate("netifi.io", random, 1024);
//    final SslContext sslServer =
//        SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
//            .sslProvider(SslProvider.OPENSSL_REFCNT)
//            .build();
    TcpServer tcpServer = TcpServer.create().port(7878);//.secure(sslServer);

    RSocketFactory.receive()
        .frameDecoder(Frame::retain)
        .acceptor(new PingHandler())
        .transport(TcpServerTransport.create(tcpServer))
        .start()
        .block()
        .onClose()
        .block();
  }
}
