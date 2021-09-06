/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.rabbitmqgw;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;
import org.apache.pulsar.proxy.server.ProxyConfiguration;

/** Initialize service channel handlers. */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

  public static final String TLS_HANDLER = "tls";
  private final GatewayService gatewayService;
  private final boolean enableTls;
  private final boolean tlsEnabledWithKeyStore;

  private SslContextAutoRefreshBuilder<SslContext> serverSslCtxRefresher;
  private NettySSLContextAutoRefreshBuilder serverSSLContextAutoRefreshBuilder;

  public ServiceChannelInitializer(
      GatewayService gatewayService, ProxyConfiguration serviceConfig, boolean enableTls) {
    super();
    this.gatewayService = gatewayService;
    this.enableTls = enableTls;
    this.tlsEnabledWithKeyStore = serviceConfig.isTlsEnabledWithKeyStore();

    if (enableTls) {
      if (tlsEnabledWithKeyStore) {
        serverSSLContextAutoRefreshBuilder =
            new NettySSLContextAutoRefreshBuilder(
                serviceConfig.getTlsProvider(),
                serviceConfig.getTlsKeyStoreType(),
                serviceConfig.getTlsKeyStore(),
                serviceConfig.getTlsKeyStorePassword(),
                serviceConfig.isTlsAllowInsecureConnection(),
                serviceConfig.getTlsTrustStoreType(),
                serviceConfig.getTlsTrustStore(),
                serviceConfig.getTlsTrustStorePassword(),
                serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                serviceConfig.getTlsCiphers(),
                serviceConfig.getTlsProtocols(),
                serviceConfig.getTlsCertRefreshCheckDurationSec());
      } else {
        serverSslCtxRefresher =
            new NettyServerSslContextBuilder(
                serviceConfig.isTlsAllowInsecureConnection(),
                serviceConfig.getTlsTrustCertsFilePath(),
                serviceConfig.getTlsCertificateFilePath(),
                serviceConfig.getTlsKeyFilePath(),
                serviceConfig.getTlsCiphers(),
                serviceConfig.getTlsProtocols(),
                serviceConfig.isTlsRequireTrustedClientCertOnConnect(),
                serviceConfig.getTlsCertRefreshCheckDurationSec());
      }
    } else {
      this.serverSslCtxRefresher = null;
    }
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    if (serverSslCtxRefresher != null && this.enableTls) {
      SslContext sslContext = serverSslCtxRefresher.get();
      if (sslContext != null) {
        ch.pipeline().addLast(TLS_HANDLER, sslContext.newHandler(ch.alloc()));
      }
    } else if (this.tlsEnabledWithKeyStore && serverSSLContextAutoRefreshBuilder != null) {
      ch.pipeline()
          .addLast(
              TLS_HANDLER,
              new SslHandler(serverSSLContextAutoRefreshBuilder.get().createSSLEngine()));
    }
    ch.pipeline().addLast("encoder", new AMQDataBlockEncoder());
    ch.pipeline().addLast("handler", new GatewayConnection(gatewayService));
  }
}
