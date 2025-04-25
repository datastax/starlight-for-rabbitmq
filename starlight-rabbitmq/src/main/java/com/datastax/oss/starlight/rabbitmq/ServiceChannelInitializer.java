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
package com.datastax.oss.starlight.rabbitmq;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Initialize service channel handlers. */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

  private static final Logger log = LoggerFactory.getLogger(ServiceChannelInitializer.class);

  public static final String TLS_HANDLER = "tls";
  private final GatewayService gatewayService;
  private final boolean enableTls;
  private final boolean tlsEnabledWithKeyStore;

  private PulsarSslFactory sslFactory;

  public ServiceChannelInitializer(
      GatewayService gatewayService, ProxyConfiguration serviceConfig, boolean enableTls) {
    super();
    this.gatewayService = gatewayService;
    this.enableTls = enableTls;
    this.tlsEnabledWithKeyStore = serviceConfig.isTlsEnabledWithKeyStore();

    if (enableTls) {
      PulsarSslConfiguration sslConfiguration = buildSslConfiguration(serviceConfig);
      try {
        this.sslFactory =
            (PulsarSslFactory)
                Class.forName(serviceConfig.getSslFactoryPlugin()).getConstructor().newInstance();
        this.sslFactory.initialize(sslConfiguration);
        this.sslFactory.createInternalSslContext();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected PulsarSslConfiguration buildSslConfiguration(ProxyConfiguration config) {
    return PulsarSslConfiguration.builder()
        .tlsProvider(config.getTlsProvider())
        .tlsKeyStoreType(config.getTlsKeyStoreType())
        .tlsKeyStorePath(config.getTlsKeyStore())
        .tlsKeyStorePassword(config.getTlsKeyStorePassword())
        .tlsTrustStoreType(config.getTlsTrustStoreType())
        .tlsTrustStorePath(config.getTlsTrustStore())
        .tlsTrustStorePassword(config.getTlsTrustStorePassword())
        .tlsCiphers(config.getTlsCiphers())
        .tlsProtocols(config.getTlsProtocols())
        .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
        .tlsCertificateFilePath(config.getTlsCertificateFilePath())
        .tlsKeyFilePath(config.getTlsKeyFilePath())
        .allowInsecureConnection(config.isTlsAllowInsecureConnection())
        .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect())
        .tlsEnabledWithKeystore(config.isTlsEnabledWithKeyStore())
        .tlsCustomParams(config.getSslFactoryPluginParams())
        .authData(null)
        .serverMode(true)
        .build();
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    if (this.enableTls) {
      ch.pipeline()
           .addLast(TLS_HANDLER, new SslHandler(this.sslFactory.createServerSslEngine(ch.alloc())));
    }
    ch.pipeline().addLast("encoder", new AMQDataBlockEncoder());
    ch.pipeline().addLast("handler", new GatewayConnection(gatewayService));
  }
}
