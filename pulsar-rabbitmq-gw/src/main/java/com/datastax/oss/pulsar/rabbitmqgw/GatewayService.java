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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pulsar RabbitMQ gateway service */
public class GatewayService implements Closeable {

  private final GatewayConfiguration config;
  private PulsarClient pulsarClient;

  private final EventLoopGroup acceptorGroup;
  private final EventLoopGroup workerGroup;

  private Channel listenChannel;
  private Channel listenChannelTls;

  private final DefaultThreadFactory acceptorThreadFactory =
      new DefaultThreadFactory("pulsar-rabbitmqgw-acceptor");
  private final DefaultThreadFactory workersThreadFactory =
      new DefaultThreadFactory("pulsar-rabbitmqgw-io");

  private static final int numThreads = Runtime.getRuntime().availableProcessors();

  public GatewayService(GatewayConfiguration config) {
    checkNotNull(config);
    this.config = config;

    this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
    this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workersThreadFactory);
  }

  public void start() throws Exception {
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
    bootstrap.group(acceptorGroup, workerGroup);
    bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
    bootstrap.childOption(
        ChannelOption.RCVBUF_ALLOCATOR,
        new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

    bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
    EventLoopUtil.enableTriggeredMode(bootstrap);

    bootstrap.childHandler(new ServiceChannelInitializer(this, config, false));
    // Bind and start to accept incoming connections.
    if (config.getServicePort().isPresent()) {
      try {
        listenChannel =
            bootstrap.bind(config.getBindAddress(), config.getServicePort().get()).sync().channel();
        LOG.info("Started Pulsar RabbitMQ Gateway at {}", listenChannel.localAddress());
      } catch (Exception e) {
        throw new IOException(
            "Failed to bind Pulsar RabbitMQ Gateway on port " + config.getServicePort().get(), e);
      }
    }

    if (config.getServicePortTls().isPresent()) {
      ServerBootstrap tlsBootstrap = bootstrap.clone();
      tlsBootstrap.childHandler(new ServiceChannelInitializer(this, config, true));
      listenChannelTls =
          tlsBootstrap
              .bind(config.getBindAddress(), config.getServicePortTls().get())
              .sync()
              .channel();
      LOG.info("Started Pulsar TLS RabbitMQ Gateway on {}", listenChannelTls.localAddress());
    }
  }

  public synchronized PulsarClient getPulsarClient() throws PulsarClientException {
    // Do lazy initialization of client
    if (pulsarClient == null) {
      pulsarClient = createClientInstance();
    }
    return pulsarClient;
  }

  private PulsarClient createClientInstance() throws PulsarClientException {
    ClientBuilder clientBuilder =
        PulsarClient.builder()
            .statsInterval(0, TimeUnit.SECONDS)
            .serviceUrl(config.getBrokerServiceURL())
            .allowTlsInsecureConnection(config.isTlsAllowInsecureConnection())
            .tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath());

    if (isNotBlank(config.getBrokerClientAuthenticationPlugin())
        && isNotBlank(config.getBrokerClientAuthenticationParameters())) {
      clientBuilder.authentication(
          config.getBrokerClientAuthenticationPlugin(),
          config.getBrokerClientAuthenticationParameters());
    }

    return clientBuilder.build();
  }

  public void close() {
    if (listenChannel != null) {
      listenChannel.close();
    }

    if (listenChannelTls != null) {
      listenChannelTls.close();
    }

    acceptorGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  public GatewayConfiguration getConfig() {
    return config;
  }

  private static final Logger LOG = LoggerFactory.getLogger(GatewayService.class);
}
