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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.datastax.oss.starlight.rabbitmq.metadata.ContextMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pulsar RabbitMQ gateway service */
public class GatewayService implements Closeable {

  private final GatewayConfiguration config;
  private final String brokerServiceUrl;
  private final String brokerWebServiceUrl;
  private final AuthenticationService authenticationService;
  private final ScheduledExecutorService executor;
  private PulsarClient pulsarClient;
  private PulsarAdmin pulsarAdmin;

  private EventLoopGroup acceptorGroup;
  private EventLoopGroup workerGroup;

  private final List<Channel> listenChannels = new ArrayList<>();

  private final DefaultThreadFactory acceptorThreadFactory =
      new DefaultThreadFactory("starlight-rabbitmq-acceptor");
  private final DefaultThreadFactory workersThreadFactory =
      new DefaultThreadFactory("starlight-rabbitmq-io");

  private static final int numThreads = Runtime.getRuntime().availableProcessors();

  private final Map<String, Map<String, Queue>> queues = new ConcurrentHashMap<>();

  private CuratorFramework curator;

  public static final ModelSpec<ContextMetadata> METADATA_MODEL_SPEC =
      ModelSpec.builder(
              ZPath.parseWithIds("/config"), JacksonModelSerializer.build(ContextMetadata.class))
          .build();
  private ModeledFramework<ContextMetadata> metadataModel;
  private final ObjectMapper mapper = new ObjectMapper();

  private volatile Versioned<ContextMetadata> contextMetadata =
      Versioned.from(new ContextMetadata(), 0);
  private SubscriptionCleaner subscriptionCleaner;

  public GatewayService(GatewayConfiguration config, AuthenticationService authenticationService) {
    this(
        config,
        authenticationService,
        config.getBrokerServiceURL(),
        config.getBrokerWebServiceURL());
  }

  public GatewayService(
      GatewayConfiguration config,
      AuthenticationService authenticationService,
      String brokerServiceUrl,
      String brokerWebServiceUrl) {
    checkNotNull(config);
    this.config = config;
    this.brokerServiceUrl = brokerServiceUrl;
    this.brokerWebServiceUrl = brokerWebServiceUrl;
    this.authenticationService = authenticationService;
    this.executor =
        Executors.newScheduledThreadPool(
            numThreads, new DefaultThreadFactory("pulsar-rabbitmq-executor"));
  }

  public void start() throws Exception {
    start(true);
  }

  public void start(boolean startChannels) throws Exception {
    pulsarClient = createClientInstance();
    pulsarAdmin = createAdminInstance();
    curator = createCuratorInstance();
    metadataModel = ModeledFramework.wrap(AsyncCuratorFramework.wrap(curator), METADATA_MODEL_SPEC);
    executor.scheduleWithFixedDelay(this::loadContext, 0, 100, TimeUnit.MILLISECONDS);
    subscriptionCleaner = new SubscriptionCleaner(this, curator);
    subscriptionCleaner.start();

    if (startChannels) {
      acceptorGroup = EventLoopUtil.newEventLoopGroup(1, true, acceptorThreadFactory);
      workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, true, workersThreadFactory);
      for (Map.Entry<InetSocketAddress, ChannelInitializer<SocketChannel>> entry :
          newChannelInitializers().entrySet()) {
        InetSocketAddress address = entry.getKey();
        ChannelInitializer<SocketChannel> channelInitializer = entry.getValue();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, workerGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(
            ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        bootstrap.childHandler(channelInitializer);
        try {
          Channel listenChannel = bootstrap.bind(address).sync().channel();
          listenChannels.add(listenChannel);
          LOG.info("Started Pulsar RabbitMQ Gateway at {}", listenChannel.localAddress());
        } catch (Exception e) {
          throw new IOException("Failed to bind Pulsar RabbitMQ on address " + address, e);
        }
      }
    }
  }

  public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
    ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
        ImmutableMap.builder();

    config
        .getAmqpListeners()
        .forEach(
            listener -> {
              URI uri = URI.create(listener);
              if (uri.getScheme().equals("amqp")) {
                builder.put(
                    new InetSocketAddress(uri.getHost(), uri.getPort()),
                    new ServiceChannelInitializer(this, config, false));
              } else if (uri.getScheme().equals("amqps")) {
                builder.put(
                    new InetSocketAddress(uri.getHost(), uri.getPort()),
                    new ServiceChannelInitializer(this, config, true));
              } else {
                LOG.warn(
                    "Malformed listener string for Pulsar RabbitMQ will be ignored: " + listener);
              }
            });
    return builder.build();
  }

  public PulsarClient getPulsarClient() {
    return pulsarClient;
  }

  private PulsarClient createClientInstance() throws PulsarClientException {
    ClientBuilder clientBuilder =
        PulsarClient.builder().statsInterval(0, TimeUnit.SECONDS).serviceUrl(brokerServiceUrl);

    if (isNotBlank(config.getBrokerClientAuthenticationPlugin())) {
      if (isNotBlank(config.getAmqpBrokerClientAuthenticationParameters())) {
        clientBuilder.authentication(
            config.getBrokerClientAuthenticationPlugin(),
            config.getAmqpBrokerClientAuthenticationParameters());
      } else if (isNotBlank(config.getBrokerClientAuthenticationParameters())) {
        clientBuilder.authentication(
            config.getBrokerClientAuthenticationPlugin(),
            config.getBrokerClientAuthenticationParameters());
      }
    }

    // set trust store if needed.
    if (config.isTlsEnabledWithBroker()) {
      if (config.isBrokerClientTlsEnabledWithKeyStore()) {
        clientBuilder
            .useKeyStoreTls(true)
            .tlsTrustStoreType(config.getBrokerClientTlsTrustStoreType())
            .tlsTrustStorePath(config.getBrokerClientTlsTrustStore())
            .tlsTrustStorePassword(config.getBrokerClientTlsTrustStorePassword());
      } else {
        clientBuilder.tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath());
      }
      clientBuilder.allowTlsInsecureConnection(config.isTlsAllowInsecureConnection());
      clientBuilder.enableTlsHostnameVerification(config.isTlsHostnameVerificationEnabled());
    }

    return clientBuilder.build();
  }

  public PulsarAdmin getPulsarAdmin() {
    return pulsarAdmin;
  }

  private PulsarAdmin createAdminInstance() throws PulsarClientException {
    PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().serviceHttpUrl(brokerWebServiceUrl);

    if (isNotBlank(config.getBrokerClientAuthenticationPlugin())) {
      if (isNotBlank(config.getAmqpBrokerClientAuthenticationParameters())) {
        adminBuilder.authentication(
            config.getBrokerClientAuthenticationPlugin(),
            config.getAmqpBrokerClientAuthenticationParameters());
      } else if (isNotBlank(config.getBrokerClientAuthenticationParameters())) {
        adminBuilder.authentication(
            config.getBrokerClientAuthenticationPlugin(),
            config.getBrokerClientAuthenticationParameters());
      }
    }

    // set trust store if needed.
    if (config.isTlsEnabledWithBroker()) {
      if (config.isBrokerClientTlsEnabledWithKeyStore()) {
        adminBuilder
            .useKeyStoreTls(true)
            .tlsTrustStoreType(config.getBrokerClientTlsTrustStoreType())
            .tlsTrustStorePath(config.getBrokerClientTlsTrustStore())
            .tlsTrustStorePassword(config.getBrokerClientTlsTrustStorePassword());
      } else {
        adminBuilder.tlsTrustCertsFilePath(config.getBrokerClientTrustCertsFilePath());
      }
      adminBuilder.allowTlsInsecureConnection(config.isTlsAllowInsecureConnection());
      adminBuilder.enableTlsHostnameVerification(config.isTlsHostnameVerificationEnabled());
    }

    return adminBuilder.build();
  }

  private CuratorFramework createCuratorInstance() {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client =
        CuratorFrameworkFactory.builder()
            .connectString(config.getConfigurationStoreServers())
            .sessionTimeoutMs(5000)
            .connectionTimeoutMs(5000)
            .retryPolicy(retryPolicy)
            .namespace("starlight-rabbitmq")
            .build();
    client.start();
    return client;
  }

  public void close() throws IOException {
    listenChannels.forEach(Channel::close);

    if (subscriptionCleaner != null) {
      subscriptionCleaner.close();
    }
    if (curator != null) {
      curator.close();
    }

    if (acceptorGroup != null) {
      acceptorGroup.shutdownGracefully();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
    if (executor != null) {
      executor.shutdown();
    }
  }

  public GatewayConfiguration getConfig() {
    return config;
  }

  private static final Logger LOG = LoggerFactory.getLogger(GatewayService.class);

  public Map<String, Map<String, Queue>> getQueues() {
    return queues;
  }

  public Versioned<ContextMetadata> getContextMetadata() {
    return contextMetadata;
  }

  @VisibleForTesting
  public void setContextMetadata(Versioned<ContextMetadata> contextMetadata) {
    this.contextMetadata = contextMetadata;
  }

  public Versioned<ContextMetadata> newContextMetadata(Versioned<ContextMetadata> previousContext) {
    try {
      ContextMetadata contextMetadata =
          mapper.readValue(
              mapper.writeValueAsString(previousContext.model()), ContextMetadata.class);
      return Versioned.from(contextMetadata, previousContext.version());
    } catch (JsonProcessingException e) {
      LOG.error("Error while cloning context", e);
      return null;
    }
  }

  public CompletionStage<ContextMetadata> saveContext(Versioned<ContextMetadata> metadata) {
    return metadataModel.versioned().set(metadata).thenCompose(it -> loadContext());
  }

  public CompletionStage<ContextMetadata> loadContext() {
    return metadataModel
        .versioned()
        .read()
        .thenApply(
            context -> {
              contextMetadata = context;
              return updateContext(contextMetadata.model());
            });
  }

  public synchronized ContextMetadata updateContext(ContextMetadata contextMetadata) {
    contextMetadata
        .getVhosts()
        .forEach(
            (namespace, vhostMetadata) -> {
              Map<String, Queue> vhostQueues =
                  queues.computeIfAbsent(namespace, it -> new ConcurrentHashMap<>());

              vhostMetadata
                  .getSubscriptions()
                  .forEach(
                      (queueName, queueSubscriptions) ->
                          queueSubscriptions.forEach(
                              (subscriptionName, bindingMetadata) -> {
                                Queue queue =
                                    vhostQueues.computeIfAbsent(queueName, q -> new Queue());

                                for (AMQConsumer consumer : queue.getConsumers()) {
                                  PulsarConsumer pulsarConsumer =
                                      consumer.startSubscription(
                                          subscriptionName, bindingMetadata.getTopic(), this);
                                  if (bindingMetadata.getLastMessageId() != null) {
                                    try {
                                      MessageId messageId =
                                          MessageId.fromByteArray(
                                              bindingMetadata.getLastMessageId());
                                      pulsarConsumer.setLastMessageId(messageId);
                                    } catch (IOException e) {
                                      LOG.error(
                                          "Error while deserializing binding's lastMessageId", e);
                                    }
                                  }
                                }
                              }));
            });

    return contextMetadata;
  }

  public AuthenticationService getAuthenticationService() {
    return authenticationService;
  }

  public ScheduledExecutorService getExecutor() {
    return executor;
  }
}
