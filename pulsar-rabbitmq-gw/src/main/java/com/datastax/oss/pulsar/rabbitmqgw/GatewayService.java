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

import com.datastax.oss.pulsar.rabbitmqgw.metadata.ContextMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
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
  private final AuthenticationService authenticationService;
  private PulsarClient pulsarClient;
  private PulsarAdmin pulsarAdmin;

  private final EventLoopGroup acceptorGroup;
  private final EventLoopGroup workerGroup;

  private Channel listenChannel;
  private Channel listenChannelTls;

  private final DefaultThreadFactory acceptorThreadFactory =
      new DefaultThreadFactory("pulsar-rabbitmqgw-acceptor");
  private final DefaultThreadFactory workersThreadFactory =
      new DefaultThreadFactory("pulsar-rabbitmqgw-io");

  private static final int numThreads = Runtime.getRuntime().availableProcessors();

  private final Map<String, Map<String, Queue>> queues = new ConcurrentHashMap<>();

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
    checkNotNull(config);
    this.config = config;
    this.authenticationService = authenticationService;

    this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, true, acceptorThreadFactory);
    this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, true, workersThreadFactory);
  }

  public void start() throws Exception {
    pulsarClient = createClientInstance();
    pulsarAdmin = createAdminInstance();
    AsyncCuratorFramework curator = createCuratorInstance();
    metadataModel = ModeledFramework.wrap(curator, METADATA_MODEL_SPEC);
    workerGroup.scheduleWithFixedDelay(this::loadContext, 0, 100, TimeUnit.MILLISECONDS);
    subscriptionCleaner = new SubscriptionCleaner(this, curator.unwrap());
    subscriptionCleaner.start();

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

  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  public PulsarClient getPulsarClient() {
    return pulsarClient;
  }

  private PulsarClient createClientInstance() throws PulsarClientException {
    ClientBuilder clientBuilder =
        PulsarClient.builder()
            .statsInterval(0, TimeUnit.SECONDS)
            .serviceUrl(config.getBrokerServiceURL());

    if (isNotBlank(config.getBrokerClientAuthenticationPlugin())
        && isNotBlank(config.getBrokerClientAuthenticationParameters())) {
      clientBuilder.authentication(
          config.getBrokerClientAuthenticationPlugin(),
          config.getBrokerClientAuthenticationParameters());
    }

    // set trust store if needed.
    if (config.isTlsEnabledWithBroker()) {
      if (config.isBrokerClientTlsEnabledWithKeyStore()) {
        clientBuilder.useKeyStoreTls(true)
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
    PulsarAdminBuilder adminBuilder =
        PulsarAdmin.builder().serviceHttpUrl(config.getBrokerWebServiceURL());

    if (isNotBlank(config.getBrokerClientAuthenticationPlugin())
        && isNotBlank(config.getBrokerClientAuthenticationParameters())) {
      adminBuilder.authentication(
          config.getBrokerClientAuthenticationPlugin(),
          config.getBrokerClientAuthenticationParameters());
    }

    // set trust store if needed.
    if (config.isTlsEnabledWithBroker()) {
      if (config.isBrokerClientTlsEnabledWithKeyStore()) {
        adminBuilder.useKeyStoreTls(true)
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

  private AsyncCuratorFramework createCuratorInstance() {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client =
        CuratorFrameworkFactory.builder()
            .connectString(config.getZookeeperServers())
            .sessionTimeoutMs(5000)
            .connectionTimeoutMs(5000)
            .retryPolicy(retryPolicy)
            .namespace("pulsar-rabbitmq-gw")
            .build();
    client.start();
    return AsyncCuratorFramework.wrap(client);
  }

  public void close() throws IOException {
    if (listenChannel != null) {
      listenChannel.close();
    }

    if (listenChannelTls != null) {
      listenChannelTls.close();
    }

    if (subscriptionCleaner != null) {
      subscriptionCleaner.close();
    }

    acceptorGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
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
                                PulsarConsumer pulsarConsumer =
                                    queue.getSubscriptions().get(subscriptionName);
                                if (pulsarConsumer == null) {
                                  pulsarConsumer =
                                      new PulsarConsumer(
                                          bindingMetadata.getTopic(),
                                          subscriptionName,
                                          this,
                                          queue);
                                  // TODO: handle subscription errors
                                  pulsarConsumer
                                      .subscribe()
                                      .thenRun(pulsarConsumer::receiveAndDeliverMessages);
                                  queue.getSubscriptions().put(subscriptionName, pulsarConsumer);
                                } else {
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
}
