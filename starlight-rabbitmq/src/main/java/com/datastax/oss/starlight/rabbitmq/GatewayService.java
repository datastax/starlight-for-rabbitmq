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

import com.datastax.oss.starlight.rabbitmq.metadata.BindingMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.BindingSetMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.ContextMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.ExchangeMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.VirtualHostMetadata;
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
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.jodah.failsafe.Failsafe;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.qpid.server.exchange.topic.TopicParser;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starlight for RabbitMQ service */
public class GatewayService implements Closeable {

  private final GatewayConfiguration config;
  private final String brokerServiceUrl;
  private final String brokerWebServiceUrl;
  private final AuthenticationService authenticationService;
  private final ScheduledExecutorService executor;
  private final ExecutorProvider internalExecutorService;
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
  private final Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();

  private CuratorFramework curator;

  private PersistentWatcher persistentWatcher;

  public static final ModelSpec<ContextMetadata> METADATA_MODEL_SPEC =
      ModelSpec.builder(
              ZPath.parseWithIds("/config"), JacksonModelSerializer.build(ContextMetadata.class))
          .build();
  private ModeledFramework<ContextMetadata> metadataModel;
  private final ObjectMapper mapper = new ObjectMapper();

  private volatile Versioned<ContextMetadata> contextMetadata =
      Versioned.from(new ContextMetadata(), 0);

  private static final Logger LOG = LoggerFactory.getLogger(GatewayService.class);

  public static final net.jodah.failsafe.RetryPolicy<Object> ZK_CONFLICT_RETRY =
      new net.jodah.failsafe.RetryPolicy<>()
          .handle(KeeperException.BadVersionException.class)
          .onRetriesExceeded(lis -> LOG.error("Zookeeper Retries exceeded", lis.getFailure()))
          .withJitter(0.3)
          .withDelay(Duration.ofMillis(50))
          .withMaxRetries(100);
  private SubscriptionCleaner subscriptionCleaner;

  private TopicExchangeUpdater topicExchangeUpdater;

  private Producer<String> exchangeKeyProducer;

  private Consumer<String> exchangeKeyConsumer;

  private final Set<String> knownTopics = ConcurrentHashMap.newKeySet();

  final Counter inBytesCounter;
  final Counter outBytesCounter;
  final Gauge activeConnections;
  final Gauge newConnections;

  final Counter inMessagesCounter;
  final Counter outMessagesCounter;

  public GatewayService(GatewayConfiguration config, AuthenticationService authenticationService) {
    this(config, authenticationService, "server");
  }

  public GatewayService(
      GatewayConfiguration config,
      AuthenticationService authenticationService,
      String metricsPrefix) {
    this(
        config,
        authenticationService,
        config.getBrokerServiceURL(),
        config.getBrokerWebServiceURL(),
        metricsPrefix);
  }

  public GatewayService(
      GatewayConfiguration config,
      AuthenticationService authenticationService,
      String brokerServiceUrl,
      String brokerWebServiceUrl,
      String metricsPrefix) {
    checkNotNull(config);
    this.config = config;
    this.brokerServiceUrl = brokerServiceUrl;
    this.brokerWebServiceUrl = brokerWebServiceUrl;
    this.authenticationService = authenticationService;
    this.executor =
        Executors.newScheduledThreadPool(
            numThreads, new DefaultThreadFactory("starlight-rabbitmq-executor"));
    this.internalExecutorService = new ExecutorProvider(numThreads, "starlight-rabbitmq-internal");

    inBytesCounter =
        Counter.build(
                metricsPrefix + "_rabbitmq_in_bytes", "Counter of Starlight for RabbitMQ bytes in")
            .labelNames("namespace")
            .register();

    outBytesCounter =
        Counter.build(
                metricsPrefix + "_rabbitmq_out_bytes",
                "Counter of Starlight for RabbitMQ bytes out")
            .labelNames("namespace")
            .register();

    inMessagesCounter =
        Counter.build(
                metricsPrefix + "_rabbitmq_in_messages_total",
                "Counter of Starlight for RabbitMQ messages in")
            .labelNames("namespace")
            .register();

    outMessagesCounter =
        Counter.build(
                metricsPrefix + "_rabbitmq_out_messages_total",
                "Counter of Starlight for RabbitMQ messages out")
            .labelNames("namespace")
            .register();

    activeConnections =
        Gauge.build(
                metricsPrefix + "_rabbitmq_active_connections",
                "Number of connections currently active for Starlight-for-RabbitMQ")
            .register();
    newConnections =
        Gauge.build(
                metricsPrefix + "_rabbitmq_new_connections",
                "Counter of connections being opened in Starlight-for-RabbitMQ")
            .register();
  }

  public void start() throws Exception {
    start(true);
  }

  public void start(boolean startChannels) throws Exception {
    pulsarClient = createClientInstance();
    pulsarAdmin = createAdminInstance();
    curator = createCuratorInstance();
    metadataModel = ModeledFramework.wrap(AsyncCuratorFramework.wrap(curator), METADATA_MODEL_SPEC);

    persistentWatcher = new PersistentWatcher(curator, "/config", true);
    persistentWatcher.start();
    persistentWatcher
        .getListenable()
        .addListener(
            event -> {
              LOG.info("Config watcher event received - {}", event);
              if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                loadContext();
              }
            });
    persistentWatcher
        .getResetListenable()
        .addListener(
            () -> {
              LOG.info("ZooKeeper connection reset, reloading config...");
              loadContext();
            });

    subscriptionCleaner = new SubscriptionCleaner(this, curator);
    subscriptionCleaner.start();

    topicExchangeUpdater = new TopicExchangeUpdater(this, curator);
    topicExchangeUpdater.start();

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
          LOG.info("Started Starlight for RabbitMQ at {}", listenChannel.localAddress());
        } catch (Exception e) {
          throw new IOException("Failed to bind Starlight for RabbitMQ on address " + address, e);
        }
      }
    }
  }

  private void createExchangeKeyProducerIfNeeded() throws PulsarClientException {
    if (exchangeKeyProducer == null) {
      exchangeKeyProducer =
          pulsarClient.newProducer(Schema.STRING).topic("__exchange-keys").create();
    }
  }

  private void createExchangeKeyConsumerIfNeeded() throws PulsarClientException {
    if (exchangeKeyConsumer == null) {
      exchangeKeyConsumer =
          pulsarClient
              .newConsumer(Schema.STRING)
              .topic("__exchange-keys")
              .subscriptionName("starlight-for-rabbitmq")
              .subscriptionType(SubscriptionType.Failover)
              .messageListener((MessageListener<String>) this::receivedExchangeKeyMessage)
              .subscribe();
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
                    new ServiceChannelInitializer(this, config, false, executor));
              } else if (uri.getScheme().equals("amqps")) {
                builder.put(
                    new InetSocketAddress(uri.getHost(), uri.getPort()),
                    new ServiceChannelInitializer(this, config, true, executor));
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
        PulsarClient.builder()
            .statsInterval(0, TimeUnit.SECONDS)
            .ioThreads(8)
            .connectionsPerBroker(8)
            .maxConcurrentLookupRequests(50000)
            .maxLookupRequests(100000)
            // .memoryLimit(256, SizeUnit.MEGA_BYTES)
            .serviceUrl(brokerServiceUrl);

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
    String connectUrl = config.getConfigurationMetadataStoreUrl();
    if (connectUrl.startsWith(ZKMetadataStore.ZK_SCHEME_IDENTIFIER)) {
      connectUrl = connectUrl.substring(ZKMetadataStore.ZK_SCHEME_IDENTIFIER.length());
    }
    CuratorFramework client =
        CuratorFrameworkFactory.builder()
            .connectString(connectUrl)
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
    producers.values().forEach(Producer::closeAsync);

    if (exchangeKeyProducer != null) {
      exchangeKeyProducer.close();
    }
    if (exchangeKeyConsumer != null) {
      exchangeKeyConsumer.close();
    }
    if (subscriptionCleaner != null) {
      subscriptionCleaner.close();
    }
    if (topicExchangeUpdater != null) {
      topicExchangeUpdater.close();
    }
    if (persistentWatcher != null) {
      persistentWatcher.close();
    }
    if (curator != null) {
      curator.close();
    }
    if (pulsarClient != null) {
      pulsarClient.close();
    }
    if (pulsarAdmin != null) {
      pulsarAdmin.close();
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
    if (internalExecutorService != null) {
      internalExecutorService.shutdownNow();
    }
  }

  public GatewayConfiguration getConfig() {
    return config;
  }

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

  public void incrementBytesIn(String namespace, double bytes) {
    inBytesCounter.labels(namespace == null ? "" : namespace).inc(bytes);
  }

  public void incrementBytesOut(String namespace, double bytes) {
    outBytesCounter.labels(namespace == null ? "" : namespace).inc(bytes);
  }

  public void incrementMessagesIn(String namespace, int count) {
    inMessagesCounter.labels(namespace == null ? "" : namespace).inc(count);
  }

  public void incrementMessagesOut(String namespace, int count) {
    outMessagesCounter.labels(namespace == null ? "" : namespace).inc(count);
  }

  public AuthenticationService getAuthenticationService() {
    return authenticationService;
  }

  public ScheduledExecutorService getExecutor() {
    return executor;
  }

  public ExecutorService getInternalExecutorService() {
    return internalExecutorService.getExecutor();
  }

  public Producer<byte[]> getOrCreateProducer(String topic) {
    return producers.computeIfAbsent(topic, this::createProducer);
  }

  public void removeProducer(Producer<byte[]> producer) {
    producers.remove(producer.getTopic(), producer);
    producer.closeAsync();
  }

  private Producer<byte[]> createProducer(String topicName) {
    try {
      createExchangeKeyConsumerIfNeeded();
      createExchangeKeyProducerIfNeeded();
      Producer<byte[]> producer =
          getPulsarClient()
              .newProducer()
              .enableBatching(config.isAmqpBatchingEnabled())
              .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
              .maxPendingMessages(10000)
              .topic(topicName)
              .create();
      exchangeKeyProducer.sendAsync(topicName);
      return producer;
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  private void receivedExchangeKeyMessage(Consumer<String> consumer, Message<String> msg) {
    Failsafe.with(ZK_CONFLICT_RETRY)
        .getStageAsync(() -> updateQueueSubscriptionsToTopicExchanges(msg.getValue()))
        .thenAccept(
            it -> {
              try {
                consumer.acknowledge(msg);
              } catch (PulsarClientException e) {
                LOG.error("Failed to acknowledge exchange with key update message", e);
                consumer.negativeAcknowledge(msg);
              }
            })
        .exceptionally(
            throwable -> {
              LOG.error("Failed to handle exchange with key update message", throwable);
              consumer.negativeAcknowledge(msg);
              return null;
            });
  }

  public CompletableFuture<Void> updateQueueSubscriptionsToTopicExchanges(String topic) {
    if (knownTopics.contains(topic)) {
      return CompletableFuture.completedFuture(null);
    }
    TopicName topicName = TopicName.get(topic);
    Versioned<ContextMetadata> metadataVersioned = newContextMetadata(contextMetadata);
    VirtualHostMetadata vhost = metadataVersioned.model().getVhosts().get(topicName.getNamespace());
    if (vhost != null) {
      String[] exchangeAndKey = topicName.getLocalName().split(".__");
      String exchangeName = exchangeAndKey[0];
      String routingKey = exchangeAndKey.length == 2 ? exchangeAndKey[1] : "";
      ExchangeMetadata exchange = vhost.getExchanges().get(exchangeName);
      if (exchange != null && ExchangeMetadata.Type.topic.equals(exchange.getType())) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<String, BindingSetMetadata> entry : exchange.getBindings().entrySet()) {
          String queue = entry.getKey();
          BindingSetMetadata bindingSetMetadata = entry.getValue();
          Map<String, BindingMetadata> subscriptions = vhost.getSubscriptions().get(queue);
          boolean subscriptionExists =
              subscriptions.values().stream().anyMatch(binding -> binding.getTopic().equals(topic));
          if (!subscriptionExists) {
            TopicParser parser = new TopicParser();
            bindingSetMetadata.getKeys().forEach(key -> parser.addBinding(key, null));
            if (parser.parse(routingKey).size() > 0) {
              String subscriptionName = topic.replace("/", "_") + "-" + UUID.randomUUID();
              CompletableFuture<Void> future =
                  pulsarAdmin
                      .topics()
                      .createSubscriptionAsync(topic, subscriptionName, MessageId.earliest)
                      .thenAccept(
                          it -> {
                            BindingMetadata bindingMetadata =
                                new BindingMetadata(exchangeName, topic, subscriptionName);
                            bindingMetadata.getKeys().add(routingKey);
                            subscriptions.put(subscriptionName, bindingMetadata);
                          });
              futures.add(future);
            }
          }
        }
        if (futures.size() > 0) {
          return CompletableFuture.allOf(futures.stream().toArray(CompletableFuture[]::new))
              .thenCompose(it -> saveContext(metadataVersioned))
              .thenAccept(it -> knownTopics.add(topic));
        }
      }
    }
    knownTopics.add(topic);
    return CompletableFuture.completedFuture(null);
  }

  @VisibleForTesting
  public void setExchangeKeyProducer(Producer<String> exchangeKeyProducer) {
    this.exchangeKeyProducer = exchangeKeyProducer;
  }

  @VisibleForTesting
  public void setExchangeKeyConsumer(Consumer<String> exchangeKeyConsumer) {
    this.exchangeKeyConsumer = exchangeKeyConsumer;
  }
}
