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

import com.datastax.oss.pulsar.rabbitmqgw.metadata.BindingSetMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ContextMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ExchangeMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletionStage;
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

  private final AMQContext amqContext = new AMQContext();

  private final Map<String, Map<String, Queue>> queues = new HashMap<>();

  public static final ModelSpec<ContextMetadata> METADATA_MODEL_SPEC =
      ModelSpec.builder(
              ZPath.parseWithIds("/config"), JacksonModelSerializer.build(ContextMetadata.class))
          .build();
  private ModeledFramework<ContextMetadata> metadataModel;
  private final ObjectMapper mapper = new ObjectMapper();

  private volatile Versioned<ContextMetadata> contextMetadata =
      Versioned.from(new ContextMetadata(), 0);

  public GatewayService(GatewayConfiguration config) {
    checkNotNull(config);
    this.config = config;

    this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, true, acceptorThreadFactory);
    this.workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, true, workersThreadFactory);
  }

  public void start() throws Exception {
    pulsarClient = createClientInstance();
    pulsarAdmin = createAdminInstance();
    metadataModel = ModeledFramework.wrap(createCuratorInstance(), METADATA_MODEL_SPEC);
    workerGroup.scheduleWithFixedDelay(this::loadContext, 0, 100, TimeUnit.MILLISECONDS);
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

  public VirtualHost getOrCreateVhost(String namespace) {
    return amqContext.getVhosts().computeIfAbsent(namespace, VirtualHost::new);
  }

  private static final Logger LOG = LoggerFactory.getLogger(GatewayService.class);

  public AMQContext getAmqContext() {
    return amqContext;
  }

  public Map<String, Map<String, Queue>> getQueues() {
    return queues;
  }

  public Versioned<ContextMetadata> getContextMetadata() {
    return contextMetadata;
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
              /*Map<String, VirtualHost> vhosts = amqContext.getVhosts();
              vhosts.putIfAbsent(namespace, new VirtualHost(namespace));
              VirtualHost vhost = vhosts.get(namespace);
              createNewQueues(vhostMetadata, vhost);
              createNewExchanges(vhostMetadata, vhost);
              */

              Map<String, Queue> vhostQueues =
                  queues.computeIfAbsent(namespace, it -> new HashMap<>());

              vhostMetadata
                  .getQueues()
                  .keySet()
                  .forEach(queueName -> vhostQueues.computeIfAbsent(queueName, it -> new Queue()));
              Map<String, ExchangeMetadata> exchanges = vhostMetadata.getExchanges();
              exchanges.forEach(
                  (exchange, exchangeMetadata) ->
                      exchangeMetadata
                          .getBindings()
                          .forEach(
                              (queueName, bindingSetMetadata) ->
                                  bindingSetMetadata
                                      .getSubscriptions()
                                      .forEach(
                                          (subscriptionName, bindingMetadata) -> {
                                            Queue queue = vhostQueues.get(queueName);
                                            PulsarConsumer pulsarConsumer =
                                                queue.getSubscriptions().get(subscriptionName);
                                            if (pulsarConsumer == null) {
                                              pulsarConsumer =
                                                  new PulsarConsumer(
                                                      exchange,
                                                      bindingMetadata.getTopic(),
                                                      subscriptionName,
                                                      this,
                                                      queue);
                                              // TODO: handle subscription errors
                                              pulsarConsumer
                                                  .subscribe()
                                                  .thenRun(
                                                      pulsarConsumer::receiveAndDeliverMessages);
                                              queue
                                                  .getSubscriptions()
                                                  .put(subscriptionName, pulsarConsumer);
                                            } else {
                                              if (bindingMetadata.getLastMessageId() != null) {
                                                MessageId messageId =
                                                    null;
                                                try {
                                                  messageId =
                                                      MessageId.fromByteArray(bindingMetadata.getLastMessageId());
                                                  pulsarConsumer.setLastMessageId(
                                                      messageId);
                                                } catch (IOException e) {
                                                  LOG.error("Error while deserializing binding's lastMessageId", e);
                                                }
                                              }
                                            }
                                          })));

              vhostQueues.forEach(
                  (queueName, queue) -> {
                    for (Map.Entry<String, PulsarConsumer> next :
                        queue.getSubscriptions().entrySet()) {
                      PulsarConsumer consumer = next.getValue();
                      ExchangeMetadata exchangeMetadata = exchanges.get(consumer.getExchange());
                      if (exchangeMetadata != null) {
                        BindingSetMetadata bindingSetMetadata =
                            exchangeMetadata.getBindings().get(queueName);
                        if (bindingSetMetadata != null) {
                          if (bindingSetMetadata.getSubscriptions().containsKey(next.getKey())) {
                            continue;
                          }
                        }
                      }
                      consumer.close();
                    }
                  });

              Iterator<Map.Entry<String, Queue>> queueIterator = vhostQueues.entrySet().iterator();
              while (queueIterator.hasNext()) {
                Map.Entry<String, Queue> queueEntry = queueIterator.next();
                if (!vhostMetadata.getQueues().containsKey(queueEntry.getKey())) {
                  queueEntry.getValue().getConsumers().forEach(AMQConsumer::unsubscribe);
                  queueIterator.remove();
                }
              }
            });

    return contextMetadata;
  }

  /*private void createNewQueues(VirtualHostMetadata vhostMetadata, VirtualHost vhost) {
    vhostMetadata
        .getQueues()
        .forEach(
            (queue, queueMetadata) ->
                vhost.getQueues().putIfAbsent(queue, Queue.fromMetadata(queue, queueMetadata)));
  }

  private void createNewExchanges(VirtualHostMetadata vhostMetadata, VirtualHost vhost) {
    vhostMetadata
        .getExchanges()
        .forEach(
            (exchangeName, exchangeMetadata) -> {
              vhost
                  .getExchanges()
                  .putIfAbsent(
                      exchangeName, AbstractExchange.fromMetadata(exchangeName, exchangeMetadata));
              createNewBindings(vhost, exchangeName, exchangeMetadata);
            });
  }

  private void createNewBindings(
      VirtualHost vhost, String exchangeName, ExchangeMetadata exchangeMetadata) {
    exchangeMetadata
        .getBindings()
        .forEach(
            (queue, bindingsMetadata) -> {
              AbstractExchange exchange = vhost.getExchanges().get(exchangeName);
              Map<String, Map<String, PulsarConsumer>> bindings = exchange.getBindings();
              bindings.putIfAbsent(queue, new HashMap<>());
              vhost.getQueue(queue).getBoundExchanges().add(exchange);
              bindingsMetadata.forEach(
                  (key, bindingMetadata) -> {
                    bindings
                        .get(queue)
                        .computeIfAbsent(
                            key,
                            it -> {
                              PulsarConsumer pulsarConsumer =
                                  new PulsarConsumer(
                                      bindingMetadata.getTopic(),
                                      bindingMetadata.getSubscription(),
                                      this,
                                      vhost.getQueue(queue));
                              // TODO: handle subscription errors
                              pulsarConsumer
                                  .subscribe()
                                  .thenRun(pulsarConsumer::receiveAndDeliverMessages);
                              return pulsarConsumer;
                            });
                  });
            });
  }*/

}
