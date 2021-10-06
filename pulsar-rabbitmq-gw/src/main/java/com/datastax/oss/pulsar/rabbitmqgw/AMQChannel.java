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

import static com.datastax.oss.pulsar.rabbitmqgw.AbstractExchange.getTopicName;
import static org.apache.qpid.server.transport.util.Functions.hex;

import com.datastax.oss.pulsar.rabbitmqgw.metadata.BindingMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.BindingSetMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ContextMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ExchangeMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.QueueMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.VirtualHostMetadata;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.flow.FlowCreditManager;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetEmptyBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicNackBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQChannel implements ServerChannelMethodProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AMQChannel.class);
  private static final long DEFAULT_HIGH_PREFETCH_LIMIT = 100L;
  private static final long DEFAULT_BATCH_LIMIT = 10L;
  private static final byte[] EARLIEST_MESSAGE_ID = MessageId.earliest.toByteArray();

  private final int _channelId;
  private final Pre0_10CreditManager _creditManager;
  /**
   * The delivery tag is unique per channel. This is pre-incremented before putting into the deliver
   * frame so that value of this represents the <b>last</b> tag sent out
   */
  private volatile long _deliveryTag = 0;

  private String defaultQueueName;
  /**
   * This tag is unique per subscription to a queue. The server returns this in response to a
   * basic.consume request.
   */
  private volatile int _consumerTag;

  private final UnacknowledgedMessageMap _unacknowledgedMessageMap;
  /** Maps from consumer tag to subscription instance. Allows us to unsubscribe from a queue. */
  private final Map<AMQShortString, AMQConsumer> _tag2SubscriptionTargetMap = new HashMap<>();
  /**
   * The current message - which may be partial in the sense that not all frames have been received
   * yet - which has been received by this channel. As the frames are received the message gets
   * updated and once all frames have been received the message can then be routed.
   */
  private IncomingMessage _currentMessage;

  private final GatewayConnection _connection;
  private final AtomicBoolean _closing = new AtomicBoolean(false);
  private boolean _confirmOnPublish;
  private long _confirmedMessageCounter;

  private final ConcurrentHashMap<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();
  public static final RetryPolicy<Object> ZK_CONFLICT_RETRY =
      new RetryPolicy<>()
          .handle(KeeperException.BadVersionException.class)
          .withDelay(Duration.ofMillis(100))
          .withMaxRetries(10);

  public AMQChannel(GatewayConnection connection, int channelId) {
    _connection = connection;
    _channelId = channelId;
    _creditManager =
        new Pre0_10CreditManager(0L, 0L, DEFAULT_HIGH_PREFETCH_LIMIT, DEFAULT_BATCH_LIMIT);
    _unacknowledgedMessageMap = new UnacknowledgedMessageMap(_creditManager);
  }

  @Override
  public void receiveAccessRequest(
      AMQShortString realm,
      boolean exclusive,
      boolean passive,
      boolean active,
      boolean write,
      boolean read) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] AccessRequest["
              + " realm: "
              + realm
              + " exclusive: "
              + exclusive
              + " passive: "
              + passive
              + " active: "
              + active
              + " write: "
              + write
              + " read: "
              + read
              + " ]");
    }

    MethodRegistry methodRegistry = _connection.getMethodRegistry();

    // Note: Qpid sends an error if the protocol version is 0.9.1 whereas RabbitMQ accepts it for
    // backward compatibility reason. See https://www.rabbitmq.com/spec-differences.html

    // We don't implement access control class, but to keep clients happy that expect it
    // always use the "0" ticket.
    AccessRequestOkBody response = methodRegistry.createAccessRequestOkBody(0);
    _connection.writeFrame(response.generateFrame(_channelId));
  }

  @Override
  public void receiveExchangeDeclare(
      AMQShortString exchangeName,
      AMQShortString type,
      boolean passive,
      boolean durable,
      boolean autoDelete,
      boolean internal,
      boolean nowait,
      FieldTable arguments) {

    String name = sanitizeExchangeName(exchangeName);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] ExchangeDeclare["
              + " exchange: "
              + name
              + " type: "
              + type
              + " passive: "
              + passive
              + " durable: "
              + durable
              + " autoDelete: "
              + autoDelete
              + " internal: "
              + internal
              + " nowait: "
              + nowait
              + " arguments: "
              + arguments
              + " ]");
    }

    final MethodRegistry methodRegistry = _connection.getMethodRegistry();
    final AMQMethodBody declareOkBody = methodRegistry.createExchangeDeclareOkBody();

    Versioned<ContextMetadata> versionedContext = getContextMetadata();
    ContextMetadata context = versionedContext.model();
    ExchangeMetadata exchange;

    if (passive) {
      exchange = getExchange(context, name);
      if (exchange == null) {
        closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange: '" + name + "'");
      } else if (!(type == null || type.length() == 0)
          && !exchange.getType().toString().equals(type.toString())) {

        closeChannel(
            ErrorCodes.NOT_ALLOWED,
            "Attempt to redeclare exchange: '"
                + name
                + "' of type "
                + exchange.getType()
                + " to "
                + type
                + ".");
      } else if (!nowait) {
        _connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
      }

    } else {
      String typeString = type == null ? null : type.toString();

      LifetimePolicy lifetimePolicy =
          autoDelete ? LifetimePolicy.DELETE_ON_NO_LINKS : LifetimePolicy.PERMANENT;

      AbstractExchange.Type exchangeType;
      try {
        exchangeType = AbstractExchange.Type.valueOf(typeString);
        if (isReservedExchangeName(name)) {
          // Note: Qpid and RabbitMQ behave differently. Qpid would return OK if the exchange
          // exists.
          closeChannel(
              ErrorCodes.ACCESS_REFUSED,
              "Attempt to declare exchange: '" + name + "' which begins with reserved prefix.");
        } else if (getVHostMetadata(context).getExchanges().containsKey(name)) {
          exchange = getExchange(context, name);
          if (!exchange.getType().toString().equals(typeString)
              || !exchange.getLifetimePolicy().equals(lifetimePolicy)
              || exchange.isDurable() != durable) {
            closeChannel(
                ErrorCodes.IN_USE,
                "Attempt to redeclare exchange with different parameters: '" + name + ".");
          } else {
            if (!nowait) {
              _connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
            }
          }
        } else {
          Failsafe.with(ZK_CONFLICT_RETRY)
              .runAsync(
                  () -> {
                    Versioned<ContextMetadata> newContext = newContextMetadata();
                    getVHostMetadata(newContext.model())
                        .getExchanges()
                        .put(
                            name,
                            new ExchangeMetadata(
                                AbstractExchange.convertType(exchangeType),
                                durable,
                                autoDelete
                                    ? LifetimePolicy.DELETE_ON_NO_LINKS
                                    : LifetimePolicy.PERMANENT));

                    try {
                      saveContext(newContext).toCompletableFuture().get();
                    } catch (ExecutionException e) {
                      throw e.getCause();
                    }
                  })
              .thenRun(
                  () -> {
                    if (!nowait) {
                      _connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
                    }
                  })
              .exceptionally(
                  t -> {
                    String errorMessage =
                        "Error while saving new exchange in configuration store: '"
                            + exchangeName
                            + "'";
                    LOGGER.error(errorMessage, t);
                    _connection.sendConnectionClose(
                        ErrorCodes.INTERNAL_ERROR, errorMessage, getChannelId());
                    return null;
                  });
        }
      } catch (IllegalArgumentException e) {
        String errorMessage =
            "Unknown exchange type '" + typeString + "' for exchange '" + name + "'";
        LOGGER.warn(errorMessage, e);
        _connection.sendConnectionClose(ErrorCodes.COMMAND_INVALID, errorMessage, getChannelId());
      }
    }
  }

  @Override
  public void receiveExchangeDelete(AMQShortString exchangeStr, boolean ifUnused, boolean nowait) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] ExchangeDelete["
              + " exchange: "
              + exchangeStr
              + " ifUnused: "
              + ifUnused
              + " nowait: "
              + nowait
              + " ]");
    }

    final String exchangeName =
        exchangeStr == null ? ExchangeDefaults.DEFAULT_EXCHANGE_NAME : exchangeStr.toString();

    Versioned<ContextMetadata> versionedContext = getContextMetadata();
    ContextMetadata context = versionedContext.model();
    ExchangeMetadata exchange = getExchange(context, exchangeName);
    if (exchange == null) {
      // Note: Since v3.2, RabbitMQ delete exchange is idempotent :
      // https://www.rabbitmq.com/specification.html#method-status-exchange.delete so we don't close
      // the channel with NOT_FOUND
      if (!nowait) {
        ExchangeDeleteOkBody responseBody =
            _connection.getMethodRegistry().createExchangeDeleteOkBody();
        _connection.writeFrame(responseBody.generateFrame(getChannelId()));
      }
    } else if (ifUnused && exchange.getBindings().size() > 0) {
      closeChannel(ErrorCodes.IN_USE, "Exchange has bindings");
    } else if (isReservedExchangeName(exchangeName)) {
      closeChannel(ErrorCodes.ACCESS_REFUSED, "Exchange '" + exchangeName + "' cannot be deleted");
    } else {
      Failsafe.with(ZK_CONFLICT_RETRY)
          .runAsync(
              () -> {
                Versioned<ContextMetadata> newContext = newContextMetadata();
                try {
                  deleteExchange(newContext, exchangeName).toCompletableFuture().get();
                } catch (ExecutionException e) {
                  throw e.getCause();
                }
              })
          .thenRun(
              () -> {
                if (!nowait) {
                  ExchangeDeleteOkBody responseBody =
                      _connection.getMethodRegistry().createExchangeDeleteOkBody();
                  _connection.writeFrame(responseBody.generateFrame(getChannelId()));
                }
              })
          .exceptionally(
              throwable -> {
                LOGGER.error(
                    "Error occurred while deleting exchange '"
                        + exchangeName
                        + "' :"
                        + throwable.getMessage(),
                    throwable);
                _connection.sendConnectionClose(
                    ErrorCodes.INTERNAL_ERROR, throwable.getMessage(), getChannelId());
                return null;
              });
    }
  }

  @Override
  public void receiveExchangeBound(
      AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {}

  @Override
  public void receiveQueueDeclare(
      AMQShortString queueStr,
      boolean passive,
      boolean durable,
      boolean exclusive,
      boolean autoDelete,
      boolean nowait,
      FieldTable arguments) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] QueueDeclare["
              + " queue: "
              + queueStr
              + " passive: "
              + passive
              + " durable: "
              + durable
              + " exclusive: "
              + exclusive
              + " autoDelete: "
              + autoDelete
              + " nowait: "
              + nowait
              + " arguments: "
              + arguments
              + " ]");
    }

    final String queueName;

    // if we aren't given a queue name, we create one which we return to the client
    if ((queueStr == null) || (queueStr.length() == 0)) {
      queueName = "auto_" + UUID.randomUUID();
    } else {
      queueName = sanitizeEntityName(queueStr.toString());
    }

    Versioned<ContextMetadata> versionedContext = getContextMetadata();
    ContextMetadata context = versionedContext.model();
    QueueMetadata queue;

    if (passive) {
      queue = getQueue(context, queueName);
      if (queue == null) {
        closeChannel(
            ErrorCodes.NOT_FOUND,
            "Queue: '"
                + queueName
                + "' not found on VirtualHost '"
                + _connection.getNamespace()
                + "'.");
      } else {
        // TODO: check exclusive queue access
        // set this as the default queue on the channel:
        setDefaultQueueName(queueName);
        if (!nowait) {
          MethodRegistry methodRegistry = _connection.getMethodRegistry();
          QueueDeclareOkBody responseBody =
              methodRegistry.createQueueDeclareOkBody(
                  AMQShortString.createAMQShortString(queueName),
                  queue.getQueueDepthMessages(),
                  queue.getConsumerCount());
          _connection.writeFrame(responseBody.generateFrame(getChannelId()));

          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Queue " + queueName + " declared successfully");
          }
        }
      }
    } else {
      try {
        LifetimePolicy lifetimePolicy;
        ExclusivityPolicy exclusivityPolicy;

        if (exclusive) {
          lifetimePolicy =
              autoDelete
                  ? LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS
                  : durable ? LifetimePolicy.PERMANENT : LifetimePolicy.DELETE_ON_CONNECTION_CLOSE;
          exclusivityPolicy = durable ? ExclusivityPolicy.CONTAINER : ExclusivityPolicy.CONNECTION;
        } else {
          lifetimePolicy =
              autoDelete ? LifetimePolicy.DELETE_ON_NO_OUTBOUND_LINKS : LifetimePolicy.PERMANENT;
          exclusivityPolicy = ExclusivityPolicy.NONE;
        }

        Map<String, Object> attributes = FieldTable.convertToMap(arguments);

        if (attributes.containsKey(org.apache.qpid.server.model.Queue.EXCLUSIVE)) {
          exclusivityPolicy =
              ExclusivityPolicy.valueOf(
                  attributes.get(org.apache.qpid.server.model.Queue.EXCLUSIVE).toString());
        }
        if (attributes.containsKey(org.apache.qpid.server.model.Queue.LIFETIME_POLICY)) {
          lifetimePolicy =
              LifetimePolicy.valueOf(
                  attributes.get(org.apache.qpid.server.model.Queue.LIFETIME_POLICY).toString());
        }

        queue = getQueue(context, queueName);
        if (queue != null) {
          // TODO; verify if queue is exclusive and opened by another connection
          if (queue.isExclusive() != exclusive) {

            closeChannel(
                ErrorCodes.ALREADY_EXISTS,
                "Cannot re-declare queue '"
                    + queueName
                    + "' with different exclusivity (was: "
                    + queue.isExclusive()
                    + " requested "
                    + exclusive
                    + ")");
          } else if (durable != queue.isDurable()) {
            closeChannel(
                ErrorCodes.IN_USE,
                "Cannot re-declare queue '"
                    + queueName
                    + "' with different durability (was: "
                    + queue.isDurable()
                    + " requested durable: "
                    + durable
                    + ")");
          } else if ((autoDelete && queue.getLifetimePolicy() == LifetimePolicy.PERMANENT)
              || (!autoDelete
                  && queue.getLifetimePolicy()
                      != ((exclusive && !durable)
                          ? LifetimePolicy.DELETE_ON_CONNECTION_CLOSE
                          : LifetimePolicy.PERMANENT))) {
            closeChannel(
                ErrorCodes.IN_USE,
                "Cannot re-declare queue '"
                    + queueName
                    + "' with different lifetime policy (was: "
                    + queue.getLifetimePolicy()
                    + " requested autodelete: "
                    + autoDelete
                    + ")");
          } else {
            setDefaultQueueName(queueName);
            if (!nowait) {
              MethodRegistry methodRegistry = _connection.getMethodRegistry();
              QueueDeclareOkBody responseBody =
                  methodRegistry.createQueueDeclareOkBody(
                      AMQShortString.createAMQShortString(queueName),
                      queue.getQueueDepthMessages(),
                      queue.getConsumerCount());
              _connection.writeFrame(responseBody.generateFrame(getChannelId()));

              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Queue " + queueName + " declared successfully");
              }
            }
          }
        } else {
          LifetimePolicy lifetimePolicy_ = lifetimePolicy;
          ExclusivityPolicy exclusivityPolicy_ = exclusivityPolicy;

          Failsafe.with(ZK_CONFLICT_RETRY)
              .runAsync(
                  () -> {
                    Versioned<ContextMetadata> newContext = newContextMetadata();
                    VirtualHostMetadata vhost =
                        newContext.model().getVhosts().get(_connection.getNamespace());
                    vhost
                        .getQueues()
                        .putIfAbsent(
                            queueName,
                            new QueueMetadata(durable, lifetimePolicy_, exclusivityPolicy_));

                    try {
                      bindQueue(
                              newContext,
                              ExchangeDefaults.DEFAULT_EXCHANGE_NAME,
                              queueName,
                              queueName)
                          .get();
                    } catch (ExecutionException e) {
                      throw e.getCause();
                    }
                  })
              .thenRun(
                  () -> {
                    setDefaultQueueName(queueName);

                    if (!nowait) {
                      MethodRegistry methodRegistry = _connection.getMethodRegistry();
                      QueueDeclareOkBody responseBody =
                          methodRegistry.createQueueDeclareOkBody(
                              AMQShortString.createAMQShortString(queueName), 0, 0);
                      _connection.writeFrame(responseBody.generateFrame(getChannelId()));

                      if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Queue " + queueName + " declared successfully");
                      }
                    }
                  })
              .exceptionally(
                  t -> {
                    String errorMessage =
                        "Error while binding queue: '" + queueName + "' to default exchange";
                    LOGGER.error(errorMessage, t);
                    closeChannel(ErrorCodes.INTERNAL_ERROR, errorMessage);
                    return null;
                  });
        }
      } catch (IllegalArgumentException e) {
        String message = String.format("Error creating queue '%s': %s", queueName, e.getMessage());
        LOGGER.warn(message, e);
        _connection.sendConnectionClose(ErrorCodes.INVALID_ARGUMENT, message, getChannelId());
      }
    }
  }

  @Override
  public void receiveQueueBind(
      AMQShortString queueNameStr,
      AMQShortString exchange,
      AMQShortString bindingKey,
      boolean nowait,
      FieldTable argumentsTable) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] QueueBind["
              + " queue: "
              + queueNameStr
              + " exchange: "
              + exchange
              + " bindingKey: "
              + bindingKey
              + " nowait: "
              + nowait
              + " arguments: "
              + argumentsTable
              + " ]");
    }

    Versioned<ContextMetadata> versionedContext = getContextMetadata();
    ContextMetadata context = versionedContext.model();
    QueueMetadata queue;
    String queueName;
    if (queueNameStr == null) {
      queueName = getDefaultQueueName();
      queue = getQueue(context, queueName);

      if (queue != null) {
        if (bindingKey == null) {
          bindingKey = AMQShortString.valueOf(queueName);
        }
      }
    } else {
      queueName = queueNameStr.toString();
      queue = getQueue(context, queueName);
    }

    if (queue == null) {
      String message =
          queueName == null
              ? "No default queue defined on channel and queue was null"
              : "Queue " + queueName + " does not exist.";
      closeChannel(ErrorCodes.NOT_FOUND, message);
    } else if (isDefaultExchange(exchange)) {
      closeChannel(
          ErrorCodes.ACCESS_REFUSED,
          "Cannot bind the queue '" + queueName + "' to the default exchange");

    } else {
      final String exchangeName = AMQShortString.toString(exchange);

      if (getExchange(context, exchangeName) == null) {
        closeChannel(ErrorCodes.NOT_FOUND, "Exchange '" + exchangeName + "' does not exist.");
      } else {
        String bindingKeyStr = bindingKey == null ? "" : AMQShortString.toString(bindingKey);
        Failsafe.with(ZK_CONFLICT_RETRY)
            .runAsync(
                () -> {
                  Versioned<ContextMetadata> newContext = newContextMetadata();
                  try {
                    bindQueue(newContext, exchangeName, queueName, bindingKeyStr).get();
                  } catch (ExecutionException e) {
                    throw e.getCause();
                  }
                })
            .thenRun(
                () -> {
                  if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Binding queue "
                            + queue
                            + " to exchange "
                            + exchangeName
                            + " with routing key "
                            + bindingKeyStr);
                  }
                  if (!nowait) {
                    MethodRegistry methodRegistry = _connection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
                    _connection.writeFrame(responseBody.generateFrame(getChannelId()));
                  }
                })
            .exceptionally(
                t -> {
                  String errorMessage =
                      "Error while binding queue: '"
                          + queueName
                          + "' to exchange '"
                          + exchangeName
                          + "' with routing key '"
                          + bindingKeyStr
                          + "'";
                  LOGGER.error(errorMessage, t);
                  closeChannel(ErrorCodes.INTERNAL_ERROR, errorMessage);
                  return null;
                });
      }
    }
  }

  @Override
  public void receiveQueuePurge(AMQShortString queueNameStr, boolean nowait) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] QueuePurge["
              + " queue: "
              + queueNameStr
              + " nowait: "
              + nowait
              + " ]");
    }

    String queueName = queueNameStr == null ? getDefaultQueueName() : queueNameStr.toString();

    if (queueName == null) {
      _connection.sendConnectionClose(
          ErrorCodes.NOT_ALLOWED, "No queue specified.", getChannelId());
    } else {
      Queue queue = getQueue(queueName);
      if (queue == null) {
        closeChannel(ErrorCodes.NOT_FOUND, "Queue '" + queueName + "' does not exist.");
      } else {
        long purged = queue.clearQueue();
        if (!nowait) {
          MethodRegistry methodRegistry = _connection.getMethodRegistry();
          AMQMethodBody responseBody = methodRegistry.createQueuePurgeOkBody(purged);
          _connection.writeFrame(responseBody.generateFrame(getChannelId()));
        }
      }
    }
  }

  @Override
  public void receiveQueueDelete(
      AMQShortString queueNameStr, boolean ifUnused, boolean ifEmpty, boolean nowait) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] QueueDelete["
              + " queue: "
              + queueNameStr
              + " ifUnused: "
              + ifUnused
              + " ifEmpty: "
              + ifEmpty
              + " nowait: "
              + nowait
              + " ]");
    }

    Versioned<ContextMetadata> versionedContext = getContextMetadata();
    ContextMetadata context = versionedContext.model();
    String queueName = queueNameStr == null ? getDefaultQueueName() : queueNameStr.toString();
    QueueMetadata queue = getQueue(context, getDefaultQueueName());

    if (queue == null) {
      // Note: Since v3.2, RabbitMQ delete queue is idempotent :
      // https://www.rabbitmq.com/specification.html#method-status-queue.delete so we don't close
      // the channel with NOT_FOUND
      if (!nowait) {
        MethodRegistry methodRegistry = _connection.getMethodRegistry();
        QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(0);
        _connection.writeFrame(responseBody.generateFrame(getChannelId()));
      }
    } else {
      if (ifEmpty && !queue.isEmpty()) {
        closeChannel(ErrorCodes.IN_USE, "Queue: '" + queueName + "' is not empty.");
      } else if (ifUnused && !queue.isUnused()) {
        closeChannel(ErrorCodes.IN_USE, "Queue: '" + queueName + "' is still used.");
      } else {
        // TODO: check if exclusive queue owned by another connection
        Failsafe.with(ZK_CONFLICT_RETRY)
            .runAsync(
                () -> {
                  Versioned<ContextMetadata> newContext = newContextMetadata();
                  VirtualHostMetadata vHostMetadata = getVHostMetadata(newContext.model());

                  // Set last message of the subscription to earliest so it is removed by the
                  // subscription cleaner
                  vHostMetadata
                      .getSubscriptions()
                      .get(queueName)
                      .forEach(
                          (s, subscription) -> subscription.setLastMessageId(EARLIEST_MESSAGE_ID));

                  vHostMetadata
                      .getExchanges()
                      .forEach(
                          (s, exchangeMetadata) ->
                              exchangeMetadata.getBindings().remove(queueName));
                  vHostMetadata.getQueues().remove(queueName);
                  try {
                    saveContext(newContext).toCompletableFuture().get();
                  } catch (ExecutionException e) {
                    throw e.getCause();
                  }
                })
            .thenRun(
                () -> {
                  if (!nowait) {
                    MethodRegistry methodRegistry = _connection.getMethodRegistry();
                    QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(0);
                    _connection.writeFrame(responseBody.generateFrame(getChannelId()));
                  }
                })
            .exceptionally(
                t -> {
                  String errorMessage = "Error while deleting queue: '" + queueName + "'";
                  LOGGER.error(errorMessage, t);
                  closeChannel(ErrorCodes.INTERNAL_ERROR, errorMessage);
                  return null;
                });
      }
    }
  }

  @Override
  public void receiveQueueUnbind(
      AMQShortString queueNameStr,
      AMQShortString exchangeName,
      AMQShortString bindingKey,
      FieldTable arguments) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] QueueUnbind["
              + " queue: "
              + queueNameStr
              + " exchange: "
              + exchangeName
              + " bindingKey: "
              + bindingKey
              + " arguments: "
              + arguments
              + " ]");
    }

    Versioned<ContextMetadata> versionedContext = getContextMetadata();
    ContextMetadata context = versionedContext.model();
    String queueName = queueNameStr == null ? getDefaultQueueName() : queueNameStr.toString();
    QueueMetadata queue = getQueue(context, getDefaultQueueName());

    if (queue == null) {
      final AMQMethodBody responseBody = _connection.getMethodRegistry().createQueueUnbindOkBody();
      _connection.writeFrame(responseBody.generateFrame(getChannelId()));
    } else if (isDefaultExchange(exchangeName)) {
      closeChannel(
          ErrorCodes.ACCESS_REFUSED,
          "Cannot unbind the queue '" + queueNameStr + "' from the default exchange");

    } else {
      // Note: Since v3.2, RabbitMQ unbind queue is idempotent :
      // https://www.rabbitmq.com/specification.html#method-status-queue.delete so we don't close
      // the channel with NOT_FOUND
      final String bindingKeyStr = bindingKey == null ? "" : AMQShortString.toString(bindingKey);

      Failsafe.with(ZK_CONFLICT_RETRY)
          .runAsync(
              () -> {
                Versioned<ContextMetadata> newContext = newContextMetadata();
                ExchangeMetadata exchangeMetadata =
                    getExchange(newContext.model(), exchangeName.toString());
                if (exchangeMetadata != null
                    && exchangeMetadata.getBindings().get(queueName) != null
                    && exchangeMetadata
                        .getBindings()
                        .get(queueName)
                        .getKeys()
                        .contains(bindingKeyStr)) {
                  AbstractExchange exch =
                      AbstractExchange.fromMetadata(exchangeName.toString(), exchangeMetadata);
                  try {
                    exch.unbind(
                            getVHostMetadata(newContext.model()),
                            exchangeName.toString(),
                            queueName,
                            bindingKeyStr,
                            _connection)
                        .thenCompose(it -> saveContext(newContext))
                        .get();
                  } catch (ExecutionException e) {
                    throw e.getCause();
                  }
                }
              })
          .thenRun(
              () -> {
                final AMQMethodBody responseBody =
                    _connection.getMethodRegistry().createQueueUnbindOkBody();
                _connection.writeFrame(responseBody.generateFrame(getChannelId()));
              })
          .exceptionally(
              throwable -> {
                _connection.sendConnectionClose(
                    ErrorCodes.INTERNAL_ERROR, throwable.getMessage(), getChannelId());
                return null;
              });
    }
  }

  @Override
  public void receiveBasicRecover(boolean requeue, boolean sync) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicRecover["
              + " requeue: "
              + requeue
              + " sync: "
              + sync
              + " ]");
    }

    if (requeue) {
      requeue();

      if (sync) {
        MethodRegistry methodRegistry = _connection.getMethodRegistry();
        AMQMethodBody recoverOk = methodRegistry.createBasicRecoverSyncOkBody();
        _connection.writeFrame(recoverOk.generateFrame(getChannelId()));
      }
      unblockConsumers();
    } else {
      // Note: RabbitMQ doesn't support requeue=false.
      // Should it be supported for Qpid clients ? This would mean storing the unacked message
      // content in the proxy :-(
      _connection.sendConnectionClose(
          ErrorCodes.NOT_IMPLEMENTED,
          "Recover to same consumer is currently not supported",
          _channelId);
    }
  }

  @Override
  public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicQos["
              + " prefetchSize: "
              + prefetchSize
              + " prefetchCount: "
              + prefetchCount
              + " global: "
              + global
              + " ]");
    }
    // TODO: per consumer QoS. Warning: RabbitMQ has reinterpreted the "global" field of AMQP. See
    //  https://www.rabbitmq.com/specification.html#method-status-basic.qos
    _creditManager.setCreditLimits(prefetchSize, prefetchCount);

    MethodRegistry methodRegistry = _connection.getMethodRegistry();
    AMQMethodBody responseBody = methodRegistry.createBasicQosOkBody();
    _connection.writeFrame(responseBody.generateFrame(getChannelId()));

    unblockConsumers();
  }

  private void unblockConsumers() {
    _tag2SubscriptionTargetMap.values().forEach(AMQConsumer::unblock);
  }

  @Override
  public void receiveBasicConsume(
      AMQShortString queueNameStr,
      AMQShortString consumerTag,
      boolean noLocal,
      boolean noAck,
      boolean exclusive,
      boolean nowait,
      FieldTable arguments) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicConsume["
              + " queue: "
              + queueNameStr
              + " consumerTag: "
              + consumerTag
              + " noLocal: "
              + noLocal
              + " noAck: "
              + noAck
              + " exclusive: "
              + exclusive
              + " nowait: "
              + nowait
              + " arguments: "
              + arguments
              + " ]");
    }

    AMQShortString consumerTag1 = consumerTag;

    String queueName = queueNameStr == null ? getDefaultQueueName() : queueNameStr.toString();
    if (queueName == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No queue name provided, no default queue defined.");
      }
      _connection.sendConnectionClose(
          ErrorCodes.NOT_ALLOWED, "No queue name provided, no default queue defined.", _channelId);
    } else {
      Queue queue = getQueue(queueName);
      if (queue == null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("No queue for '" + queueName + "'");
        }
        closeChannel(ErrorCodes.NOT_FOUND, "No such queue, '" + queueName + "'");
      } else {
        if (consumerTag1 == null) {
          consumerTag1 = AMQShortString.createAMQShortString("sgen_" + getNextConsumerTag());
        }
        // TODO: check exclusive queue owned by another connection
        if (_tag2SubscriptionTargetMap.containsKey(consumerTag1)) {
          _connection.sendConnectionClose(
              ErrorCodes.NOT_ALLOWED,
              "Non-unique consumer tag, '" + consumerTag1 + "'",
              _channelId);
        } else if (queue.hasExclusiveConsumer()) {
          _connection.sendConnectionClose(
              ErrorCodes.ACCESS_REFUSED,
              "Cannot subscribe to queue '"
                  + queueName
                  + "' as it already has an existing exclusive consumer",
              _channelId);
        } else if (exclusive && queue.getConsumerCount() != 0) {
          _connection.sendConnectionClose(
              ErrorCodes.ACCESS_REFUSED,
              "Cannot subscribe to queue '"
                  + queueName
                  + "' exclusively as it already has a consumer",
              _channelId);
        } else {
          if (!nowait) {
            MethodRegistry methodRegistry = _connection.getMethodRegistry();
            AMQMethodBody responseBody = methodRegistry.createBasicConsumeOkBody(consumerTag1);
            _connection.writeFrame(responseBody.generateFrame(_channelId));
          }
          AMQConsumer consumer = new AMQConsumer(this, consumerTag1, queue, noAck);
          queue.addConsumer(consumer, exclusive);
          _tag2SubscriptionTargetMap.put(consumerTag1, consumer);
        }
      }
    }
  }

  @Override
  public void receiveBasicCancel(AMQShortString consumerTag, boolean nowait) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicCancel["
              + " consumerTag: "
              + consumerTag
              + " noWait: "
              + nowait
              + " ]");
    }

    unsubscribeConsumer(consumerTag);
    if (!nowait) {
      MethodRegistry methodRegistry = _connection.getMethodRegistry();
      BasicCancelOkBody cancelOkBody = methodRegistry.createBasicCancelOkBody(consumerTag);
      _connection.writeFrame(cancelOkBody.generateFrame(_channelId));
    }
  }

  @Override
  public void receiveBasicPublish(
      AMQShortString exchangeNameStr,
      AMQShortString routingKey,
      boolean mandatory,
      boolean immediate) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicPublish["
              + " exchange: "
              + exchangeNameStr
              + " routingKey: "
              + routingKey
              + " mandatory: "
              + mandatory
              + " immediate: "
              + immediate
              + " ]");
    }

    String exchangeName =
        exchangeNameStr == null
            ? ExchangeDefaults.DEFAULT_EXCHANGE_NAME
            : exchangeNameStr.toString();
    if (!getVHostMetadata(getContextMetadata().model()).getExchanges().containsKey(exchangeName)) {
      closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange name: '" + exchangeName + "'");
    }

    MessagePublishInfo info =
        new MessagePublishInfo(exchangeNameStr, immediate, mandatory, routingKey);

    setPublishFrame(info);
  }

  private Pair<Consumer<byte[]>, Message<byte[]>> getMessageFromConsumers(
      Iterable<Pair<Consumer<byte[]>, MessageId>> subscriptions) {
    Iterator<Pair<Consumer<byte[]>, MessageId>> subscriptionIterator = subscriptions.iterator();
    while (subscriptionIterator.hasNext()) {
      Pair<Consumer<byte[]>, MessageId> subscription = subscriptionIterator.next();
      try {
        Consumer<byte[]> consumer = subscription.getLeft();
        consumer.pause();
        Message<byte[]> receive = consumer.receive(0, TimeUnit.SECONDS);
        if (receive != null) {
          if (subscription.getRight() != null
              && receive.getMessageId().compareTo(subscription.getRight()) > 0) {
            consumer.closeAsync();
            subscriptionIterator.remove();
          } else {

            return Pair.of(consumer, receive);
          }
        } else {
          consumer.resume();
        }
      } catch (PulsarClientException e) {
        LOGGER.debug("Exception while receiving from consumer", e);
      }
    }
    return null;
  }

  private void getMessageFromConsumers(
      Iterable<Pair<Consumer<byte[]>, MessageId>> consumers,
      int numberOfRetries,
      ScheduledExecutorService scheduledExecutorService,
      CompletableFuture<Pair<Consumer<byte[]>, Message<byte[]>>> messageCompletableFuture) {
    Pair<Consumer<byte[]>, Message<byte[]>> message = getMessageFromConsumers(consumers);
    if (message == null && numberOfRetries > 0) {
      scheduledExecutorService.schedule(
          () ->
              getMessageFromConsumers(
                  consumers,
                  numberOfRetries - 1,
                  scheduledExecutorService,
                  messageCompletableFuture),
          10,
          TimeUnit.MILLISECONDS);
    } else {
      messageCompletableFuture.complete(message);
    }
  }

  @Override
  public void receiveBasicGet(AMQShortString queueNameStr, boolean noAck) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicGet["
              + " queue: "
              + queueNameStr
              + " noAck: "
              + noAck
              + " ]");
    }
    String queueName = queueNameStr == null ? getDefaultQueueName() : queueNameStr.toString();

    if (queueName == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No queue name provided, no default queue defined.");
      }
      _connection.sendConnectionClose(
          ErrorCodes.NOT_ALLOWED, "No queue name provided, no default queue defined.", _channelId);
    } else {
      Queue queue = getQueue(queueName);
      if (queue == null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("No queue for '" + queueName + "'");
        }
        _connection.sendConnectionClose(
            ErrorCodes.NOT_FOUND, "No such queue, '" + queueName + "'", _channelId);
      } else {
        // TODO: check exclusive queue owned by another connection
        if (queue.hasExclusiveConsumer()) {
          _connection.sendConnectionClose(
              ErrorCodes.ACCESS_REFUSED,
              "Cannot subscribe to queue '"
                  + queueName
                  + "' as it already has an existing exclusive consumer",
              _channelId);
        } else {
          VirtualHostMetadata vHostMetadata = getVHostMetadata(getContextMetadata().model());
          Map<String, BindingMetadata> subscriptions =
              vHostMetadata.getSubscriptions().get(queueName);
          ConcurrentLinkedQueue<Pair<Consumer<byte[]>, MessageId>> consumers =
              new ConcurrentLinkedQueue<>();
          CompletableFuture<Pair<Consumer<byte[]>, Message<byte[]>>> messageCompletableFuture =
              new CompletableFuture<>();
          subscriptions.forEach(
              (subscription, bindingMetadata) -> {
                if (!Arrays.equals(EARLIEST_MESSAGE_ID, bindingMetadata.getLastMessageId())) {
                  getGatewayService()
                      .getPulsarClient()
                      .newConsumer()
                      .topic(bindingMetadata.getTopic())
                      .receiverQueueSize(1)
                      .isAckReceiptEnabled(true)
                      .subscriptionName(subscription)
                      .subscriptionType(SubscriptionType.Shared)
                      .negativeAckRedeliveryDelay(0, TimeUnit.MILLISECONDS)
                      .enableBatchIndexAcknowledgment(true)
                      .subscribeAsync()
                      .thenAccept(
                          consumer -> {
                            if (!messageCompletableFuture.isDone()) {
                              try {
                                MessageId lastMessageId =
                                    bindingMetadata.getLastMessageId() == null
                                        ? null
                                        : MessageId.fromByteArray(
                                            bindingMetadata.getLastMessageId());
                                consumers.add(Pair.of(consumer, lastMessageId));
                              } catch (IOException e) {
                                LOGGER.error(
                                    "Error while deserializing binding's lastMessageId", e);
                                consumer.closeAsync();
                              }
                            } else {
                              consumer.closeAsync();
                            }
                          });
                }
              });

          getMessageFromConsumers(
              consumers, 100, getGatewayService().getWorkerGroup(), messageCompletableFuture);

          messageCompletableFuture.thenAccept(
              consumerMessage -> {
                if (consumerMessage != null) {
                  Message<byte[]> message = consumerMessage.getRight();
                  Consumer<byte[]> consumer = consumerMessage.getLeft();
                  consumers.removeIf(
                      subscription ->
                          subscription
                              .getLeft()
                              .getSubscription()
                              .equals(consumer.getSubscription()));
                  long deliveryTag = getNextDeliveryTag();
                  ContentBody contentBody = new ContentBody(ByteBuffer.wrap(message.getData()));
                  if (!noAck) {
                    BasicGetMessageConsumerAssociation messageConsumerAssociation =
                        new BasicGetMessageConsumerAssociation(
                            message.getMessageId(), consumer, contentBody.getSize());
                    addUnacknowledgedMessage(deliveryTag, messageConsumerAssociation);
                  }
                  _connection
                      .getProtocolOutputConverter()
                      .writeGetOk(
                          MessageUtils.getMessagePublishInfo(message),
                          contentBody,
                          MessageUtils.getContentHeaderBody(message),
                          message.getRedeliveryCount() > 0,
                          _channelId,
                          deliveryTag,
                          queue.getQueueDepthMessages());
                  if (noAck) {
                    consumer.acknowledgeAsync(message.getMessageId()).thenRun(consumer::closeAsync);
                  }
                } else {
                  MethodRegistry methodRegistry = _connection.getMethodRegistry();
                  BasicGetEmptyBody responseBody = methodRegistry.createBasicGetEmptyBody(null);
                  _connection.writeFrame(responseBody.generateFrame(_channelId));
                }
                consumers.forEach(subscription -> subscription.getLeft().closeAsync());
              });
        }
      }
    }
  }

  @Override
  public void receiveChannelFlow(boolean active) {}

  @Override
  public void receiveChannelFlowOk(boolean active) {}

  @Override
  public void receiveChannelClose(
      int replyCode, AMQShortString replyText, int classId, int methodId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] ChannelClose["
              + " replyCode: "
              + replyCode
              + " replyText: "
              + replyText
              + " classId: "
              + classId
              + " methodId: "
              + methodId
              + " ]");
    }
    _connection.closeChannel(this);

    _connection.writeFrame(
        new AMQFrame(getChannelId(), _connection.getMethodRegistry().createChannelCloseOkBody()));
  }

  @Override
  public void receiveChannelCloseOk() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RECV[" + _channelId + "] ChannelCloseOk");
    }

    _connection.closeChannelOk(getChannelId());
  }

  @Override
  public void receiveMessageContent(QpidByteBuffer data) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] MessageContent["
              + " data: "
              + hex(data, getConfiguration().getAmqpDebugBinaryDataLength())
              + " ] ");
    }

    if (hasCurrentMessage()) {
      publishContentBody(new ContentBody(data));
    } else {
      _connection.sendConnectionClose(
          // Note: RabbitMQ sends error 505 while Qpid sends 503
          505,
          "Attempt to send a content header without first sending a publish frame",
          _channelId);
    }
  }

  @Override
  public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV[{}] MessageHeader[ properties: {{}} bodySize: {}]",
          _channelId,
          properties,
          bodySize);
    }

    if (hasCurrentMessage()) {
      int maxMessageSize = getConfiguration().getAmqpMaxMessageSize();
      if (bodySize > maxMessageSize) {
        properties.dispose();
        closeChannel(
            ErrorCodes.MESSAGE_TOO_LARGE,
            "Message size of " + bodySize + " greater than allowed maximum of " + maxMessageSize);
      } else {
        if (properties.checkValid()) {
          publishContentHeader(new ContentHeaderBody(properties, bodySize));
        } else {
          properties.dispose();
          _connection.sendConnectionClose(
              ErrorCodes.FRAME_ERROR, "Attempt to send a malformed content header", _channelId);
        }
      }
    } else {
      properties.dispose();
      _connection.sendConnectionClose(
          // Note: RabbitMQ sends error 505 while Qpid sends 503
          505,
          "Attempt to send a content header without first sending a publish frame",
          _channelId);
    }
  }

  @Override
  public boolean ignoreAllButCloseOk() {
    return false;
  }

  @Override
  public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicNack["
              + " deliveryTag: "
              + deliveryTag
              + " multiple: "
              + multiple
              + " requeue: "
              + requeue
              + " ]");
    }
    // Note : A delivery tag equal to 0 nacks all messages in RabbitMQ, not in Qpid
    nackMessages(deliveryTag == 0 && multiple ? _deliveryTag : deliveryTag, multiple, requeue);
  }

  @Override
  public void receiveBasicAck(long deliveryTag, boolean multiple) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicAck["
              + " deliveryTag: "
              + deliveryTag
              + " multiple: "
              + multiple
              + " ]");
    }

    Collection<MessageConsumerAssociation> ackedMessages =
        _unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);

    // TODO: optimize by acknowledging multiple message IDs at once
    if (!ackedMessages.isEmpty()) {
      ackedMessages.forEach(MessageConsumerAssociation::ack);
      unblockConsumers();
    } else {
      // Note: This error is sent by RabbitMQ but not by Qpid
      closeChannel(ErrorCodes.IN_USE, "precondition-failed: Delivery tag '%d' is not valid.");
    }
  }

  @Override
  public void receiveBasicReject(long deliveryTag, boolean requeue) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicReject["
              + " deliveryTag: "
              + deliveryTag
              + " requeue: "
              + requeue
              + " ]");
    }
    nackMessages(deliveryTag, false, requeue);
  }

  private void nackMessages(long deliveryTag, boolean multiple, boolean requeue) {
    Collection<MessageConsumerAssociation> nackedMessages =
        _unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);

    for (MessageConsumerAssociation unackedMessageConsumerAssociation : nackedMessages) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Nack-ing: DT:"
                + deliveryTag
                + "-"
                + Arrays.toString(unackedMessageConsumerAssociation.getMessageId().toByteArray())
                + ": Requeue:"
                + requeue);
      }

      if (requeue) {
        unackedMessageConsumerAssociation.requeue();
      } else {
        // TODO: send message to DLQ
        unackedMessageConsumerAssociation.ack();
      }
    }
    if (!nackedMessages.isEmpty()) {
      unblockConsumers();
    } else {
      // Note: This error is sent by RabbitMQ but not by Qpid
      closeChannel(ErrorCodes.IN_USE, "precondition-failed: Delivery tag '%d' is not valid.");
    }
  }

  @Override
  public void receiveTxSelect() {}

  @Override
  public void receiveTxCommit() {}

  @Override
  public void receiveTxRollback() {}

  @Override
  public void receiveConfirmSelect(boolean nowait) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RECV[" + _channelId + "] ConfirmSelect [ nowait: " + nowait + " ]");
    }
    _confirmOnPublish = true;

    if (!nowait) {
      _connection.writeFrame(new AMQFrame(_channelId, ConfirmSelectOkBody.INSTANCE));
    }
  }

  private void publishContentHeader(ContentHeaderBody contentHeaderBody) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Content header received on channel " + _channelId);
    }

    _currentMessage.setContentHeaderBody(contentHeaderBody);

    deliverCurrentMessageIfComplete();
  }

  private void publishContentBody(ContentBody contentBody) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Content body received on channel " + _channelId);
    }

    try {
      if (_currentMessage.getContentHeader() == null) {
        _connection.sendConnectionClose(
            505, "Content body received without content header", _channelId);
        return;
      }
      long currentSize = _currentMessage.addContentBodyFrame(contentBody);
      if (currentSize > _currentMessage.getSize()) {
        _connection.sendConnectionClose(
            ErrorCodes.FRAME_ERROR,
            "More message data received than content header defined",
            _channelId);
      } else {
        deliverCurrentMessageIfComplete();
      }
    } catch (RuntimeException e) {
      // we want to make sure we don't keep a reference to the message in the
      // event of an error
      _currentMessage = null;
      throw e;
    }
  }

  public long getNextDeliveryTag() {
    return ++_deliveryTag;
  }

  private int getNextConsumerTag() {
    return ++_consumerTag;
  }

  private void deliverCurrentMessageIfComplete() {
    // check and deliver if header says body length is zero
    if (_currentMessage.allContentReceived()) {
      MessagePublishInfo info = _currentMessage.getMessagePublishInfo();
      String routingKey = AMQShortString.toString(info.getRoutingKey());
      String exchangeName =
          info.getExchange() == null
              ? ExchangeDefaults.DEFAULT_EXCHANGE_NAME
              : info.getExchange().toString();

      Producer<byte[]> producer;
      try {
        producer = getOrCreateProducerForExchange(exchangeName, routingKey);
      } catch (Exception e) {
        _connection.sendConnectionClose(
            ErrorCodes.INTERNAL_ERROR,
            String.format(
                "Failed in creating producer for exchange [%s] and routing key [%s]",
                exchangeName, routingKey),
            _channelId);
        return;
      }

      TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage();
      QpidByteBuffer qpidByteBuffer = QpidByteBuffer.emptyQpidByteBuffer();
      int bodyCount = _currentMessage.getBodyCount();
      if (bodyCount > 0) {
        for (int i = 0; i < bodyCount; i++) {
          ContentBody contentChunk = _currentMessage.getContentChunk(i);
          qpidByteBuffer = QpidByteBuffer.concatenate(qpidByteBuffer, contentChunk.getPayload());
          contentChunk.dispose();
        }
      }
      byte[] value = new byte[qpidByteBuffer.remaining()];
      qpidByteBuffer.copyTo(value);
      messageBuilder.value(value);

      messageBuilder.property(
          MessageUtils.MESSAGE_PROPERTY_AMQP_IMMEDIATE, String.valueOf(info.isImmediate()));
      boolean mandatory = info.isMandatory();
      messageBuilder.property(
          MessageUtils.MESSAGE_PROPERTY_AMQP_MANDATORY, String.valueOf(mandatory));

      ContentHeaderBody contentHeader = _currentMessage.getContentHeader();
      byte[] bytes = new byte[contentHeader.getSize()];
      QpidByteBuffer buf = QpidByteBuffer.wrap(bytes);
      contentHeader.writePayload(buf);

      messageBuilder.property(
          MessageUtils.MESSAGE_PROPERTY_AMQP_HEADERS, Base64.getEncoder().encodeToString(bytes));
      if (contentHeader.getProperties().getTimestamp() > 0) {
        messageBuilder.eventTime(contentHeader.getProperties().getTimestamp());
      }

      QpidByteBuffer returnPayload = qpidByteBuffer.rewind();

      messageBuilder
          .sendAsync()
          .thenAccept(
              messageId -> {
                if (_confirmOnPublish) {
                  _confirmedMessageCounter++;
                  _connection.writeFrame(
                      new AMQFrame(_channelId, new BasicAckBody(_confirmedMessageCounter, false)));
                }
              })
          .exceptionally(
              throwable -> {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug(
                      "Unroutable message exchange='{}', routing key='{}', mandatory={},"
                          + " confirmOnPublish={}",
                      exchangeName,
                      routingKey,
                      mandatory,
                      _confirmOnPublish,
                      throwable);
                }

                /* TODO: review this error management.
                 *  NO_ROUTE should probably be checked before (no queue bound to exchange and
                 *  mandatory flag)
                 *  NO_CONSUMERS should probably be checked before (no consumer to queue bound to
                 *  exchange and immediate flag)
                 *  Random PulsarClientException should Nack with INTERNAL_ERROR. See https://www.rabbitmq.com/confirms.html#server-sent-nacks
                 */
                int errorCode = ErrorCodes.NO_ROUTE;
                String errorMessage =
                    String.format(
                        "No route for message with exchange '%s' and routing key '%s'",
                        exchangeName, routingKey);
                if (throwable instanceof PulsarClientException.ProducerQueueIsFullError) {
                  errorCode = ErrorCodes.RESOURCE_ERROR;
                  errorMessage = errorMessage + ":" + throwable.getMessage();
                }
                if (mandatory || info.isImmediate()) {
                  _connection
                      .getProtocolOutputConverter()
                      .writeReturn(
                          info,
                          contentHeader,
                          returnPayload,
                          _channelId,
                          errorCode,
                          AMQShortString.validValueOf(errorMessage));
                  if (_confirmOnPublish) {
                    _confirmedMessageCounter++;
                    _connection.writeFrame(
                        new AMQFrame(
                            _channelId, new BasicNackBody(_confirmedMessageCounter, false, false)));
                  }
                } else {
                  if (_confirmOnPublish) {
                    _confirmedMessageCounter++;
                    _connection.writeFrame(
                        new AMQFrame(
                            _channelId, new BasicAckBody(_confirmedMessageCounter, false)));
                  }
                }
                return null;
              });
    }
    // TODO: auth, immediate, mandatory, closeOnNoRoute
  }

  private void setPublishFrame(MessagePublishInfo info) {
    _currentMessage = new IncomingMessage(info);
  }

  private void closeChannel(int cause, final String message) {
    _connection.closeChannelAndWriteFrame(this, cause, message);
  }

  @Override
  public String toString() {
    return "("
        + _closing.get()
        + ", "
        + _connection.isClosing()
        + ") "
        + "["
        + _connection
        + ":"
        + _channelId
        + "]";
  }

  public boolean isClosing() {
    return _closing.get() || getConnection().isClosing();
  }

  public GatewayConnection getConnection() {
    return _connection;
  }

  private boolean hasCurrentMessage() {
    return _currentMessage != null;
  }

  public int getChannelId() {
    return _channelId;
  }

  /**
   * Unsubscribe a consumer from a queue.
   *
   * @param consumerTag
   * @return true if the consumerTag had a mapped queue that could be unregistered.
   */
  public boolean unsubscribeConsumer(AMQShortString consumerTag) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Unsubscribing consumer '{}' on channel {}", consumerTag, this);
    }

    AMQConsumer target = _tag2SubscriptionTargetMap.remove(consumerTag);
    if (target != null) {
      target.close();
      return true;
    } else {
      LOGGER.warn(
          "Attempt to unsubscribe consumer with tag '"
              + consumerTag
              + "' which is not registered.");
    }
    return false;
  }

  public void close() {
    close(0, null);
  }

  public void close(int cause, String message) {
    if (!_closing.compareAndSet(false, true)) {
      // Channel is already closing
      return;
    }
    try {
      unsubscribeAllConsumers();
      setDefaultQueueName(null);
      requeue();
    } finally {
      LogMessage operationalLogMessage =
          cause == 0 ? ChannelMessages.CLOSE() : ChannelMessages.CLOSE_FORCED(cause, message);
      messageWithSubject(operationalLogMessage);
    }
  }

  private void unsubscribeAllConsumers() {
    if (LOGGER.isDebugEnabled()) {
      if (!_tag2SubscriptionTargetMap.isEmpty()) {
        LOGGER.debug("Unsubscribing all consumers on channel " + toString());
      } else {
        LOGGER.debug("No consumers to unsubscribe on channel " + toString());
      }
    }

    Set<AMQShortString> subscriptionTags = new HashSet<>(_tag2SubscriptionTargetMap.keySet());
    for (AMQShortString tag : subscriptionTags) {
      unsubscribeConsumer(tag);
    }
  }

  /** Add a message to the channel-based list of unacknowledged messages */
  public void addUnacknowledgedMessage(
      long deliveryTag, MessageConsumerAssociation messageConsumerAssociation) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Adding unacked message("
              + messageConsumerAssociation.getMessageId()
              + " DT:"
              + deliveryTag
              + ")");
    }

    _unacknowledgedMessageMap.add(deliveryTag, messageConsumerAssociation);
  }

  /**
   * Called to attempt re-delivery all outstanding unacknowledged messages on the channel. May
   * result in delivery to this same channel or to other subscribers.
   */
  private void requeue() {
    final Map<Long, MessageConsumerAssociation> copy = _unacknowledgedMessageMap.all();

    if (!copy.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Requeuing {} unacked messages", copy.size());
      }
    }

    for (Map.Entry<Long, MessageConsumerAssociation> entry : copy.entrySet()) {
      // here we wish to restore credit
      _unacknowledgedMessageMap.remove(entry.getKey(), true);
      entry.getValue().requeue();
    }
  }

  private void messageWithSubject(final LogMessage operationalLogMessage) {
    Logger logger = LoggerFactory.getLogger(operationalLogMessage.getLogHierarchy());
    logger.info(operationalLogMessage.toString());
  }

  private boolean isDefaultExchange(final AMQShortString exchangeName) {
    return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
  }

  private void setDefaultQueueName(String queue) {
    defaultQueueName = queue;
  }

  private String getDefaultQueueName() {
    return defaultQueueName;
  }

  private VirtualHostMetadata getVHostMetadata(ContextMetadata contextMetadata) {
    return contextMetadata.getVhosts().get(_connection.getNamespace());
  }

  private Versioned<ContextMetadata> getContextMetadata() {
    return getGatewayService().getContextMetadata();
  }

  private ExchangeMetadata getExchange(ContextMetadata context, String name) {
    return getVHostMetadata(context).getExchanges().get(name);
  }

  private CompletionStage<ContextMetadata> deleteExchange(
      Versioned<ContextMetadata> context, String exchange) {
    VirtualHostMetadata vHost = getVHostMetadata(context.model());
    ExchangeMetadata exchangeMetadata = vHost.getExchanges().get(exchange);
    AbstractExchange exch = AbstractExchange.fromMetadata(exchange, exchangeMetadata);
    List<CompletableFuture<Void>> unbinds = new ArrayList<>();
    exchangeMetadata
        .getBindings()
        .forEach(
            (queue, bindingSetMetadata) ->
                bindingSetMetadata
                    .getKeys()
                    .forEach(
                        routingKey ->
                            unbinds.add(
                                exch.unbind(vHost, exchange, queue, routingKey, _connection))));
    return CompletableFuture.allOf(unbinds.toArray(new CompletableFuture[0]))
        .thenCompose(
            it -> {
              vHost.getExchanges().remove(exchange);
              return saveContext(context);
            });
  }

  private Queue getQueue(String name) {
    return getGatewayService().getQueues().get(_connection.getNamespace()).get(name);
  }

  private QueueMetadata getQueue(ContextMetadata context, String queueName) {
    return context.getVhosts().get(_connection.getNamespace()).getQueues().get(queueName);
  }

  public CompletableFuture<ContextMetadata> bindQueue(
      Versioned<ContextMetadata> context, String exchange, String queue, String routingKey) {
    ExchangeMetadata exchangeMetadata = getExchange(context.model(), exchange);
    if (exchangeMetadata == null) {
      return CompletableFuture.completedFuture(context.model());
    }
    exchangeMetadata.getBindings().putIfAbsent(queue, new BindingSetMetadata());
    AbstractExchange exch = AbstractExchange.fromMetadata(exchange, exchangeMetadata);
    return exch.bind(getVHostMetadata(context.model()), exchange, queue, routingKey, _connection)
        .thenCompose(it -> saveContext(context));
  }

  private boolean isReservedExchangeName(String name) {
    return name == null
        || ExchangeDefaults.DEFAULT_EXCHANGE_NAME.equals(name)
        || name.startsWith("amq.")
        || name.startsWith("qpid.");
  }

  private String sanitizeExchangeName(AMQShortString exchange) {
    String name = exchange == null ? ExchangeDefaults.DEFAULT_EXCHANGE_NAME : exchange.toString();
    return sanitizeEntityName(name);
  }

  private String sanitizeEntityName(String entityName) {
    // RabbitMQ strips CR and LR
    return entityName.replace("\r", "").replace("\n", "");
  }

  private Producer<byte[]> getOrCreateProducerForExchange(String exchangeName, String routingKey) {
    ExchangeMetadata exchangeMetadata = getExchange(getContextMetadata().model(), exchangeName);
    String key =
        ExchangeMetadata.Type.fanout.equals(exchangeMetadata.getType())
                || ExchangeMetadata.Type.headers.equals(exchangeMetadata.getType())
            ? ""
            : routingKey;

    TopicName topicName = getTopicName(_connection.getNamespace(), exchangeName, key);

    return producers.computeIfAbsent(topicName.toString(), this::createProducer);
  }

  private Producer<byte[]> createProducer(String topicName) {
    try {
      return getGatewayService()
          .getPulsarClient()
          .newProducer()
          .enableBatching(getConfiguration().isAmqpBatchingEnabled())
          .topic(topicName)
          .create();
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  public FlowCreditManager getCreditManager() {
    return _creditManager;
  }

  public Versioned<ContextMetadata> newContextMetadata() {
    return getGatewayService().newContextMetadata(getContextMetadata());
  }

  private CompletionStage<ContextMetadata> saveContext(Versioned<ContextMetadata> newContext) {
    return getGatewayService().saveContext(newContext);
  }

  private GatewayConfiguration getConfiguration() {
    return getGatewayService().getConfig();
  }

  private GatewayService getGatewayService() {
    return _connection.getGatewayService();
  }
}
