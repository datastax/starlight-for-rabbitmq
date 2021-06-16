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

import static org.apache.qpid.server.transport.util.Functions.hex;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
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
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetEmptyBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQChannel implements ServerChannelMethodProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AMQChannel.class);

  private final int _channelId;
  /**
   * The delivery tag is unique per channel. This is pre-incremented before putting into the deliver
   * frame so that value of this represents the <b>last</b> tag sent out
   */
  private volatile long _deliveryTag = 0;

  private Queue _defaultQueue;
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

  public AMQChannel(GatewayConnection connection, int channelId) {
    _connection = connection;
    _channelId = channelId;
  }

  @Override
  public void receiveAccessRequest(
      AMQShortString realm,
      boolean exclusive,
      boolean passive,
      boolean active,
      boolean write,
      boolean read) {}

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

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] ExchangeDeclare["
              + " exchange: "
              + exchangeName
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

    Exchange exchange;
    String name =
        exchangeName == null ? ExchangeDefaults.DEFAULT_EXCHANGE_NAME : exchangeName.toString();

    if (passive) {
      exchange = getExchange(name);
      if (exchange == null) {
        closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange: '" + name + "'");
      } else if (!(type == null || type.length() == 0)
          && !exchange.getType().equals(type.toString())) {

        _connection.sendConnectionClose(
            ErrorCodes.NOT_ALLOWED,
            "Attempt to redeclare exchange: '"
                + name
                + "' of type "
                + exchange.getType()
                + " to "
                + type
                + ".",
            getChannelId());
      } else if (!nowait) {
        _connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
      }

    } else {
      String typeString = type == null ? null : type.toString();

      Exchange.Type exChangeType;
      try {
        exChangeType = Exchange.Type.valueOf(typeString);
        if (isReservedExchangeName(name)) {
          Exchange existing = getExchange(name);
          if (existing == null || !existing.getType().equals(typeString)) {
            _connection.sendConnectionClose(
                ErrorCodes.NOT_ALLOWED,
                "Attempt to declare exchange: '"
                    + exchangeName
                    + "' which begins with reserved prefix.",
                getChannelId());
          } else if (!nowait) {
            _connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
          }
        } else if (_connection.getVhost().hasExchange(name)) {
          exchange = getExchange(name);
          if (!exchange.getType().equals(typeString)) {
            _connection.sendConnectionClose(
                ErrorCodes.NOT_ALLOWED,
                "Attempt to redeclare exchange: '"
                    + exchangeName
                    + "' of type "
                    + exchange.getType()
                    + " to "
                    + type
                    + ".",
                getChannelId());
          } else {
            if (!nowait) {
              _connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
            }
          }
        } else {
          exchange =
              new Exchange(
                  name,
                  exChangeType,
                  durable,
                  autoDelete ? LifetimePolicy.DELETE_ON_NO_LINKS : LifetimePolicy.PERMANENT);
          addExchange(exchange);

          if (!nowait) {
            _connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
          }
        }
      } catch (IllegalArgumentException e) {
        String errorMessage =
            "Unknown exchange type '" + typeString + "' for exchange '" + exchangeName + "'";
        LOGGER.debug(errorMessage, e);
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

    final String exchangeName = exchangeStr.toString();

    final Exchange exchange = getExchange(exchangeName);
    if (exchange == null) {
      closeChannel(ErrorCodes.NOT_FOUND, "No such exchange: '" + exchangeStr + "'");
    } else if (ifUnused && exchange.hasBindings()) {
      closeChannel(ErrorCodes.IN_USE, "Exchange has bindings");
    } else if (isReservedExchangeName(exchangeName)) {
      closeChannel(ErrorCodes.NOT_ALLOWED, "Exchange '" + exchangeStr + "' cannot be deleted");
    } else {
      deleteExchange(exchange);

      if (!nowait) {
        ExchangeDeleteOkBody responseBody =
            _connection.getMethodRegistry().createExchangeDeleteOkBody();
        _connection.writeFrame(responseBody.generateFrame(getChannelId()));
      }
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

    final AMQShortString queueName;

    // if we aren't given a queue name, we create one which we return to the client
    if ((queueStr == null) || (queueStr.length() == 0)) {
      queueName = AMQShortString.createAMQShortString("tmp_" + UUID.randomUUID());
    } else {
      queueName = queueStr;
    }

    Queue queue;

    // TODO: do we need to check that the queue already exists with exactly the same
    // "configuration"?

    if (passive) {
      queue = getQueue(queueName.toString());
      if (queue == null) {
        closeChannel(
            ErrorCodes.NOT_FOUND,
            "Queue: '"
                + queueName
                + "' not found on VirtualHost '"
                + _connection.getVhost().getNamespace()
                + "'.");
      } else {
        // TODO: check exclusive queue access
        {
          // set this as the default queue on the channel:
          setDefaultQueue(queue);
          if (!nowait) {
            MethodRegistry methodRegistry = _connection.getMethodRegistry();
            QueueDeclareOkBody responseBody =
                methodRegistry.createQueueDeclareOkBody(
                    queueName, queue.getQueueDepthMessages(), queue.getConsumerCount());
            _connection.writeFrame(responseBody.generateFrame(getChannelId()));

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Queue " + queueName + " declared successfully");
            }
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

        queue = getQueue(queueName.toString());
        if (queue != null) {
          // TODO; verify if queue is exclusive and opened by another connection
          if (queue.isExclusive() != exclusive) {

            closeChannel(
                ErrorCodes.ALREADY_EXISTS,
                "Cannot re-declare queue '"
                    + queue.getName()
                    + "' with different exclusivity (was: "
                    + queue.isExclusive()
                    + " requested "
                    + exclusive
                    + ")");
          } else if ((autoDelete && queue.getLifetimePolicy() == LifetimePolicy.PERMANENT)
              || (!autoDelete
                  && queue.getLifetimePolicy()
                      != ((exclusive && !durable)
                          ? LifetimePolicy.DELETE_ON_CONNECTION_CLOSE
                          : LifetimePolicy.PERMANENT))) {
            closeChannel(
                ErrorCodes.ALREADY_EXISTS,
                "Cannot re-declare queue '"
                    + queue.getName()
                    + "' with different lifetime policy (was: "
                    + queue.getLifetimePolicy()
                    + " requested autodelete: "
                    + autoDelete
                    + ")");
          } else {
            setDefaultQueue(queue);
            if (!nowait) {
              MethodRegistry methodRegistry = _connection.getMethodRegistry();
              QueueDeclareOkBody responseBody =
                  methodRegistry.createQueueDeclareOkBody(
                      queueName, queue.getQueueDepthMessages(), queue.getConsumerCount());
              _connection.writeFrame(responseBody.generateFrame(getChannelId()));

              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Queue " + queueName + " declared successfully");
              }
            }
          }
        } else {

          queue = new Queue(queueName.toString(), lifetimePolicy, exclusivityPolicy);
          addQueue(queue);
          _connection
              .getVhost()
              .getExchange(ExchangeDefaults.DEFAULT_EXCHANGE_NAME)
              .bind(queue, queue.getName(), _connection);

          setDefaultQueue(queue);

          if (!nowait) {
            MethodRegistry methodRegistry = _connection.getMethodRegistry();
            QueueDeclareOkBody responseBody =
                methodRegistry.createQueueDeclareOkBody(
                    queueName, queue.getQueueDepthMessages(), queue.getConsumerCount());
            _connection.writeFrame(responseBody.generateFrame(getChannelId()));

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Queue " + queueName + " declared successfully");
            }
          }
        }
      } catch (IllegalArgumentException e) {
        String message = String.format("Error creating queue '%s': %s", queueName, e.getMessage());
        _connection.sendConnectionClose(ErrorCodes.INVALID_ARGUMENT, message, getChannelId());
      } catch (PulsarClientException e) {
        _connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR, e.getMessage(), getChannelId());
      }
    }
  }

  @Override
  public void receiveQueueBind(
      AMQShortString queue,
      AMQShortString exchange,
      AMQShortString bindingKey,
      boolean nowait,
      FieldTable arguments) {}

  @Override
  public void receiveQueuePurge(AMQShortString queue, boolean nowait) {}

  @Override
  public void receiveQueueDelete(
      AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {}

  @Override
  public void receiveQueueUnbind(
      AMQShortString queue,
      AMQShortString exchange,
      AMQShortString bindingKey,
      FieldTable arguments) {}

  @Override
  public void receiveBasicRecover(boolean requeue, boolean sync) {}

  @Override
  public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {}

  @Override
  public void receiveBasicConsume(
      AMQShortString queue,
      AMQShortString consumerTag,
      boolean noLocal,
      boolean noAck,
      boolean exclusive,
      boolean nowait,
      FieldTable arguments) {}

  @Override
  public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {}

  @Override
  public void receiveBasicPublish(
      AMQShortString exchangeName,
      AMQShortString routingKey,
      boolean mandatory,
      boolean immediate) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicPublish["
              + " exchange: "
              + exchangeName
              + " routingKey: "
              + routingKey
              + " mandatory: "
              + mandatory
              + " immediate: "
              + immediate
              + " ]");
    }

    if (!_connection
        .getVhost()
        .hasExchange(
            exchangeName == null
                ? ExchangeDefaults.DEFAULT_EXCHANGE_NAME
                : exchangeName.toString())) {
      closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange name: '" + exchangeName + "'");
    }

    MessagePublishInfo info =
        new MessagePublishInfo(exchangeName, immediate, mandatory, routingKey);

    setPublishFrame(info);
  }

  @Override
  public void receiveBasicGet(AMQShortString queueName, boolean noAck) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RECV["
              + _channelId
              + "] BasicGet["
              + " queue: "
              + queueName
              + " noAck: "
              + noAck
              + " ]");
    }

    Queue queue =
        queueName == null
            ? getDefaultQueue()
            : _connection.getVhost().getQueue(queueName.toString());

    if (queue == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No queue for '" + queueName + "'");
      }
      if (queueName != null) {
        _connection.sendConnectionClose(
            ErrorCodes.NOT_FOUND, "No such queue, '" + queueName + "'", _channelId);

      } else {
        _connection.sendConnectionClose(
            ErrorCodes.NOT_ALLOWED,
            "No queue name provided, no default queue defined.",
            _channelId);
      }
    } else {

      // try {
      Message<byte[]> message = queue.receive(noAck);
      if (message != null) {
        _connection
            .getProtocolOutputConverter()
            .writeGetOk(
                MessageUtils.getMessagePublishInfo(message),
                new ContentBody(ByteBuffer.wrap(message.getData())),
                MessageUtils.getContentHeaderBody(message),
                message.getRedeliveryCount() > 0,
                _channelId,
                getNextDeliveryTag(),
                queue.getQueueDepthMessages());
      } else {
        MethodRegistry methodRegistry = _connection.getMethodRegistry();
        BasicGetEmptyBody responseBody = methodRegistry.createBasicGetEmptyBody(null);
        _connection.writeFrame(responseBody.generateFrame(_channelId));
      }
      /*} catch (AccessControlException e) {
        _connection.sendConnectionClose(ErrorCodes.ACCESS_REFUSED, e.getMessage(), _channelId);
      }
      catch (MessageSource.ExistingExclusiveConsumer e)
      {
        _connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED, "Queue has an exclusive consumer", _channelId);
      }
      catch (MessageSource.ExistingConsumerPreventsExclusive e)
      {
        _connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
                "The GET request has been evaluated as an exclusive consumer, " +
                        "this is likely due to a programming error in the Qpid broker", _channelId);
      }
      catch (MessageSource.ConsumerAccessRefused consumerAccessRefused)
      {
        _connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED,
                "Queue has an incompatible exclusivity policy", _channelId);
      }
      catch (MessageSource.QueueDeleted queueDeleted)
      {
        _connection.sendConnectionClose(ErrorCodes.NOT_FOUND, "Queue has been deleted", _channelId);
      }*/
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
              + hex(
                  data, _connection.getGatewayService().getConfig().getAmqpDebugBinaryDataLength())
              + " ] ");
    }

    if (hasCurrentMessage()) {
      publishContentBody(new ContentBody(data));
    } else {
      _connection.sendConnectionClose(
          ErrorCodes.COMMAND_INVALID,
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
      int maxMessageSize = _connection.getGatewayService().getConfig().getAmqpMaxMessageSize();
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
          ErrorCodes.COMMAND_INVALID,
          "Attempt to send a content header without first sending a publish frame",
          _channelId);
    }
  }

  @Override
  public boolean ignoreAllButCloseOk() {
    return false;
  }

  @Override
  public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {}

  @Override
  public void receiveBasicAck(long deliveryTag, boolean multiple) {}

  @Override
  public void receiveBasicReject(long deliveryTag, boolean requeue) {}

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

  private void deliverCurrentMessageIfComplete() {
    // check and deliver if header says body length is zero
    if (_currentMessage.allContentReceived()) {
      MessagePublishInfo info = _currentMessage.getMessagePublishInfo();
      String routingKey = AMQShortString.toString(info.getRoutingKey());
      String exchangeName = AMQShortString.toString(info.getExchange());

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
      messageBuilder.property(
          MessageUtils.MESSAGE_PROPERTY_AMQP_MANDATORY, String.valueOf(info.isMandatory()));

      ContentHeaderBody contentHeader = _currentMessage.getContentHeader();
      byte[] bytes = new byte[contentHeader.getSize()];
      QpidByteBuffer buf = QpidByteBuffer.wrap(bytes);
      contentHeader.writePayload(buf);

      messageBuilder.property(
          MessageUtils.MESSAGE_PROPERTY_AMQP_HEADERS, Base64.getEncoder().encodeToString(bytes));
      if (contentHeader.getProperties().getTimestamp() > 0) {
        messageBuilder.eventTime(contentHeader.getProperties().getTimestamp());
      }

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
                LOGGER.error("Failed to write message to exchange", throwable);
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

  public boolean isClosing() {
    return _closing.get() || getConnection().isClosing();
  }

  private GatewayConnection getConnection() {
    return _connection;
  }

  private boolean hasCurrentMessage() {
    return _currentMessage != null;
  }

  public int getChannelId() {
    return _channelId;
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
      setDefaultQueue(null);
    } finally {
      LogMessage operationalLogMessage =
          cause == 0 ? ChannelMessages.CLOSE() : ChannelMessages.CLOSE_FORCED(cause, message);
      messageWithSubject(operationalLogMessage);
    }
  }

  private void unsubscribeAllConsumers() {
    // TODO unsubscribeAllConsumers
  }

  private void messageWithSubject(final LogMessage operationalLogMessage) {
    Logger logger = LoggerFactory.getLogger(operationalLogMessage.getLogHierarchy());
    logger.info(operationalLogMessage.toString());
  }

  private void setDefaultQueue(Queue queue) {
    _defaultQueue = queue;
  }

  private Queue getDefaultQueue() {
    return _defaultQueue;
  }

  private Exchange getExchange(String name) {
    return _connection.getVhost().getExchange(name);
  }

  private void addExchange(Exchange exchange) {
    _connection.getVhost().addExchange(exchange);
  }

  private void deleteExchange(Exchange exchange) {
    _connection.getVhost().deleteExchange(exchange);
  }

  private Queue getQueue(String name) {
    return _connection.getVhost().getQueue(name);
  }

  private void addQueue(Queue queue) {
    _connection.getVhost().addQueue(queue);
  }

  private boolean isReservedExchangeName(String name) {
    return name == null
        || ExchangeDefaults.DEFAULT_EXCHANGE_NAME.equals(name)
        || name.startsWith("amq.")
        || name.startsWith("qpid.");
  }

  private Producer<byte[]> getOrCreateProducerForExchange(String exchangeName, String routingKey) {
    String vHost = _connection.getNamespace();
    TopicName topicName = Exchange.getTopicName(vHost, exchangeName, routingKey);

    return producers.computeIfAbsent(topicName.toString(), this::createProducer);
  }

  private Producer<byte[]> createProducer(String topicName) {
    try {
      return _connection
          .getGatewayService()
          .getPulsarClient()
          .newProducer()
          .topic(topicName)
          .create();
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }
}
