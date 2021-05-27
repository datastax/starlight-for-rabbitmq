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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQChannel implements ServerChannelMethodProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AMQChannel.class);

  private final int _channelId;
  private final GatewayConnection _connection;
  private final AtomicBoolean _closing = new AtomicBoolean(false);

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
      AMQShortString exchange,
      AMQShortString type,
      boolean passive,
      boolean durable,
      boolean autoDelete,
      boolean internal,
      boolean nowait,
      FieldTable arguments) {}

  @Override
  public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {}

  @Override
  public void receiveExchangeBound(
      AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {}

  @Override
  public void receiveQueueDeclare(
      AMQShortString queue,
      boolean passive,
      boolean durable,
      boolean exclusive,
      boolean autoDelete,
      boolean nowait,
      FieldTable arguments) {}

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
      AMQShortString exchange, AMQShortString routingKey, boolean mandatory, boolean immediate) {}

  @Override
  public void receiveBasicGet(AMQShortString queue, boolean noAck) {}

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
    if(LOGGER.isDebugEnabled())
    {
      LOGGER.debug("RECV[" + _channelId + "] ChannelCloseOk");
    }

    _connection.closeChannelOk(getChannelId());
  }

  @Override
  public void receiveMessageContent(QpidByteBuffer data) {}

  @Override
  public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {}

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
  public void receiveConfirmSelect(boolean nowait) {}

  public boolean isClosing() {
    return _closing.get() || getConnection().isClosing();
  }

  private GatewayConnection getConnection() {
    return _connection;
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

  private void setDefaultQueue(Queue<?> queue) {
    // TODO setDefaultQueue
  }
}
