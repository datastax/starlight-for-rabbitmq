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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.codec.binary.Base64;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetEmptyBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class AMQChannelTest extends AbstractBaseTest {

  public static final String TEST_EXCHANGE = "test-exchange";
  public static final String TEST_QUEUE = "test-queue";
  public static final byte[] TEST_MESSAGE = "test-message".getBytes(StandardCharsets.UTF_8);

  @Test
  void testReceiveChannelClose() {
    openChannel();
    assertNotNull(connection.getChannel(CHANNEL_ID));

    AMQFrame frame = sendChannelClose();

    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof ChannelCloseOkBody);
    assertNull(connection.getChannel(CHANNEL_ID));
  }

  @Test
  void testReceiveChannelCloseOk() {
    openChannel();
    connection.markChannelAwaitingCloseOk(CHANNEL_ID);
    assertTrue(connection.channelAwaitingClosure(CHANNEL_ID));

    AMQFrame frame = sendChannelCloseOk();

    assertNull(frame);
    assertFalse(connection.channelAwaitingClosure(CHANNEL_ID));
  }

  @Test
  void testReceiveExchangeDeclare() {
    openChannel();

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclareDefaultExchange() {
    openChannel();

    AMQFrame frame =
        sendExchangeDeclare(
            ExchangeDefaults.DEFAULT_EXCHANGE_NAME, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclareDefaultExchangeInvalidType() {
    openChannel();

    AMQFrame frame =
        sendExchangeDeclare(
            ExchangeDefaults.DEFAULT_EXCHANGE_NAME, ExchangeDefaults.FANOUT_EXCHANGE_CLASS, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeDeclarePassive() {
    openChannel();
    sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, true);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclarePassiveNotFound() {
    openChannel();

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, true);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  @Test
  void testReceiveExchangeDeclarePassiveInvalidType() {
    openChannel();
    sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS, true);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeDeclareReservedName() {
    openChannel();

    AMQFrame frame = sendExchangeDeclare("amq.test", ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeDeclareAlreadyExists() {
    openChannel();
    sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclareAlreadyExistsDifferentType() {
    openChannel();
    sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeDeclareInvalidType() {
    openChannel();

    AMQFrame frame = sendExchangeDeclare(TEST_EXCHANGE, "invalid-type", false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
  }

  @Test
  void testReceiveExchangeDelete() {
    openChannel();
    sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    AMQFrame frame = sendExchangeDelete(TEST_EXCHANGE, false);

    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof ExchangeDeleteOkBody);
  }

  @Test
  void testReceiveExchangeDeleteNotFound() {
    openChannel();

    AMQFrame frame = sendExchangeDelete(TEST_EXCHANGE, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  // TODO: test ExchangeDelete with ifUnused when bindings implemented
  // @Test
  void testReceiveExchangeDeleteIfUnused() {
    openConnection();
    sendChannelOpen();

    AMQFrame frame = sendExchangeDelete(TEST_EXCHANGE, true);

    assertIsChannelCloseFrame(frame, ErrorCodes.IN_USE);
  }

  @Test
  void testReceiveExchangeDeleteReservedExchange() {
    openChannel();

    AMQFrame frame = sendExchangeDelete(ExchangeDefaults.FANOUT_EXCHANGE_NAME, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveQueueDeclare() {
    openChannel();

    AMQFrame frame = sendQueueDeclare();

    assertIsQueueDeclareOk(frame);
  }

  @Test
  void testReceiveQueueDeclareEmptyName() {
    openChannel();

    AMQFrame frame = sendQueueDeclare("", false);

    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof QueueDeclareOkBody);
    QueueDeclareOkBody queueDeclareOkBody = (QueueDeclareOkBody) frame.getBodyFrame();
    assertTrue(queueDeclareOkBody.getQueue().toString().startsWith("tmp_"));
  }

  @Test
  void testReceiveQueueDeclarePassive() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendQueueDeclare(TEST_QUEUE, true);

    assertIsQueueDeclareOk(frame);
  }

  @Test
  void testReceiveQueueDeclarePassiveNotFound() {
    openChannel();

    AMQFrame frame = sendQueueDeclare(TEST_QUEUE, true);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  @Test
  void testReceiveQueueDeclareInvalidExclusivityAttribute() {
    openChannel();

    AMQFrame frame = sendQueueDeclare(TEST_QUEUE, false, null, "invalid");

    assertIsConnectionCloseFrame(frame, ErrorCodes.INVALID_ARGUMENT);
  }

  @Test
  void testReceiveQueueDeclareInvalidLifetimePolicyAttribute() {
    openChannel();

    AMQFrame frame = sendQueueDeclare(TEST_QUEUE, false, "invalid", null);

    assertIsConnectionCloseFrame(frame, ErrorCodes.INVALID_ARGUMENT);
  }

  @Test
  void testReceiveQueueDeclareAlreadyExists() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendQueueDeclare();

    assertIsQueueDeclareOk(frame);
  }

  @Test
  void testReceiveQueueDeclareAlreadyExistsDifferentExclusivity() {
    openChannel();
    sendQueueDeclare();

    QueueDeclareBody queueDeclareBody =
        new QueueDeclareBody(
            0,
            AMQShortString.createAMQShortString(TEST_QUEUE),
            false,
            true,
            true,
            false,
            false,
            FieldTable.convertToFieldTable(Collections.emptyMap()));
    AMQFrame frame = exchangeData(queueDeclareBody.generateFrame(CHANNEL_ID));

    assertIsChannelCloseFrame(frame, ErrorCodes.ALREADY_EXISTS);
  }

  @Test
  void testReceiveQueueDeclareAlreadyExistsDifferentLifetime() {
    openChannel();
    sendQueueDeclare();

    QueueDeclareBody queueDeclareBody =
        new QueueDeclareBody(
            0,
            AMQShortString.createAMQShortString(TEST_QUEUE),
            false,
            true,
            false,
            true,
            false,
            FieldTable.convertToFieldTable(Collections.emptyMap()));
    AMQFrame frame = exchangeData(queueDeclareBody.generateFrame(CHANNEL_ID));

    assertIsChannelCloseFrame(frame, ErrorCodes.ALREADY_EXISTS);
  }

  @Test
  void testReceiveBasicPublishExchangeNotFound() {
    openChannel();

    AMQFrame frame = sendBasicPublish(TEST_EXCHANGE);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  @Test
  void testReceiveMessageHeaderNoCurrentMessage() {
    openChannel();

    AMQFrame frame = sendMessageHeader(1);

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
  }

  @Test
  void testReceiveMessageHeaderBodyTooLarge() {
    openChannel();
    sendBasicPublish();

    AMQFrame frame = sendMessageHeader(config.getAmqpMaxMessageSize() + 1);

    assertIsChannelCloseFrame(frame, ErrorCodes.MESSAGE_TOO_LARGE);
  }

  @Test
  void testReceiveMessageHeaderMalformed() {
    openChannel();
    sendBasicPublish();

    BasicContentHeaderProperties props = new BasicContentHeaderProperties();
    props.setHeaders(FieldTableFactory.createFieldTable(QpidByteBuffer.allocateDirect(1)));
    AMQFrame frame = exchangeData(ContentHeaderBody.createAMQFrame(CHANNEL_ID, props, 1));

    assertIsConnectionCloseFrame(frame, ErrorCodes.FRAME_ERROR);
  }

  @Test
  void testReceiveMessageContentNoCurrentMessage() {
    openChannel();

    AMQFrame frame = sendMessageContent();

    assertIsConnectionCloseFrame(frame, ErrorCodes.COMMAND_INVALID);
  }

  @Test
  void testReceiveMessageContentTooLarge() {
    openChannel();
    sendBasicPublish();
    sendMessageHeader(1);

    AMQFrame frame = sendMessageContent();

    assertIsConnectionCloseFrame(frame, ErrorCodes.FRAME_ERROR);
  }

  @Test
  void testReceiveMessagePulsarClientError() throws Exception {
    when(gatewayService.getPulsarClient()).thenThrow(PulsarClientException.class);

    openChannel();
    sendBasicPublish();
    sendMessageHeader(TEST_MESSAGE.length);

    AMQFrame frame = sendMessageContent();

    assertIsConnectionCloseFrame(frame, ErrorCodes.INTERNAL_ERROR);
  }

  @Test
  void testReceiveMessageSuccess() throws Exception {
    TypedMessageBuilder messageBuilder = mock(TypedMessageBuilder.class);

    when(producer.newMessage()).thenReturn(messageBuilder);
    when(messageBuilder.sendAsync())
        .thenReturn(CompletableFuture.completedFuture(new MessageIdImpl(1, 2, 3)));

    openChannel();
    sendBasicPublish();

    BasicContentHeaderProperties props = new BasicContentHeaderProperties();
    props.setContentType("application/json");
    props.setTimestamp(1234);

    exchangeData(ContentHeaderBody.createAMQFrame(CHANNEL_ID, props, TEST_MESSAGE.length));
    AMQFrame frame = sendMessageContent();

    assertNull(frame);
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(messageBuilder)
        .property(eq(MessageUtils.MESSAGE_PROPERTY_AMQP_HEADERS), captor.capture());
    byte[] bytes = Base64.decodeBase64(captor.getValue());
    ContentHeaderBody contentHeaderBody =
        new ContentHeaderBody(QpidByteBuffer.wrap(bytes), bytes.length);
    assertEquals("application/json", contentHeaderBody.getProperties().getContentType().toString());

    verify(messageBuilder).eventTime(1234);
    verify(messageBuilder).value(TEST_MESSAGE);
    verify(messageBuilder).sendAsync();
  }

  @Test
  void testReceiveMessageConfirm() {
    TypedMessageBuilder messageBuilder = mock(TypedMessageBuilder.class);

    when(producer.newMessage()).thenReturn(messageBuilder);
    when(messageBuilder.sendAsync())
        .thenReturn(CompletableFuture.completedFuture(new MessageIdImpl(1, 2, 3)));

    openChannel();
    AMQFrame frame = exchangeData(new ConfirmSelectBody(false).generateFrame(CHANNEL_ID));

    assertTrue(frame.getBodyFrame() instanceof ConfirmSelectOkBody);

    sendBasicPublish();
    sendMessageHeader(TEST_MESSAGE.length);
    frame = sendMessageContent();

    assertTrue(frame.getBodyFrame() instanceof BasicAckBody);
    BasicAckBody basicAckBody = (BasicAckBody) frame.getBodyFrame();
    assertEquals(1, basicAckBody.getDeliveryTag());
    assertFalse(basicAckBody.getMultiple());
    verify(messageBuilder).value(TEST_MESSAGE);

    sendBasicPublish();
    sendMessageHeader(TEST_MESSAGE.length);
    frame = sendMessageContent();

    assertTrue(frame.getBodyFrame() instanceof BasicAckBody);
    basicAckBody = (BasicAckBody) frame.getBodyFrame();
    assertEquals(2, basicAckBody.getDeliveryTag());
  }

  @Test
  void testReceiveBasicGet() {
    openChannel();
    MessageImpl message = mock(MessageImpl.class);
    when(message.getData()).thenReturn(TEST_MESSAGE);
    when(message.getTopicName()).thenReturn(TEST_EXCHANGE + "$$" + TEST_QUEUE);
    when(message.getRedeliveryCount()).thenReturn(2);

    BasicContentHeaderProperties props = new BasicContentHeaderProperties();
    props.setContentType("application/json");
    ContentHeaderBody contentHeader = new ContentHeaderBody(props, TEST_MESSAGE.length);
    byte[] bytes = new byte[contentHeader.getSize()];
    QpidByteBuffer buf = QpidByteBuffer.wrap(bytes);
    contentHeader.writePayload(buf);
    String headers = java.util.Base64.getEncoder().encodeToString(bytes);
    when(message.getProperty(MessageUtils.MESSAGE_PROPERTY_AMQP_HEADERS)).thenReturn(headers);

    MessageImpl message2 = mock(MessageImpl.class);
    when(message2.getData()).thenReturn(TEST_MESSAGE);
    when(message2.getTopicName()).thenReturn(TEST_EXCHANGE + "$$" + TEST_QUEUE);
    when(message2.getRedeliveryCount()).thenReturn(0);

    when(consumer.receiveAsync())
        .thenReturn(
            CompletableFuture.completedFuture(message),
            CompletableFuture.completedFuture(message2));

    sendQueueDeclare();

    ProtocolOutputConverter.CompositeAMQBodyBlock compositeAMQBodyBlock = sendBasicGet();

    assertNotNull(compositeAMQBodyBlock);
    assertTrue(compositeAMQBodyBlock.getMethodBody() instanceof BasicGetOkBody);
    BasicGetOkBody basicGetOkBody = (BasicGetOkBody) compositeAMQBodyBlock.getMethodBody();
    assertEquals(TEST_EXCHANGE, basicGetOkBody.getExchange().toString());
    assertEquals(TEST_QUEUE, basicGetOkBody.getRoutingKey().toString());
    assertEquals(1, basicGetOkBody.getDeliveryTag());
    assertTrue(basicGetOkBody.getRedelivered());

    assertTrue(compositeAMQBodyBlock.getHeaderBody() instanceof ContentHeaderBody);
    ContentHeaderBody contentHeaderBody = (ContentHeaderBody) compositeAMQBodyBlock.getHeaderBody();
    assertEquals(TEST_MESSAGE.length, contentHeaderBody.getBodySize());
    assertEquals("application/json", contentHeaderBody.getProperties().getContentTypeAsString());

    AMQBody contentBody = compositeAMQBodyBlock.getContentBody();
    ByteBuf byteBuf = Unpooled.buffer(contentBody.getSize());
    contentBody.writePayload(new NettyByteBufferSender(byteBuf));
    assertArrayEquals(TEST_MESSAGE, byteBuf.array());

    compositeAMQBodyBlock = sendBasicGet();
    assertNotNull(compositeAMQBodyBlock);
    assertTrue(compositeAMQBodyBlock.getMethodBody() instanceof BasicGetOkBody);
    basicGetOkBody = (BasicGetOkBody) compositeAMQBodyBlock.getMethodBody();
    assertEquals(2, basicGetOkBody.getDeliveryTag());
    assertFalse(basicGetOkBody.getRedelivered());
  }

  @Test
  void testReceiveBasicGetEmpty() {
    openChannel();
    when(consumer.receiveAsync()).thenReturn(new CompletableFuture<>());
    sendQueueDeclare();

    AMQFrame frame = sendBasicGet();

    assertNotNull(frame);
    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof BasicGetEmptyBody);
  }

  @Test
  void testReceiveBasicGetQueueNotFound() {
    openChannel();

    AMQFrame frame = sendBasicGet();

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  @Test
  void testReceiveBasicGetQueueNameMissing() {
    openChannel();

    AMQFrame frame = sendBasicGet("");

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveBasicGetDefaultQueue() {
    openChannel();
    MessageImpl message = mock(MessageImpl.class);
    when(message.getData()).thenReturn(TEST_MESSAGE);
    when(message.getTopicName()).thenReturn(TEST_EXCHANGE + "$$" + TEST_QUEUE);
    when(message.getRedeliveryCount()).thenReturn(0);
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));
    sendQueueDeclare();

    ProtocolOutputConverter.CompositeAMQBodyBlock compositeAMQBodyBlock = sendBasicGet("");

    assertNotNull(compositeAMQBodyBlock);
  }

  private void openChannel() {
    openConnection();
    sendChannelOpen();
  }

  private <T> T sendBasicGet() {
    return sendBasicGet(TEST_QUEUE);
  }

  private <T> T sendBasicGet(String queueName) {
    BasicGetBody basicGetBody =
        new BasicGetBody(0, AMQShortString.createAMQShortString(queueName), true);
    return exchangeData(basicGetBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendChannelClose() {
    ChannelCloseBody channelCloseBody = new ChannelCloseBody(0, AMQShortString.EMPTY_STRING, 0, 0);
    return exchangeData(channelCloseBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendExchangeDeclare(String exchange, String type, boolean passive) {
    ExchangeDeclareBody exchangeDeclareBody =
        new ExchangeDeclareBody(
            0,
            AMQShortString.createAMQShortString(exchange),
            AMQShortString.createAMQShortString(type),
            passive,
            true,
            false,
            false,
            false,
            FieldTable.convertToFieldTable(Collections.emptyMap()));
    return exchangeData(exchangeDeclareBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendQueueDeclare() {
    return sendQueueDeclare(TEST_QUEUE, false);
  }

  private AMQFrame sendQueueDeclare(String queue, boolean passive) {
    return sendQueueDeclare(queue, passive, null, null);
  }

  private AMQFrame sendQueueDeclare(
      String queue, boolean passive, String lifetimePolicy, String exclusivityPolicy) {
    HashMap<String, Object> attributes = new HashMap<>();
    if (lifetimePolicy != null) {
      attributes.put(Queue.LIFETIME_POLICY, lifetimePolicy);
    }
    if (exclusivityPolicy != null) {
      attributes.put(Queue.EXCLUSIVE, exclusivityPolicy);
    }
    QueueDeclareBody queueDeclareBody =
        new QueueDeclareBody(
            0,
            AMQShortString.createAMQShortString(queue),
            passive,
            true,
            false,
            false,
            false,
            FieldTable.convertToFieldTable(attributes));
    return exchangeData(queueDeclareBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendExchangeDelete(String exchange, boolean ifUnused) {
    ExchangeDeleteBody exchangeDeleteBody =
        new ExchangeDeleteBody(0, AMQShortString.createAMQShortString(exchange), ifUnused, false);
    return exchangeData(exchangeDeleteBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendBasicPublish() {
    BasicPublishBody basicPublishBody = new BasicPublishBody(0, null, null, false, false);
    return exchangeData(basicPublishBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendBasicPublish(String exchange) {
    BasicPublishBody basicPublishBody =
        new BasicPublishBody(0, AMQShortString.createAMQShortString(exchange), null, false, false);
    return exchangeData(basicPublishBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendMessageHeader(long bodySize) {
    BasicContentHeaderProperties props = new BasicContentHeaderProperties();
    return exchangeData(ContentHeaderBody.createAMQFrame(CHANNEL_ID, props, bodySize));
  }

  private AMQFrame sendMessageContent() {
    ContentBody contentBody = new ContentBody(ByteBuffer.wrap(TEST_MESSAGE));
    return exchangeData(ContentBody.createAMQFrame(CHANNEL_ID, contentBody));
  }

  private void assertIsChannelCloseFrame(AMQFrame frame, int errorCode) {
    assertNotNull(frame);
    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof ChannelCloseBody);
    ChannelCloseBody channelCloseBody = (ChannelCloseBody) frame.getBodyFrame();
    assertEquals(errorCode, channelCloseBody.getReplyCode());
  }

  private void assertIsExchangeDeclareOk(AMQFrame frame) {
    assertNotNull(frame);
    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof ExchangeDeclareOkBody);
  }

  private void assertIsQueueDeclareOk(AMQFrame frame) {
    assertNotNull(frame);
    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof QueueDeclareOkBody);
    QueueDeclareOkBody queueDeclareOkBody = (QueueDeclareOkBody) frame.getBodyFrame();
    assertEquals(TEST_QUEUE, queueDeclareOkBody.getQueue().toString());
  }
}
