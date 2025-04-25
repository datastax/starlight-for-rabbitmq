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

import static org.apache.qpid.server.model.ConfiguredObject.LIFETIME_POLICY;
import static org.apache.qpid.server.model.Queue.EXCLUSIVE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Base64;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicDeliverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetEmptyBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicNackBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRecoverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRecoverSyncBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRecoverSyncOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRejectBody;
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
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class AMQChannelTest extends AbstractBaseTest {

  public static final String TEST_EXCHANGE = "test-exchange";
  public static final String TEST_QUEUE = "test-queue";
  public static final byte[] TEST_MESSAGE = "test-message".getBytes(StandardCharsets.UTF_8);
  public static final String TEST_CONSUMER_TAG = "test-consumer-tag";
  public static final String TEST_KEY = "test-key";

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

    AMQFrame frame = sendExchangeDeclare();

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclareDefaultExchange() {
    openChannel();

    AMQFrame frame =
        sendExchangeDeclare(
            ExchangeDefaults.DEFAULT_EXCHANGE_NAME, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testReceiveExchangeDeclareDefaultExchangeInvalidType() {
    openChannel();

    AMQFrame frame =
        sendExchangeDeclare(
            ExchangeDefaults.DEFAULT_EXCHANGE_NAME, ExchangeDefaults.FANOUT_EXCHANGE_CLASS, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testReceiveExchangeDeclarePassive() {
    openChannel();
    sendExchangeDeclare();

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
    sendExchangeDeclare();

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS, true);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveExchangeDeclareReservedName() {
    openChannel();

    AMQFrame frame = sendExchangeDeclare("amq.test", ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testReceiveExchangeDeclareAlreadyExists() {
    openChannel();
    sendExchangeDeclare();

    AMQFrame frame = sendExchangeDeclare();

    assertIsExchangeDeclareOk(frame);
  }

  @Test
  void testReceiveExchangeDeclareAlreadyExistsDifferentType() {
    openChannel();
    sendExchangeDeclare();

    AMQFrame frame =
        sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.IN_USE);
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
    sendExchangeDeclare();

    AMQFrame frame = sendExchangeDelete(TEST_EXCHANGE, false);

    assertEquals(CHANNEL_ID, frame.getChannel());
    assertTrue(frame.getBodyFrame() instanceof ExchangeDeleteOkBody);
  }

  @Test
  void testReceiveExchangeDeleteNotFound() {
    openChannel();

    AMQFrame frame = sendExchangeDelete(TEST_EXCHANGE, false);

    // Exchange delete made idempotent op in RabbitMQ 3.2+
    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof ExchangeDeleteOkBody);
  }

  // TODO: test ExchangeDelete with ifUnused when bindings implemented
  // @Test
  void testReceiveExchangeDeleteIfUnused() {
    openChannel();

    AMQFrame frame = sendExchangeDelete(TEST_EXCHANGE, true);

    assertIsChannelCloseFrame(frame, ErrorCodes.IN_USE);
  }

  @Test
  void testReceiveExchangeDeleteReservedExchange() {
    openChannel();

    AMQFrame frame = sendExchangeDelete(ExchangeDefaults.FANOUT_EXCHANGE_NAME, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
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
    assertTrue(queueDeclareOkBody.getQueue().toString().startsWith("auto_"));
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

    assertIsChannelCloseFrame(frame, ErrorCodes.IN_USE);
  }

  @Test
  void testReceiveQueueBind() {
    openChannel();
    sendExchangeDeclare();
    sendQueueDeclare();

    AMQFrame frame = sendQueueBind(TEST_KEY);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueBindOkBody);
  }

  @Test
  void testReceiveQueueBindQueueNotFound() {
    openChannel();
    sendExchangeDeclare();

    AMQFrame frame = sendQueueBind(TEST_KEY);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  @Test
  void testReceiveQueueBindExchangeNotFound() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendQueueBind(TEST_KEY);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  @Test
  void testReceiveQueueBindDefaultExchangeNotAllowed() {
    openChannel();
    sendQueueDeclare();
    sendExchangeDeclare();

    AMQFrame frame = sendQueueBind(TEST_QUEUE, ExchangeDefaults.DEFAULT_EXCHANGE_NAME, TEST_KEY);

    assertIsChannelCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testReceiveQueueBindDefaultQueue() {
    openChannel();
    sendExchangeDeclare();
    sendQueueDeclare();

    AMQFrame frame = sendQueueBind("", TEST_EXCHANGE, TEST_KEY);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueBindOkBody);
  }

  @Test
  void testReceiveQueueUnBind() {
    openChannel();
    sendExchangeDeclare();
    sendQueueDeclare();
    sendQueueBind(TEST_KEY);

    AMQFrame frame = sendQueueUnbind(TEST_KEY);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueUnbindOkBody);
  }

  @Test
  void testReceiveQueueUnbindQueueNotFound() {
    openChannel();
    sendExchangeDeclare();

    AMQFrame frame = sendQueueUnbind(TEST_KEY);

    // Queue unbind made idempotent op in RabbitMQ 3.2+
    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueUnbindOkBody);
  }

  @Test
  void testReceiveQueueUnbindExchangeNotFound() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendQueueUnbind(TEST_KEY);

    // Queue unbind made idempotent op in RabbitMQ 3.2+
    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueUnbindOkBody);
  }

  @Test
  void testReceiveQueueUnbindBindingNotFound() {
    openChannel();
    sendExchangeDeclare();
    sendQueueDeclare();

    AMQFrame frame = sendQueueUnbind(TEST_KEY);

    // Queue unbind made idempotent op in RabbitMQ 3.2+
    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueUnbindOkBody);
  }

  @Test
  void testReceiveQueueUnbindDefaultExchangeNotAllowed() {
    openChannel();
    sendQueueDeclare();
    sendExchangeDeclare();

    AMQFrame frame = sendQueueUnbind(TEST_QUEUE, ExchangeDefaults.DEFAULT_EXCHANGE_NAME, TEST_KEY);

    assertIsChannelCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testReceiveQueueUnbindDefaultQueue() {
    openChannel();
    sendExchangeDeclare();
    sendQueueDeclare();
    sendQueueBind(TEST_KEY);

    AMQFrame frame = sendQueueUnbind("", TEST_EXCHANGE, TEST_KEY);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueUnbindOkBody);
  }

  @Test
  void testReceiveQueueDelete() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendQueueDelete(TEST_QUEUE, false);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueDeleteOkBody);
  }

  @Test
  void testReceiveQueueDeleteNotFound() {
    openChannel();

    AMQFrame frame = sendQueueDelete(TEST_QUEUE, false);

    // Queue delete made idempotent op in RabbitMQ 3.2+
    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueDeleteOkBody);
  }

  @Test
  void testReceiveQueueDeleteDefaultQueue() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendQueueDelete("", false);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof QueueDeleteOkBody);
  }

  // TODO: support delete ifUnused with multiple gateways
  // @Test
  void testReceiveQueueDeleteInUse() {
    openChannel();
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    AMQFrame frame = sendQueueDelete(TEST_QUEUE, true);

    assertIsChannelCloseFrame(frame, ErrorCodes.IN_USE);
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

    assertIsConnectionCloseFrame(frame, 505);
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

    assertIsConnectionCloseFrame(frame, 505);
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
    PulsarClient pulsarClient = mock(PulsarClient.class);
    ProducerBuilder producerBuilder = mock(ProducerBuilder.class);
    when(pulsarClient.newProducer()).thenReturn(producerBuilder);
    when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
    when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
    when(producerBuilder.batchingMaxPublishDelay(anyLong(), any(TimeUnit.class)))
        .thenReturn(producerBuilder);
    when(producerBuilder.maxPendingMessages(anyInt())).thenReturn(producerBuilder);
    when(producerBuilder.create()).thenThrow(PulsarClientException.class);
    when(gatewayService.getPulsarClient()).thenReturn(pulsarClient);

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
  void testReceiveBasicGet() throws Exception {
    openChannel();
    MessageImpl message = createMessageMock();
    when(message.getRedeliveryCount()).thenReturn(2);

    BasicContentHeaderProperties props = new BasicContentHeaderProperties();
    props.setContentType("application/json");
    ContentHeaderBody contentHeader = new ContentHeaderBody(props, TEST_MESSAGE.length);
    byte[] bytes = new byte[contentHeader.getSize()];
    QpidByteBuffer buf = QpidByteBuffer.wrap(bytes);
    contentHeader.writePayload(buf);
    String headers = java.util.Base64.getEncoder().encodeToString(bytes);
    when(message.getProperty(MessageUtils.MESSAGE_PROPERTY_AMQP_HEADERS)).thenReturn(headers);

    MessageImpl message2 = createMessageMock();

    when(consumer.receive(anyInt(), any(TimeUnit.class))).thenReturn(message, message2, null);

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

    byte[] content = getContent(compositeAMQBodyBlock);
    assertArrayEquals(TEST_MESSAGE, content);

    compositeAMQBodyBlock = sendBasicGet();
    assertNotNull(compositeAMQBodyBlock);
    assertTrue(compositeAMQBodyBlock.getMethodBody() instanceof BasicGetOkBody);
    basicGetOkBody = (BasicGetOkBody) compositeAMQBodyBlock.getMethodBody();
    assertEquals(2, basicGetOkBody.getDeliveryTag());
    assertFalse(basicGetOkBody.getRedelivered());
  }

  @Test
  void testReceiveBasicGetEmpty() throws Exception {
    openChannel();
    sendQueueDeclare();

    BasicGetBody basicGetBody =
        new BasicGetBody(0, AMQShortString.createAMQShortString(TEST_QUEUE), true);
    sendData(basicGetBody.generateFrame(CHANNEL_ID));

    Thread.sleep(2000);

    AMQFrame frame = channel.readOutbound();
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
  void testReceiveBasicGetDefaultQueue() throws Exception {
    MessageImpl message = createMessageMock();
    when(consumer.receive(anyInt(), any(TimeUnit.class))).thenReturn(message);

    openChannel();
    sendQueueDeclare();

    ProtocolOutputConverter.CompositeAMQBodyBlock compositeAMQBodyBlock = sendBasicGet("");

    assertNotNull(compositeAMQBodyBlock);
  }

  @Test
  void testReceiveBasicGetExistingExclusiveConsumer() {
    openChannel();
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, true);

    AMQFrame frame = sendBasicGet(TEST_QUEUE);

    assertIsConnectionCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testReceiveBasicConsume() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendBasicConsume(TEST_CONSUMER_TAG, TEST_QUEUE, false);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof BasicConsumeOkBody);
    assertEquals(
        TEST_CONSUMER_TAG, ((BasicConsumeOkBody) frame.getBodyFrame()).getConsumerTag().toString());

    frame = sendBasicConsume("test-consumer-tag2", TEST_QUEUE, false);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof BasicConsumeOkBody);
    assertEquals(
        "test-consumer-tag2",
        ((BasicConsumeOkBody) frame.getBodyFrame()).getConsumerTag().toString());
  }

  @Test
  void testReceiveBasicConsumeEmptyConsumerTag() {
    openChannel();
    sendQueueDeclare();

    AMQFrame frame = sendBasicConsume(null, TEST_QUEUE, false);

    assertNotNull(frame);
    assertTrue(frame.getBodyFrame() instanceof BasicConsumeOkBody);
    assertTrue(
        ((BasicConsumeOkBody) frame.getBodyFrame()).getConsumerTag().toString().startsWith("sgen"));
  }

  @Test
  void testReceiveBasicConsumeQueueNotFound() {
    openChannel();

    AMQFrame frame = sendBasicConsume(null, TEST_QUEUE, false);

    assertIsChannelCloseFrame(frame, ErrorCodes.NOT_FOUND);
  }

  @Test
  void testReceiveBasicConsumeQueueNameMissing() {
    openChannel();

    AMQFrame frame = sendBasicConsume(null, "", false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveBasicConsumeExistingExclusiveConsumer() {
    openChannel();
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, true);

    AMQFrame frame = sendBasicConsume(null, TEST_QUEUE, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testReceiveBasicConsumeExistingConsumerTag() {
    openChannel();
    sendQueueDeclare();
    sendBasicConsume(TEST_CONSUMER_TAG, TEST_QUEUE, true);

    AMQFrame frame = sendBasicConsume(TEST_CONSUMER_TAG, TEST_QUEUE, false);

    assertIsConnectionCloseFrame(frame, ErrorCodes.NOT_ALLOWED);
  }

  @Test
  void testReceiveBasicConsumeExclusiveExistingConsumer() {
    openChannel();
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    AMQFrame frame = sendBasicConsume(null, TEST_QUEUE, true);

    assertIsConnectionCloseFrame(frame, ErrorCodes.ACCESS_REFUSED);
  }

  @Test
  void testConsumerDeliver() throws Exception {
    MessageImpl message = createMessageMock();
    when(message.getRedeliveryCount()).thenReturn(2);

    BasicContentHeaderProperties props = new BasicContentHeaderProperties();
    props.setContentType("application/json");
    ContentHeaderBody contentHeader = new ContentHeaderBody(props, TEST_MESSAGE.length);
    byte[] bytes = new byte[contentHeader.getSize()];
    QpidByteBuffer buf = QpidByteBuffer.wrap(bytes);
    contentHeader.writePayload(buf);
    String headers = java.util.Base64.getEncoder().encodeToString(bytes);
    when(message.getProperty(MessageUtils.MESSAGE_PROPERTY_AMQP_HEADERS)).thenReturn(headers);

    MessageImpl message2 = createMessageMock();

    when(consumer.receiveAsync())
        .thenReturn(
            CompletableFuture.completedFuture(message),
            CompletableFuture.completedFuture(message2),
            new CompletableFuture<>());

    openChannel();
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    ProtocolOutputConverter.CompositeAMQBodyBlock compositeAMQBodyBlock = null;

    for (int i = 0; i < 100 && compositeAMQBodyBlock == null; i++) {
      compositeAMQBodyBlock = channel.readOutbound();
      Thread.sleep(10);
    }

    assertNotNull(compositeAMQBodyBlock);
    assertTrue(
        compositeAMQBodyBlock.getMethodBody()
            instanceof ProtocolOutputConverter.EncodedDeliveryBody);
    BasicDeliverBody basicDeliverBody =
        (BasicDeliverBody)
            ((ProtocolOutputConverter.EncodedDeliveryBody) compositeAMQBodyBlock.getMethodBody())
                .createAMQBody();
    assertEquals(TEST_EXCHANGE, basicDeliverBody.getExchange().toString());
    assertEquals(TEST_QUEUE, basicDeliverBody.getRoutingKey().toString());
    assertEquals(1, basicDeliverBody.getDeliveryTag());
    assertTrue(basicDeliverBody.getRedelivered());

    assertTrue(compositeAMQBodyBlock.getHeaderBody() instanceof ContentHeaderBody);
    ContentHeaderBody contentHeaderBody = (ContentHeaderBody) compositeAMQBodyBlock.getHeaderBody();
    assertEquals(TEST_MESSAGE.length, contentHeaderBody.getBodySize());
    assertEquals("application/json", contentHeaderBody.getProperties().getContentTypeAsString());

    compositeAMQBodyBlock = null;
    for (int i = 0; i < 100 && compositeAMQBodyBlock == null; i++) {
      compositeAMQBodyBlock = channel.readOutbound();
      Thread.sleep(10);
    }
    assertNotNull(compositeAMQBodyBlock);
    assertTrue(
        compositeAMQBodyBlock.getMethodBody()
            instanceof ProtocolOutputConverter.EncodedDeliveryBody);
    basicDeliverBody =
        (BasicDeliverBody)
            ((ProtocolOutputConverter.EncodedDeliveryBody) compositeAMQBodyBlock.getMethodBody())
                .createAMQBody();
    assertEquals(2, basicDeliverBody.getDeliveryTag());
    assertFalse(basicDeliverBody.getRedelivered());
  }

  @Test
  void testReceiveBasicCancel() throws Exception {
    MessageImpl message = createMessageMock();
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendQueueDeclare();
    sendBasicConsume(TEST_CONSUMER_TAG, TEST_QUEUE, false);

    ProtocolOutputConverter.CompositeAMQBodyBlock compositeAMQBodyBlock = null;

    for (int i = 0; i < 100 && compositeAMQBodyBlock == null; i++) {
      compositeAMQBodyBlock = channel.readOutbound();
      Thread.sleep(10);
    }

    assertNotNull(compositeAMQBodyBlock);

    AMQDataBlock frame = sendBasicCancel();

    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() - now < 5000 && !(frame instanceof AMQFrame)) {
      frame = channel.readOutbound();
    }

    assertNotNull(frame);
    assertTrue(frame instanceof AMQFrame);
    AMQBody body = ((AMQFrame) frame).getBodyFrame();
    assertTrue(body instanceof BasicCancelOkBody);
    assertEquals(TEST_CONSUMER_TAG, ((BasicCancelOkBody) body).getConsumerTag().toString());

    // Check nothing is received anymore
    Thread.sleep(100);
    assertNull(channel.readOutbound());
  }

  @Test
  void testReceiveBasicQos() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();

    AMQFrame frame = sendBasicQos(0, 2);
    assertIsBasicQosOk(frame);

    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(2);

    frame = sendBasicQos(0, 3);
    assertIsBasicQosOk(frame);

    assertReceivesMessages(1);

    // Limit by size. 3 unacked messages were already sent so we should only receive one more
    // message
    frame = sendBasicQos(TEST_MESSAGE.length * 4L, 0);
    assertIsBasicQosOk(frame);

    assertReceivesMessages(1);

    frame = sendBasicQos(0, 0);
    assertIsBasicQosOk(frame);

    // Check that consumption resumes without limits
    Thread.sleep(100);
    for (int i = 0; i < 10; i++) {
      assertTrue(channel.readOutbound() instanceof ProtocolOutputConverter.CompositeAMQBodyBlock);
    }
  }

  @Test
  void testReceiveBasicAck() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicAck(2, false);

    assertReceivesMessages(1);

    verify(consumer).acknowledgeAsync(MessageId.latest);
  }

  @Test
  void testReceiveBasicAckMultiple() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicAck(2, true);

    assertReceivesMessages(2);

    verify(consumer, times(2)).acknowledgeAsync(MessageId.latest);
  }

  @Test
  void testReceiveBasicAckInvalidDeliveryTag() {
    openChannel();
    sendBasicQos(0, 1);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    sendBasicAck(1, true);
    assertIsChannelCloseFrame(channel.readOutbound(), ErrorCodes.IN_USE);
  }

  @Test
  void testReceiveBasicNack() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicNack(2, false, false);

    assertReceivesMessages(1);

    verify(consumer).acknowledgeAsync(MessageId.latest);
  }

  @Test
  void testReceiveBasicNackMultiple() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicNack(2, true, false);

    assertReceivesMessages(2);

    verify(consumer, times(2)).acknowledgeAsync(MessageId.latest);
  }

  @Test
  void testReceiveBasicNackRequeue() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicNack(2, false, true);

    assertReceivesMessages(1);

    verify(consumer).negativeAcknowledge(MessageId.latest);
  }

  @Test
  void testReceiveBasicReject() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicReject(2, false);

    assertReceivesMessages(1);

    verify(consumer).acknowledgeAsync(MessageId.latest);
  }

  @Test
  void testReceiveBasicRejectRequeue() throws Exception {
    MessageImpl message = createMessageMock();

    // Binding always has a message ready for delivery
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicReject(2, true);

    assertReceivesMessages(1);

    verify(consumer).negativeAcknowledge(MessageId.latest);
  }

  @Test
  void testReceiveBasicRecover() throws Exception {
    MessageImpl message = createMessageMock();
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    sendBasicRecover(true);

    assertReceivesMessages(3);

    verify(consumer, times(3)).negativeAcknowledge(MessageId.latest);
  }

  @Test
  void testReceiveBasicRecoverSync() throws Exception {
    MessageImpl message = createMessageMock();
    when(consumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));

    openChannel();
    sendBasicQos(0, 3);
    sendQueueDeclare();
    sendBasicConsume(null, TEST_QUEUE, false);

    assertReceivesMessages(3);

    AMQFrame frame = sendBasicRecoverSync(true);
    assertNotNull(frame);
    assert (frame.getBodyFrame() instanceof BasicRecoverSyncOkBody);

    assertReceivesMessages(3);

    verify(consumer, times(3)).negativeAcknowledge(MessageId.latest);
  }

  private void openChannel() {
    openConnection();
    sendChannelOpen();
  }

  private AMQFrame sendBasicRecoverSync(boolean requeue) {
    BasicRecoverSyncBody basicRecoverBody =
        new BasicRecoverSyncBody(ProtocolVersion.v0_91, requeue);
    return exchangeData(basicRecoverBody.generateFrame(CHANNEL_ID));
  }

  private void sendBasicRecover(boolean requeue) {
    BasicRecoverBody basicRecoverBody = new BasicRecoverBody(requeue);
    sendData(basicRecoverBody.generateFrame(CHANNEL_ID));
  }

  private void sendBasicReject(long deliveryTag, boolean requeue) {
    BasicRejectBody basicRejectBody = new BasicRejectBody(deliveryTag, requeue);
    sendData(basicRejectBody.generateFrame(CHANNEL_ID));
  }

  private void sendBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
    BasicNackBody basicNackBody = new BasicNackBody(deliveryTag, multiple, requeue);
    sendData(basicNackBody.generateFrame(CHANNEL_ID));
  }

  private void sendBasicAck(long deliveryTag, boolean multiple) {
    BasicAckBody basicAckBody = new BasicAckBody(deliveryTag, multiple);
    sendData(basicAckBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendBasicQos(long prefetchSize, int prefetchCount) {
    BasicQosBody basicQosBody = new BasicQosBody(prefetchSize, prefetchCount, true);
    return exchangeData(basicQosBody.generateFrame(CHANNEL_ID));
  }

  private <T> T sendBasicCancel() {
    BasicCancelBody basicConsumeBody =
        new BasicCancelBody(AMQShortString.createAMQShortString(TEST_CONSUMER_TAG), false);
    return exchangeData(basicConsumeBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendBasicConsume(String consumerTag, String queueName, boolean exclusive) {
    BasicConsumeBody basicConsumeBody =
        new BasicConsumeBody(
            0,
            AMQShortString.createAMQShortString(queueName),
            AMQShortString.createAMQShortString(consumerTag),
            false,
            false,
            exclusive,
            false,
            FieldTable.convertToFieldTable(new HashMap<>()));
    return exchangeData(basicConsumeBody.generateFrame(CHANNEL_ID));
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

  private AMQFrame sendExchangeDeclare() {
    return sendExchangeDeclare(TEST_EXCHANGE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS, false);
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

  private AMQFrame sendQueueUnbind(String key) {
    return sendQueueUnbind(TEST_QUEUE, TEST_EXCHANGE, key);
  }

  private AMQFrame sendQueueUnbind(String queue, String exchange, String key) {
    QueueUnbindBody queueUnbindBody =
        new QueueUnbindBody(
            0,
            AMQShortString.createAMQShortString(queue),
            AMQShortString.createAMQShortString(exchange),
            AMQShortString.createAMQShortString(key),
            FieldTable.convertToFieldTable(Collections.emptyMap()));
    return exchangeData(queueUnbindBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendQueueBind(String key) {
    return sendQueueBind(TEST_QUEUE, TEST_EXCHANGE, key);
  }

  private AMQFrame sendQueueBind(String queue, String exchange, String key) {
    QueueBindBody queueBindBody =
        new QueueBindBody(
            0,
            AMQShortString.createAMQShortString(queue),
            AMQShortString.createAMQShortString(exchange),
            AMQShortString.createAMQShortString(key),
            false,
            FieldTable.convertToFieldTable(Collections.emptyMap()));
    return exchangeData(queueBindBody.generateFrame(CHANNEL_ID));
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
      attributes.put(LIFETIME_POLICY, lifetimePolicy);
    }
    if (exclusivityPolicy != null) {
      attributes.put(EXCLUSIVE, exclusivityPolicy);
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

  private AMQFrame sendQueueDelete(String queue, boolean ifUnused) {
    QueueDeleteBody queueDeleteBody =
        new QueueDeleteBody(0, AMQShortString.createAMQShortString(queue), ifUnused, false, false);
    return exchangeData(queueDeleteBody.generateFrame(CHANNEL_ID));
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

  private void assertIsBasicQosOk(AMQDataBlock read) {
    assertTrue(read instanceof AMQFrame);
    assertTrue(((AMQFrame) read).getBodyFrame() instanceof BasicQosOkBody);
  }

  private void assertReceivesMessages(int numberOfMessages) throws InterruptedException {
    for (int i = 0; i < numberOfMessages; i++) {
      Object readOutbound = null;
      for (int j = 0; j < 500 && readOutbound == null; j++) {
        readOutbound = channel.readOutbound();
        Thread.sleep(10);
      }
      assertTrue(readOutbound instanceof ProtocolOutputConverter.CompositeAMQBodyBlock);
    }
    assertNull(channel.readOutbound());
  }

  private MessageImpl createMessageMock() {
    return createMessageMock("test-message");
  }

  private MessageImpl createMessageMock(String content) {
    MessageImpl message = mock(MessageImpl.class);
    when(message.getData()).thenReturn(content.getBytes(StandardCharsets.UTF_8));
    when(message.size()).thenReturn(content.getBytes(StandardCharsets.UTF_8).length);
    when(message.getTopicName()).thenReturn(TEST_EXCHANGE + ".__" + TEST_QUEUE);
    when(message.getRedeliveryCount()).thenReturn(0);
    when(message.getMessageId()).thenReturn(MessageId.latest);
    return message;
  }

  private byte[] getContent(ProtocolOutputConverter.CompositeAMQBodyBlock compositeAMQBodyBlock) {
    AMQBody contentBody = compositeAMQBodyBlock.getContentBody();
    ByteBuf byteBuf = Unpooled.buffer(contentBody.getSize());
    contentBody.writePayload(new NettyByteBufferSender(byteBuf));
    return byteBuf.array();
  }
}
