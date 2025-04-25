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

import static org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody.CONNECTION_CLOSE_OK_0_9;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datastax.oss.starlight.rabbitmq.metadata.ContextMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.prometheus.client.CollectorRegistry;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.stubbing.Answer;

public class AbstractBaseTest {
  public static final int CHANNEL_ID = 42;
  protected final GatewayConfiguration config = new GatewayConfiguration();
  protected GatewayService gatewayService;
  protected GatewayConnection connection;
  protected EmbeddedChannel channel;
  protected ProducerBase producer = mock(ProducerBase.class);
  protected ConsumerBase consumer = mock(ConsumerBase.class);

  @BeforeEach
  void setup() throws Exception {
    CollectorRegistry.defaultRegistry.clear();
    gatewayService = spy(new GatewayService(config, null));
    connection = new GatewayConnection(gatewayService);
    channel = new EmbeddedChannel(connection, new AMQDataBlockEncoder());

    PulsarClient pulsarClient = mock(PulsarClient.class);

    ProducerBuilder producerBuilder = mock(ProducerBuilder.class);
    when(pulsarClient.newProducer()).thenReturn(producerBuilder);
    when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
    when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
    when(producerBuilder.batchingMaxPublishDelay(anyLong(), any(TimeUnit.class)))
        .thenReturn(producerBuilder);
    when(producerBuilder.maxPendingMessages(anyInt())).thenReturn(producerBuilder);
    when(producerBuilder.create()).thenReturn(producer);

    ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class);
    when(pulsarClient.newConsumer()).thenReturn(consumerBuilder);
    when(consumerBuilder.topics(anyList())).thenReturn(consumerBuilder);
    when(consumerBuilder.topic(anyString())).thenReturn(consumerBuilder);
    when(consumerBuilder.subscriptionName(anyString())).thenReturn(consumerBuilder);
    when(consumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(consumerBuilder);
    when(consumerBuilder.receiverQueueSize(anyInt())).thenReturn(consumerBuilder);
    when(consumerBuilder.isAckReceiptEnabled(anyBoolean())).thenReturn(consumerBuilder);
    when(consumerBuilder.enableBatchIndexAcknowledgment(anyBoolean())).thenReturn(consumerBuilder);
    when(consumerBuilder.negativeAckRedeliveryDelay(anyLong(), any(TimeUnit.class)))
        .thenReturn(consumerBuilder);
    when(consumerBuilder.maxTotalReceiverQueueSizeAcrossPartitions(anyInt()))
        .thenReturn(consumerBuilder);
    when(consumerBuilder.subscribeAsync()).thenReturn(CompletableFuture.completedFuture(consumer));

    when(consumer.receive(anyInt(), any(TimeUnit.class))).thenReturn(null);
    when(consumer.receiveAsync()).thenReturn(new CompletableFuture<>());
    when(consumer.getLastMessageIdAsync())
        .thenReturn(CompletableFuture.completedFuture(MessageId.latest));
    when(consumer.acknowledgeAsync(any(MessageId.class))).thenReturn(new CompletableFuture<>());

    doReturn(pulsarClient).when(gatewayService).getPulsarClient();
    gatewayService.setExchangeKeyConsumer(mock(Consumer.class));
    gatewayService.setExchangeKeyProducer(mock(Producer.class));

    PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
    Topics topics = mock(Topics.class);

    doReturn(CompletableFuture.completedFuture(null))
        .when(topics)
        .createSubscriptionAsync(anyString(), anyString(), any());
    doReturn(CompletableFuture.completedFuture(MessageId.latest))
        .when(topics)
        .getLastMessageIdAsync(anyString());
    doReturn(topics).when(pulsarAdmin).topics();
    doReturn(pulsarAdmin).when(gatewayService).getPulsarAdmin();

    Answer<CompletionStage<ContextMetadata>> answer =
        invocation -> {
          Versioned<ContextMetadata> contextMetadata =
              (Versioned<ContextMetadata>) invocation.getArguments()[0];
          gatewayService.setContextMetadata(contextMetadata);
          return CompletableFuture.completedFuture(
              gatewayService.updateContext(contextMetadata.model()));
        };
    doAnswer(answer).when(gatewayService).saveContext(any());
  }

  @AfterEach
  void cleanup() throws Exception {
    gatewayService.close();
    channel.close().await(5, TimeUnit.SECONDS);
  }

  protected void sendData(AMQDataBlock data) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    data.writePayload(new NettyByteBufferSender(byteBuf));
    channel.writeInbound(byteBuf);
  }

  protected <T> T exchangeDataNoWait(AMQDataBlock data) {
    sendData(data);
    return channel.readOutbound();
  }

  protected <T> T exchangeData(AMQDataBlock data) {
    sendData(data);

    T read = null;
    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() - now < 1000 && read == null) {
      read = channel.readOutbound();
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
    return read;
  }

  protected void openConnection() {
    sendProtocolHeader();
    sendConnectionStartOk();
    sendConnectionTuneOk();
    sendConnectionOpen("/");
  }

  protected AMQFrame sendProtocolHeader() {
    return exchangeData(new ProtocolInitiation(ProtocolVersion.v0_91));
  }

  protected AMQFrame sendConnectionStartOk() {
    return sendConnectionStartOk("PLAIN");
  }

  protected AMQFrame sendConnectionStartOk(String mechanism) {
    ConnectionStartOkBody connectionStartOkBody =
        new ConnectionStartOkBody(
            FieldTable.convertToFieldTable(Collections.emptyMap()),
            AMQShortString.createAMQShortString(mechanism),
            new byte[0],
            AMQShortString.createAMQShortString("en_US"));
    return exchangeData(connectionStartOkBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionTuneOk() {
    return sendConnectionTuneOk(256, 128 * 1024, 60);
  }

  protected AMQFrame sendConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {
    ConnectionTuneOkBody connectionTuneOkBody =
        new ConnectionTuneOkBody(channelMax, frameMax, heartbeat);
    return exchangeDataNoWait(connectionTuneOkBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionOpen(String vhost) {
    ConnectionOpenBody connectionOpenBody =
        new ConnectionOpenBody(
            AMQShortString.createAMQShortString(vhost),
            AMQShortString.createAMQShortString("test-capabilities"),
            false);
    return exchangeData(connectionOpenBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionClose() {
    ConnectionCloseBody connectionCloseBody =
        new ConnectionCloseBody(
            ProtocolVersion.v0_91,
            ErrorCodes.INTERNAL_ERROR,
            AMQShortString.createAMQShortString("test-replyText"),
            43,
            44);
    return exchangeData(connectionCloseBody.generateFrame(1));
  }

  protected AMQFrame sendConnectionCloseOk() {
    return exchangeData(CONNECTION_CLOSE_OK_0_9.generateFrame(1));
  }

  protected AMQFrame sendChannelOpen() {
    return sendChannelOpen(CHANNEL_ID);
  }

  protected AMQFrame sendChannelOpen(int channelId) {
    ChannelOpenBody channelOpenBody = new ChannelOpenBody();
    return exchangeData(channelOpenBody.generateFrame(channelId));
  }

  protected AMQFrame sendChannelCloseOk() {
    return exchangeData(ChannelCloseOkBody.INSTANCE.generateFrame(CHANNEL_ID));
  }

  protected void assertIsConnectionCloseFrame(AMQFrame frame, int errorCode) {
    assertNotNull(frame);
    assertEquals(0, frame.getChannel());
    AMQBody body = frame.getBodyFrame();
    assertTrue(body instanceof ConnectionCloseBody);
    ConnectionCloseBody connectionCloseBody = (ConnectionCloseBody) body;
    assertEquals(errorCode, connectionCloseBody.getReplyCode());
  }

  public static class NettyByteBufferSender implements ByteBufferSender {

    private final ByteBuf byteBuf;

    NettyByteBufferSender(ByteBuf byteBuf) {
      this.byteBuf = byteBuf;
    }

    @Override
    public boolean isDirectBufferPreferred() {
      return true;
    }

    @Override
    public void send(QpidByteBuffer msg) {
      try {
        byteBuf.writeBytes(msg.asInputStream(), msg.remaining());
      } catch (Exception e) {
        // Oops
      }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }
}
