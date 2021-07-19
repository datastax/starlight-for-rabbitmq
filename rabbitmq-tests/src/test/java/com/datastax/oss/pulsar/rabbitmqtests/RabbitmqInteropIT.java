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
package com.datastax.oss.pulsar.rabbitmqtests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.pulsar.rabbitmqgw.GatewayConfiguration;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayService;
import com.datastax.oss.pulsar.rabbitmqtests.utils.PulsarCluster;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.codec.binary.Base64;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RabbitmqInteropIT {

  public static final String TEST_QUEUE = "test-queue";
  public static final String TEST_CONSUMER_TAG = "test-consumer-tag";
  public static final String TEST_MESSAGE = "test-message";
  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;
  private static ConnectionFactory factory;

  @BeforeAll
  public static void before() throws Exception {
    cluster = new PulsarCluster(tempDir);
    cluster.start();
    GatewayConfiguration config = new GatewayConfiguration();
    config.setBrokerServiceURL(cluster.getAddress());
    config.setServicePort(Optional.of(PortManager.nextFreePort()));
    gatewayService = new GatewayService(config);
    gatewayService.start();

    factory = new ConnectionFactory();
    factory.setVirtualHost("/");
    factory.setHost("localhost");
    factory.setPort(config.getServicePort().get());
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @Test
  void testRabbitProducerPulsarConsumer() throws Exception {
    Connection conn = factory.newConnection();
    Channel channel = conn.createChannel();

    PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(cluster.getAddress()).build();
    Consumer<String> consumer =
        pulsarClient
            .newConsumer(Schema.STRING)
            .topic("amq.default")
            .subscriptionName("test-subscription")
            .subscribe();

    Date now = new Date();
    AMQP.BasicProperties properties =
        new AMQP.BasicProperties.Builder()
            .contentType("application/octet-stream")
            .timestamp(now)
            .build();
    channel.basicPublish("", "", properties, TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));

    Message<String> receive = consumer.receive(10, TimeUnit.SECONDS);

    assertNotNull(receive);
    assertEquals(TEST_MESSAGE, receive.getValue());

    assertTrue(receive.hasProperty("amqp-headers"));
    byte[] bytes = Base64.decodeBase64(receive.getProperty("amqp-headers"));
    ContentHeaderBody contentHeaderBody =
        new ContentHeaderBody(QpidByteBuffer.wrap(bytes), bytes.length);
    assertEquals(
        "application/octet-stream", contentHeaderBody.getProperties().getContentType().toString());

    assertEquals(now.getTime() / 1000, receive.getEventTime());

    consumer.close();
    channel.close();
    conn.close();
  }

  @Test
  void testPulsarProducerRabbitConsumer() throws Exception {
    Connection conn = factory.newConnection();
    Channel channel = conn.createChannel();

    PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(cluster.getAddress()).build();
    Producer<String> producer =
        pulsarClient.newProducer(Schema.STRING).topic("amq.default$$test-queue").create();

    channel.queueDeclare(TEST_QUEUE, true, false, false, new HashMap<>());
    producer.send(TEST_MESSAGE);

    GetResponse getResponse = null;
    for (int i = 0; i < 100 && getResponse == null; i++) {
      getResponse = channel.basicGet(TEST_QUEUE, false);
      Thread.sleep(10);
    }
    assertNotNull(getResponse);
    assertEquals("amq.default", getResponse.getEnvelope().getExchange());
    assertEquals(TEST_QUEUE, getResponse.getEnvelope().getRoutingKey());
    assertArrayEquals(TEST_MESSAGE.getBytes(StandardCharsets.UTF_8), getResponse.getBody());

    producer.close();
    channel.close();
    conn.close();
  }

  @Test
  void testRabbitProducerRabbitConsumer() throws Exception {
    Connection conn1 = factory.newConnection();
    Channel channel1 = conn1.createChannel();

    Connection conn2 = factory.newConnection();
    Channel channel2 = conn2.createChannel();

    channel1.queueDeclare(TEST_QUEUE, true, false, false, new HashMap<>());

    Date now = new Date();
    AMQP.BasicProperties properties =
        new AMQP.BasicProperties.Builder()
            .contentType("application/octet-stream")
            .timestamp(now)
            .build();
    channel2.basicPublish(
        "", TEST_QUEUE, properties, TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));

    CountDownLatch consumeOkReceived = new CountDownLatch(1);
    CountDownLatch messageReceived = new CountDownLatch(1);
    CountDownLatch consumerCanceled = new CountDownLatch(1);
    channel1.basicConsume(
        TEST_QUEUE,
        true,
        TEST_CONSUMER_TAG,
        new com.rabbitmq.client.Consumer() {
          @Override
          public void handleConsumeOk(String consumerTag) {
            assertEquals(TEST_CONSUMER_TAG, consumerTag);
            consumeOkReceived.countDown();
          }

          @Override
          public void handleCancelOk(String consumerTag) {
            assertEquals(TEST_CONSUMER_TAG, consumerTag);
            consumerCanceled.countDown();
          }

          @Override
          public void handleCancel(String consumerTag) {}

          @Override
          public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {}

          @Override
          public void handleRecoverOk(String consumerTag) {}

          @Override
          public void handleDelivery(
              String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            assertEquals(TEST_CONSUMER_TAG, consumerTag);
            assertEquals("amq.default", envelope.getExchange());
            assertEquals(TEST_QUEUE, envelope.getRoutingKey());
            assertEquals("application/octet-stream", properties.getContentType());
            assertEquals(now.getTime() / 1000, properties.getTimestamp().getTime() / 1000);
            assertArrayEquals(TEST_MESSAGE.getBytes(StandardCharsets.UTF_8), body);
            messageReceived.countDown();
          }
        });

    assertTrue(consumeOkReceived.await(5, TimeUnit.SECONDS));
    assertTrue(messageReceived.await(5, TimeUnit.SECONDS));

    channel1.basicCancel(TEST_CONSUMER_TAG);

    assertTrue(consumerCanceled.await(5, TimeUnit.SECONDS));

    channel1.close();
    conn1.close();
    channel2.close();
    conn2.close();
  }
}
