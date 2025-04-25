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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.prometheus.client.CollectorRegistry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.junit.jupiter.api.Test;

public class MetricsTest extends AbstractBaseTest {
  public static final String TEST_QUEUE = "test-queue";
  public static final byte[] TEST_MESSAGE = "test-message".getBytes(StandardCharsets.UTF_8);

  @Test
  void testBytesInOutMetrics() {
    CollectorRegistry registry = CollectorRegistry.defaultRegistry;
    assertNull(
        registry.getSampleValue(
            "server_rabbitmq_in_bytes_total",
            new String[] {"namespace"},
            new String[] {"public/default"}));
    assertNull(
        registry.getSampleValue(
            "server_rabbitmq_out_bytes_total",
            new String[] {"namespace"},
            new String[] {"public/default"}));

    TypedMessageBuilder messageBuilder = mock(TypedMessageBuilder.class);

    when(producer.newMessage()).thenReturn(messageBuilder);
    when(messageBuilder.sendAsync())
        .thenReturn(CompletableFuture.completedFuture(new MessageIdImpl(1, 2, 3)));

    openConnection();
    sendChannelOpen();
    sendBasicPublish();

    BasicContentHeaderProperties props = new BasicContentHeaderProperties();
    props.setContentType("application/json");
    props.setTimestamp(1234);

    exchangeData(ContentHeaderBody.createAMQFrame(CHANNEL_ID, props, TEST_MESSAGE.length));
    sendMessageContent();

    assertTrue(
        registry.getSampleValue(
                "server_rabbitmq_in_bytes_total",
                new String[] {"namespace"},
                new String[] {"public/default"})
            > 0);
    assertTrue(
        registry.getSampleValue(
                "server_rabbitmq_out_bytes_total",
                new String[] {"namespace"},
                new String[] {"public/default"})
            > 0);
    assertTrue(
        registry.getSampleValue(
                "server_rabbitmq_in_bytes_created",
                new String[] {"namespace"},
                new String[] {"public/default"})
            > 0);
    assertTrue(
        registry.getSampleValue(
                "server_rabbitmq_out_bytes_created",
                new String[] {"namespace"},
                new String[] {"public/default"})
            > 0);
    assertTrue(
        registry.getSampleValue(
                "server_rabbitmq_in_messages_total",
                new String[] {"namespace"},
                new String[] {"public/default"})
            > 0);
  }

  @Test
  void testActiveAndNewConnections() {
    CollectorRegistry registry = CollectorRegistry.defaultRegistry;

    assertEquals(1, registry.getSampleValue("server_rabbitmq_active_connections"));
    assertEquals(1, registry.getSampleValue("server_rabbitmq_new_connections"));

    channel.close();

    assertEquals(0, registry.getSampleValue("server_rabbitmq_active_connections"));
    assertEquals(1, registry.getSampleValue("server_rabbitmq_new_connections"));
  }

  private AMQFrame sendBasicPublish() {
    BasicPublishBody basicPublishBody = new BasicPublishBody(0, null, null, false, false);
    return exchangeData(basicPublishBody.generateFrame(CHANNEL_ID));
  }

  private AMQFrame sendMessageContent() {
    ContentBody contentBody = new ContentBody(ByteBuffer.wrap(TEST_MESSAGE));
    return exchangeData(ContentBody.createAMQFrame(CHANNEL_ID, contentBody));
  }
}
