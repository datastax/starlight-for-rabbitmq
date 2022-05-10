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
/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.starlight.rabbitmqnartests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.Network;

public class DockerIT {

  private static final String IMAGE_LUNASTREAMING210 = "datastax/lunastreaming:2.10_0.1";
  private static final String IMAGE_PULSAR210 = "apachepulsar/pulsar:2.10.0";

  @ParameterizedTest
  @ValueSource(strings = {IMAGE_PULSAR210, IMAGE_LUNASTREAMING210})
  public void test(String image) throws Exception {
    // create a docker network
    try (Network network = Network.newNetwork();
        PulsarContainer pulsarContainer = new PulsarContainer(network, true, image)) {
      // start Pulsar and wait for it to be ready to accept requests
      pulsarContainer.start();

      List<String> amqpUris =
          Arrays.asList(
              "amqp://pulsar:5672", "amqp://pulsarproxy:5672", "amqps://pulsarproxy:5671");
      for (String amqpUri : amqpUris) {
        URI uri = URI.create(amqpUri);
        URI containerUri = uri;
        if (uri.getHost().equals("pulsar")) {
          containerUri =
              new URI(
                  uri.getScheme(),
                  uri.getUserInfo(),
                  "localhost",
                  pulsarContainer.getPulsarContainer().getMappedPort(uri.getPort()),
                  uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
        }
        if (uri.getHost().equals("pulsarproxy")) {
          containerUri =
              new URI(
                  uri.getScheme(),
                  uri.getUserInfo(),
                  "localhost",
                  pulsarContainer.getProxyContainer().getMappedPort(uri.getPort()),
                  uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(containerUri);
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
          final String queue = "test-queue-" + UUID.randomUUID();
          channel.queueDeclare(queue, true, false, false, new HashMap<>());
          channel.basicPublish("", queue, null, "test".getBytes(StandardCharsets.UTF_8));

          final CountDownLatch latch = new CountDownLatch(1);
          Consumer c =
              new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(
                    String consumerTag,
                    Envelope envelope,
                    AMQP.BasicProperties properties,
                    byte[] body)
                    throws IOException {
                  if (new String(body, StandardCharsets.UTF_8).equals("test")) {
                    latch.countDown();
                  }
                }
              };
          channel.basicConsume(queue, c);
          assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
      }
    }
  }
}
