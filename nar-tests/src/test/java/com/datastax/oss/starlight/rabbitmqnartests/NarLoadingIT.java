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
package com.datastax.oss.starlight.rabbitmqnartests;

import com.datastax.oss.starlight.rabbitmqnartests.utils.BookKeeperCluster;
import com.datastax.oss.starlight.rabbitmqnartests.utils.PulsarCluster;
import com.google.common.collect.Sets;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class NarLoadingIT {

  @TempDir public static Path tempDir;

  @Test
  public void loadsNarWithZkDiscovery() throws Exception {
    loadProtocolHandler(true);
  }

  @Test
  public void loadsNarWithBrokerServiceUrl() throws Exception {
    loadProtocolHandler(false);
  }

  private void loadProtocolHandler(boolean zkDiscovery) throws Exception {
    Path handlerPath = Paths.get("target/test-protocol-handler.nar").toAbsolutePath();
    String protocolHandlerDir = handlerPath.toFile().getParent();

    ServiceConfiguration pulsarConfig = new ServiceConfiguration();
    pulsarConfig.setProtocolHandlerDirectory(protocolHandlerDir);
    pulsarConfig.setMessagingProtocols(Sets.newHashSet("rabbitmq"));

    int portOnBroker = PortManager.nextFreePort();
    pulsarConfig.getProperties().put("amqpListeners", "amqp://127.0.0.1:" + portOnBroker);

    BookKeeperCluster bookKeeperCluster =
        new BookKeeperCluster(tempDir, PortManager.nextFreePort());
    pulsarConfig
        .getProperties()
        .put("configurationStoreServers", bookKeeperCluster.getZooKeeperAddress());

    PulsarCluster cluster = new PulsarCluster(pulsarConfig, bookKeeperCluster);
    cluster.start();

    ConnectionFactory factory = new ConnectionFactory();
    factory.setPort(portOnBroker);
    Connection connection = factory.newConnection();
    connection.close();

    cluster.close();
  }
}
