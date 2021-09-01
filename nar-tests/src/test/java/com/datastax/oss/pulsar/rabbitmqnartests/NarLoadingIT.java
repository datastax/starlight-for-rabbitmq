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
package com.datastax.oss.pulsar.rabbitmqnartests;

import com.datastax.oss.pulsar.rabbitmqnartests.utils.PulsarCluster;
import com.google.common.collect.Sets;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class NarLoadingIT {

  @TempDir public static Path tempDir;

  @Test
  public void loadsNar() throws Exception {
    PulsarCluster cluster = new PulsarCluster(tempDir);
    cluster.start();

    ProxyConfiguration proxyConfiguration = new ProxyConfiguration();

    Path handlerPath = Paths.get("target/test-protocol-proxy-handler.nar").toAbsolutePath();

    String protocolHandlerDir = handlerPath.toFile().getParent();

    proxyConfiguration.setProxyProtocolHandlerDirectory(protocolHandlerDir);
    proxyConfiguration.setProxyMessagingProtocols(Sets.newHashSet("rabbitmq"));

    int port = PortManager.nextFreePort();
    proxyConfiguration.getProperties().put("amqpServicePort",String.valueOf(port));
    proxyConfiguration
        .getProperties()
        .put("zookeeperServers", cluster.getService().getConfig().getZookeeperServers());
    proxyConfiguration
        .getProperties()
        .put("brokerServiceURL", cluster.getService().getBrokerServiceUrl());
    proxyConfiguration
        .getProperties()
        .put("brokerWebServiceURL", cluster.getService().getWebServiceAddress());

    ProxyService pulsarProxy = new ProxyService(proxyConfiguration, null);
    pulsarProxy.start();

    ConnectionFactory factory = new ConnectionFactory();
    factory.setPort(port);

    Connection connection = factory.newConnection();

    connection.close();
    pulsarProxy.close();
  }
}
