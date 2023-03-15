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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class PulsarContainer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PulsarContainer.class);

  protected static final String PROTOCOLS_TEST_PROTOCOL_HANDLER_NAR = "/test-protocol-handler.nar";

  private GenericContainer<?> pulsarContainer;
  private GenericContainer<?> proxyContainer;
  private final Network network;
  private final boolean startProxy;
  private final String image;

  public PulsarContainer(Network network, boolean startProxy, String image) {
    this.network = network;
    this.startProxy = startProxy;
    this.image = image;
  }

  public void start() throws Exception {
    pulsarContainer =
        new org.testcontainers.containers.PulsarContainer(
                DockerImageName.parse(image).asCompatibleSubstituteFor("apachepulsar/pulsar"))
            .withNetwork(network)
            .withNetworkAliases("pulsar")
            .withExposedPorts(8080, 5672) // ensure that the ports are listening
            .withEnv("PULSAR_STANDALONE_USE_ZOOKEEPER", "true")
            .withClasspathResourceMapping(
                PROTOCOLS_TEST_PROTOCOL_HANDLER_NAR,
                "/pulsar/protocols/starlight-for-rabbitmq.nar",
                BindMode.READ_ONLY)
            .withClasspathResourceMapping(
                "standalone_with_s4r.conf", "/pulsar/conf/standalone.conf", BindMode.READ_ONLY)
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  log.info(text);
                });
    pulsarContainer.start();

    if (startProxy) {
      proxyContainer =
          new GenericContainer<>(image)
              .withNetwork(network)
              .withNetworkAliases("pulsarproxy")
              .withExposedPorts(8089, 5672, 5671) // ensure that the ports are listening
              .withClasspathResourceMapping(
                  PROTOCOLS_TEST_PROTOCOL_HANDLER_NAR,
                  "/pulsar/proxyextensions/starlight-for-rabbitmq.nar",
                  BindMode.READ_ONLY)
              .withClasspathResourceMapping(
                  "proxy_with_s4r.conf", "/pulsar/conf/proxy.conf", BindMode.READ_ONLY)
              .withClasspathResourceMapping(
                  "ssl/proxy.cert.pem", "/pulsar/conf/proxy.cert.pem", BindMode.READ_ONLY)
              .withClasspathResourceMapping(
                  "ssl/proxy.key-pk8.pem", "/pulsar/conf/proxy.key-pk8.pem", BindMode.READ_ONLY)
              .withClasspathResourceMapping(
                  "ssl/ca.cert.pem", "/pulsar/conf/ca.cert.pem", BindMode.READ_ONLY)
              .withCommand("bin/pulsar", "proxy")
              .waitingFor(Wait.forLogMessage(".*Server started at end point.*", 1))
              .withLogConsumer(
                  (f) -> {
                    String text = f.getUtf8String().trim();
                    log.info(text);
                  });
      proxyContainer.start();
    }
  }

  @Override
  public void close() {
    if (proxyContainer != null) {
      proxyContainer.stop();
    }
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
  }

  public GenericContainer<?> getPulsarContainer() {
    return pulsarContainer;
  }

  public GenericContainer<?> getProxyContainer() {
    return proxyContainer;
  }
}
