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
package com.datastax.oss.starlight.rabbitmqtests;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.starlight.rabbitmq.ConfigurationUtils;
import com.datastax.oss.starlight.rabbitmq.GatewayConfiguration;
import com.datastax.oss.starlight.rabbitmq.GatewayService;
import com.datastax.oss.starlight.rabbitmqtests.utils.PulsarCluster;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.util.PortManager;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BrokerClientTlsConnectionWithKeyStoreIT {

  private static final String BROKER_KEYSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/broker.keystore.jks";
  private static final String BROKER_TRUSTSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/broker.truststore.jks";
  private static final String BROKER_KEYSTORE_PW = "111111";
  private static final String BROKER_TRUSTSTORE_PW = "111111";

  private static final String CLIENT_TRUSTSTORE_FILE_PATH =
      "./src/test/resources/authentication/keystoretls/client.truststore.jks";
  private static final String CLIENT_TRUSTSTORE_PW = "111111";

  private static final String KEYSTORE_TYPE = "JKS";

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;
  private static final int brokerServicePortTls = PortManager.nextFreePort();
  private static final int webServicePortTls = PortManager.nextFreePort();
  private GatewayConfiguration gatewayConfiguration;

  @BeforeAll
  public static void before() throws Exception {
    ServiceConfiguration pulsarConfig = new ServiceConfiguration();

    pulsarConfig.setTlsEnabledWithKeyStore(true);
    pulsarConfig.setTlsKeyStoreType(KEYSTORE_TYPE);
    pulsarConfig.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
    pulsarConfig.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);
    pulsarConfig.setTlsTrustStoreType(KEYSTORE_TYPE);
    pulsarConfig.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
    pulsarConfig.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);

    pulsarConfig.setBrokerServicePort(Optional.empty());
    pulsarConfig.setBrokerServicePortTls(Optional.of(brokerServicePortTls));

    pulsarConfig.setWebServicePort(Optional.empty());
    pulsarConfig.setWebServicePortTls(Optional.of(webServicePortTls));

    cluster = new PulsarCluster(tempDir, pulsarConfig);
    cluster.start();
  }

  @AfterAll
  public static void after() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }

  @BeforeEach
  public void beforeEach() {
    CollectorRegistry.defaultRegistry.clear();
    gatewayConfiguration = new GatewayConfiguration();
    gatewayConfiguration.setBrokerServiceURL("pulsar+ssl://localhost:" + brokerServicePortTls);
    gatewayConfiguration.setBrokerWebServiceURL("https://localhost:" + webServicePortTls);

    gatewayConfiguration.setAmqpListeners(
        Collections.singleton("amqp://127.0.0.1:" + PortManager.nextFreePort()));
    gatewayConfiguration.setConfigurationStoreServers(
        cluster.getService().getConfig().getConfigurationStoreServers());
    gatewayConfiguration.setTlsEnabledWithBroker(true);
    // PulsarAdmin will only verify the client cert if this is enabled
    gatewayConfiguration.setTlsHostnameVerificationEnabled(true);
  }

  @AfterEach
  public void afterEach() throws IOException {
    if (gatewayService != null) {
      gatewayService.close();
    }
  }

  @Test
  public void testBrokerTlsConnectionWithKeyStoreSuccessful() throws Exception {
    gatewayConfiguration.setBrokerClientTlsEnabledWithKeyStore(true);
    gatewayConfiguration.setBrokerClientTlsTrustStoreType(KEYSTORE_TYPE);
    gatewayConfiguration.setBrokerClientTlsTrustStore(BROKER_TRUSTSTORE_FILE_PATH);
    gatewayConfiguration.setBrokerClientTlsTrustStorePassword(BROKER_TRUSTSTORE_PW);

    gatewayService =
        new GatewayService(
            gatewayConfiguration,
            new AuthenticationService(ConfigurationUtils.convertFrom(gatewayConfiguration)));
    gatewayService.start();

    gatewayService.getPulsarAdmin().clusters().getClusters();
    gatewayService.getPulsarClient().getPartitionsForTopic("test").get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testBrokerTlsConnectionWithKeyStoreFailed() throws Exception {
    gatewayService =
        new GatewayService(
            gatewayConfiguration,
            new AuthenticationService(ConfigurationUtils.convertFrom(gatewayConfiguration)));
    gatewayService.start();

    assertThrows(
        PulsarAdminException.class, () -> gatewayService.getPulsarAdmin().clusters().getClusters());
    try {
      gatewayService.getPulsarClient().getPartitionsForTopic("test").get(5, TimeUnit.SECONDS);
      fail("Should have timed out or thrown PulsarClientException");
    } catch (TimeoutException e) {
      // ignore
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof PulsarClientException);
    }
  }
}
