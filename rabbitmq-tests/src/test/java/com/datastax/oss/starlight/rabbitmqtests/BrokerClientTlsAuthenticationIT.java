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
import com.google.common.collect.Sets;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BrokerClientTlsAuthenticationIT {
  private static final String TLS_TRUST_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/cacert.pem";
  private static final String TLS_BROKER_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/server-cert.pem";
  private static final String TLS_BROKER_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/server-key.pem";
  private static final String TLS_CLIENT_CERT_FILE_PATH =
      "./src/test/resources/authentication/tls/client-cert.pem";
  private static final String TLS_CLIENT_KEY_FILE_PATH =
      "./src/test/resources/authentication/tls/client-key.pem";

  @TempDir public static Path tempDir;
  private static PulsarCluster cluster;
  private static GatewayService gatewayService;
  private GatewayConfiguration gatewayConfiguration;
  private static final int brokerServicePortTls = PortManager.nextFreePort();
  private static final int webServicePortTls = PortManager.nextFreePort();

  @BeforeAll
  public static void before() throws Exception {
    ServiceConfiguration pulsarConfig = new ServiceConfiguration();
    pulsarConfig.setAuthenticationEnabled(true);
    pulsarConfig.setAuthenticationProviders(
        Sets.newHashSet("org.apache.pulsar.broker.authentication.AuthenticationProviderTls"));
    pulsarConfig.setSuperUserRoles(Sets.newHashSet("superUser"));

    pulsarConfig.setTlsAllowInsecureConnection(false);
    pulsarConfig.setTlsCertificateFilePath(TLS_BROKER_CERT_FILE_PATH);
    pulsarConfig.setTlsKeyFilePath(TLS_BROKER_KEY_FILE_PATH);
    pulsarConfig.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);

    pulsarConfig.setBrokerServicePort(Optional.empty());
    pulsarConfig.setBrokerServicePortTls(Optional.of(brokerServicePortTls));

    pulsarConfig.setWebServicePort(Optional.empty());
    pulsarConfig.setWebServicePortTls(Optional.of(webServicePortTls));

    pulsarConfig.setBrokerClientTlsEnabled(true);
    pulsarConfig.setBrokerClientTrustCertsFilePath(TLS_BROKER_CERT_FILE_PATH);
    pulsarConfig.setBrokerClientAuthenticationPlugin(
        "org.apache.pulsar.client.impl.auth.AuthenticationTls");
    pulsarConfig.setBrokerClientAuthenticationParameters(
        "tlsCertFile:" + TLS_CLIENT_CERT_FILE_PATH + ",tlsKeyFile:" + TLS_CLIENT_KEY_FILE_PATH);

    cluster = new PulsarCluster(tempDir, pulsarConfig);
    cluster.start();
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

  @BeforeEach
  public void beforeEach() {
    gatewayConfiguration = new GatewayConfiguration();
    gatewayConfiguration.setBrokerServiceURL("pulsar+ssl://localhost:" + brokerServicePortTls);
    gatewayConfiguration.setBrokerWebServiceURL("https://localhost:" + webServicePortTls);
    gatewayConfiguration.setAmqpListeners(
        Collections.singleton("amqp://127.0.0.1:" + PortManager.nextFreePort()));
    gatewayConfiguration.setConfigurationStoreServers(
        cluster.getService().getConfig().getConfigurationStoreServers());
    gatewayConfiguration.setTlsEnabledWithBroker(true);
    gatewayConfiguration.setTlsHostnameVerificationEnabled(true);
    gatewayConfiguration.setBrokerClientTrustCertsFilePath(TLS_BROKER_CERT_FILE_PATH);
    gatewayConfiguration.setBrokerClientAuthenticationPlugin(
        "org.apache.pulsar.client.impl.auth.AuthenticationTls");
    gatewayConfiguration.setBrokerClientAuthenticationParameters(
        "tlsCertFile:" + TLS_CLIENT_CERT_FILE_PATH + ",tlsKeyFile:" + TLS_CLIENT_KEY_FILE_PATH);
  }

  @Test
  public void testBrokerAuthenticationTlsSuccessful() throws Exception {
    gatewayService =
        new GatewayService(
            gatewayConfiguration,
            new AuthenticationService(ConfigurationUtils.convertFrom(gatewayConfiguration)));
    gatewayService.start();

    gatewayService.getPulsarAdmin().clusters().getClusters();
    gatewayService.getPulsarClient().getPartitionsForTopic("test").get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testBrokerAuthenticationTlsInProxySuccessful() throws Exception {
    gatewayConfiguration.setBrokerClientAuthenticationParameters("");
    gatewayConfiguration.setAmqpBrokerClientAuthenticationParameters(
        "tlsCertFile:" + TLS_CLIENT_CERT_FILE_PATH + ",tlsKeyFile:" + TLS_CLIENT_KEY_FILE_PATH);
    gatewayService =
        new GatewayService(
            gatewayConfiguration,
            new AuthenticationService(ConfigurationUtils.convertFrom(gatewayConfiguration)));
    gatewayService.start();

    gatewayService.getPulsarAdmin().clusters().getClusters();
    gatewayService.getPulsarClient().getPartitionsForTopic("test").get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testBrokerTlsConnexionFails() throws Exception {
    gatewayConfiguration.setBrokerClientTrustCertsFilePath(
        "./src/test/resources/authentication/tls/other-cacert.pem");
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

  @Test
  public void testBrokerTlsAuthenticationFails() throws Exception {
    gatewayConfiguration.setBrokerClientAuthenticationPlugin("");
    gatewayService =
        new GatewayService(
            gatewayConfiguration,
            new AuthenticationService(ConfigurationUtils.convertFrom(gatewayConfiguration)));
    gatewayService.start();

    assertThrows(
        PulsarAdminException.NotAuthorizedException.class,
        () -> gatewayService.getPulsarAdmin().clusters().getClusters());
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
