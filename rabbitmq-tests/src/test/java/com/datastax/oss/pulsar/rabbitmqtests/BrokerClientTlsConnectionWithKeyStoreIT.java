package com.datastax.oss.pulsar.rabbitmqtests;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayConfiguration;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayService;
import com.datastax.oss.pulsar.rabbitmqgw.GatewayServiceStarter;
import com.datastax.oss.pulsar.rabbitmqtests.utils.PulsarCluster;
import java.io.IOException;
import java.nio.file.Path;
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

  @TempDir
  public static Path tempDir;
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
    gatewayConfiguration = new GatewayConfiguration();
    gatewayConfiguration.setBrokerServiceURL("pulsar+ssl://localhost:" + brokerServicePortTls);
    gatewayConfiguration.setBrokerWebServiceURL("https://localhost:" + webServicePortTls);

    gatewayConfiguration.setServicePort(Optional.of(PortManager.nextFreePort()));
    gatewayConfiguration.setZookeeperServers(cluster.getService().getConfig().getZookeeperServers());
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
            gatewayConfiguration, new AuthenticationService(GatewayServiceStarter.convertFrom(gatewayConfiguration)));
    gatewayService.start();

    gatewayService.getPulsarAdmin().clusters().getClusters();
    gatewayService.getPulsarClient().getPartitionsForTopic("test").get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testBrokerTlsConnectionWithKeyStoreFailed() throws Exception {
    gatewayService =
        new GatewayService(
            gatewayConfiguration, new AuthenticationService(GatewayServiceStarter.convertFrom(gatewayConfiguration)));
    gatewayService.start();

    assertThrows(PulsarAdminException.class, () -> gatewayService.getPulsarAdmin().clusters().getClusters());
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
