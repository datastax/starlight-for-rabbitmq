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

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.prometheus.client.exporter.MetricsServlet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.proxy.server.WebServer;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts an instance of Starlight for RabbitMQ */
public class GatewayServiceStarter {

  @Parameter(
    names = {"-c", "--config"},
    description = "Configuration file path",
    required = true
  )
  private String configFile = null;

  @Parameter(
    names = {"-h", "--help"},
    description = "Show this help message"
  )
  private boolean help = false;

  public GatewayServiceStarter(String[] args) throws Exception {
    try {

      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");

      Thread.setDefaultUncaughtExceptionHandler(
          (thread, exception) ->
              System.out.printf(
                  "%s [%s] error Uncaught exception in thread %s: %s%n",
                  dateFormat.format(new Date()),
                  thread.getContextClassLoader(),
                  thread.getName(),
                  exception.getMessage()));

      JCommander jcommander = new JCommander();
      try {
        jcommander.addObject(this);
        jcommander.parse(args);
        if (help || isBlank(configFile)) {
          jcommander.usage();
          return;
        }
      } catch (Exception e) {
        jcommander.usage();
        System.exit(-1);
      }

      // load config file
      final GatewayConfiguration config =
          ConfigurationUtils.create(configFile, GatewayConfiguration.class);

      AuthenticationService authenticationService =
          new AuthenticationService(ConfigurationUtils.convertFrom(config));

      // create gateway service
      GatewayService gatewayService = new GatewayService(config, authenticationService);
      WebServer webServer = new WebServer(config, authenticationService);

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      gatewayService.close();
                    } catch (Exception e) {
                      log.warn("gRPC server couldn't stop gracefully {}", e.getMessage(), e);
                    }
                    try {
                      webServer.stop();
                    } catch (Exception e) {
                      log.warn("Web server couldn't stop gracefully {}", e.getMessage(), e);
                    }
                  }));

      gatewayService.start(true);

      webServer.addServlet(
          "/metrics",
          new ServletHolder(MetricsServlet.class),
          Collections.emptyList(),
          config.isAuthenticateMetricsEndpoint());

      webServer.start();

    } catch (Exception e) {
      log.error("Failed to start Starlight for RabbitMQ. error msg " + e.getMessage(), e);
      throw new PulsarServerException(e);
    }
  }

  public static void main(String[] args) throws Exception {
    new GatewayServiceStarter(args);
  }

  private static final Logger log = LoggerFactory.getLogger(GatewayServiceStarter.class);
}
