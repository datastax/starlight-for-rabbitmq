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
package com.datastax.oss.pulsar.rabbitmqgw;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;

public class GatewayProtocolHandler implements ProtocolHandler {
  public static final String PROTOCOL_NAME = "rabbitmq";

  private GatewayConfiguration config;
  private GatewayService service;
  private String bindAddress;

  @Override
  public String protocolName() {
    return PROTOCOL_NAME;
  }

  @Override
  public boolean accept(String protocol) {
    return PROTOCOL_NAME.equals(protocol);
  }

  @SneakyThrows
  @Override
  public void initialize(ServiceConfiguration conf) {
    config = ConfigurationUtils.create(conf.getProperties(), GatewayConfiguration.class);
    bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(config.getBindAddress());
  }

  @Override
  public String getProtocolDataToAdvertise() {
    Set<String> addresses = new HashSet<>();
    config
        .getAmqpServicePort()
        .map(port -> addresses.add(String.format("amqp://%s:%d", bindAddress, port)));
    config
        .getAmqpServicePortTls()
        .map(port -> addresses.add(String.format("amqps://%s:%d", bindAddress, port)));
    return String.join(", ", addresses);
  }

  @SneakyThrows
  @Override
  public void start(BrokerService brokerService) {
    bindAddress = brokerService.getPulsar().getBindAddress();
    service =
        new GatewayService(
            config,
            brokerService.getAuthenticationService(),
            brokerService.getPulsar().getSafeBrokerServiceUrl(),
            brokerService.getPulsar().getSafeWebServiceAddress());
    service.start(false);
  }

  @Override
  public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
    return service.newChannelInitializers();
  }

  @SneakyThrows
  @Override
  public void close() {
    if (service != null) {
      service.close();
    }
  }
}
