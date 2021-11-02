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

import com.datastax.oss.starlight.rabbitmq.metadata.VirtualHostMetadata;
import java.util.concurrent.CompletableFuture;
import org.apache.qpid.server.model.LifetimePolicy;

public class TopicExchange extends AbstractExchange {

  public TopicExchange(String name, boolean durable, LifetimePolicy lifetimePolicy) {
    super(name, Type.topic, durable, lifetimePolicy);
  }

  @Override
  public CompletableFuture<Void> bind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection connection) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    result.completeExceptionally(
        new UnsupportedOperationException("Binding topic exchange not supported at the moment"));
    return result;
  }

  @Override
  public CompletableFuture<Void> unbind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection gatewayConnection) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    result.completeExceptionally(
        new UnsupportedOperationException("Binding header exchange not supported at the moment"));
    return result;
  }
}
