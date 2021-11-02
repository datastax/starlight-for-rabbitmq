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
package com.datastax.oss.starlight.rabbitmq.metadata;

import java.util.HashMap;
import java.util.Map;
import org.apache.qpid.server.model.LifetimePolicy;

public class ExchangeMetadata {

  public enum Type {
    direct,
    fanout,
    topic,
    headers
  }

  private Type type;
  private boolean durable;
  private LifetimePolicy lifetimePolicy;
  private Map<String, BindingSetMetadata> bindings = new HashMap<>();

  public ExchangeMetadata() {
    // For Jackson
  }

  public ExchangeMetadata(Type type, boolean durable, LifetimePolicy lifetimePolicy) {
    this.type = type;
    this.durable = durable;
    this.lifetimePolicy = lifetimePolicy;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public boolean isDurable() {
    return durable;
  }

  public void setDurable(boolean durable) {
    this.durable = durable;
  }

  public LifetimePolicy getLifetimePolicy() {
    return lifetimePolicy;
  }

  public void setLifetimePolicy(LifetimePolicy lifetimePolicy) {
    this.lifetimePolicy = lifetimePolicy;
  }

  public Map<String, BindingSetMetadata> getBindings() {
    return bindings;
  }

  public void setBindings(Map<String, BindingSetMetadata> bindings) {
    this.bindings = bindings;
  }
}
