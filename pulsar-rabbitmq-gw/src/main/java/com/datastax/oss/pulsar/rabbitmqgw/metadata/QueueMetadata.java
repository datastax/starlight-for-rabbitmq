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
package com.datastax.oss.pulsar.rabbitmqgw.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;

public class QueueMetadata {
  private boolean durable;
  private LifetimePolicy lifetimePolicy;
  private ExclusivityPolicy exclusivityPolicy;

  public QueueMetadata() {
    // for Jackson
  }

  public QueueMetadata(
      boolean durable, LifetimePolicy lifetimePolicy, ExclusivityPolicy exclusivityPolicy) {
    this.durable = durable;
    this.lifetimePolicy = lifetimePolicy;
    this.exclusivityPolicy = exclusivityPolicy;
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

  public ExclusivityPolicy getExclusivityPolicy() {
    return exclusivityPolicy;
  }

  public void setExclusivityPolicy(ExclusivityPolicy exclusivityPolicy) {
    this.exclusivityPolicy = exclusivityPolicy;
  }

  @JsonIgnore
  public int getQueueDepthMessages() {
    // TODO: implement message count in queue ?
    return 0;
  }

  @JsonIgnore
  public int getConsumerCount() {
    // TODO: implement distributed count of consumers
    return 0;
  }

  @JsonIgnore
  public boolean isUnused() {
    return getConsumerCount() == 0;
  }

  @JsonIgnore
  public boolean isEmpty() {
    return getQueueDepthMessages() == 0;
  }

  @JsonIgnore
  public boolean isExclusive() {
    return exclusivityPolicy != ExclusivityPolicy.NONE;
  }
}
