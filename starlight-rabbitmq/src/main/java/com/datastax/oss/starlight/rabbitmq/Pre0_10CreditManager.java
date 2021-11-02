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

import java.util.concurrent.atomic.LongAdder;
import org.apache.qpid.server.protocol.v0_8.FlowCreditManager_0_8;

/**
 * Copy of package-private org.apache.qpid.server.protocol.v0_8.Pre0_10CreditManager with fixes for
 * concurrency
 */
public class Pre0_10CreditManager implements FlowCreditManager_0_8 {

  private final long _highPrefetchLimit;
  private final long _batchLimit;
  private volatile long _bytesCreditLimit;
  private volatile long _messageCreditLimit;

  private final LongAdder _bytesCredit = new LongAdder();
  private final LongAdder _messageCredit = new LongAdder();

  Pre0_10CreditManager(
      long bytesCreditLimit, long messageCreditLimit, long highPrefetchLimit, long batchLimit) {
    _bytesCreditLimit = bytesCreditLimit;
    _messageCreditLimit = messageCreditLimit;
    _bytesCredit.add(bytesCreditLimit);
    _messageCredit.add(messageCreditLimit);
    _highPrefetchLimit = highPrefetchLimit;
    _batchLimit = batchLimit;
  }

  void setCreditLimits(final long bytesCreditLimit, final long messageCreditLimit) {
    long bytesCreditChange = bytesCreditLimit - _bytesCreditLimit;
    long messageCreditChange = messageCreditLimit - _messageCreditLimit;

    if (bytesCreditChange != 0L) {
      _bytesCredit.add(bytesCreditChange);
    }

    if (messageCreditChange != 0L) {
      _messageCredit.add(messageCreditChange);
    }

    _bytesCreditLimit = bytesCreditLimit;
    _messageCreditLimit = messageCreditLimit;
  }

  @Override
  public void restoreCredit(final long messageCredit, final long bytesCredit) {
    _messageCredit.add(messageCredit);
    long messageCreditSum = _messageCredit.longValue();
    if (_messageCreditLimit != 0 && messageCreditSum > _messageCreditLimit) {
      throw new IllegalStateException(
          String.format(
              "Consumer credit accounting error. Restored more credit than we ever had: messageCredit=%d  messageCreditLimit=%d",
              messageCreditSum, _messageCreditLimit));
    }

    _bytesCredit.add(bytesCredit);
    long _bytesCreditSum = _bytesCredit.longValue();
    if (_bytesCreditLimit != 0 && _bytesCreditSum > _bytesCreditLimit) {
      throw new IllegalStateException(
          String.format(
              "Consumer credit accounting error. Restored more credit than we ever had: bytesCredit=%d  bytesCreditLimit=%d",
              _bytesCreditSum, _bytesCreditLimit));
    }
  }

  @Override
  public boolean hasCredit() {
    return (_bytesCreditLimit == 0L || _bytesCredit.longValue() > 0)
        && (_messageCreditLimit == 0L || _messageCredit.longValue() > 0);
  }

  @Override
  public boolean useCreditForMessage(final long msgSize) {
    if (_messageCreditLimit != 0) {
      if (_messageCredit.longValue() <= 0) {
        return false;
      }
    }
    if (_bytesCreditLimit != 0) {
      long _bytesCreditSum = _bytesCredit.longValue();
      if ((_bytesCreditSum < msgSize) && (_bytesCreditSum != _bytesCreditLimit)) {
        return false;
      }
    }

    _messageCredit.decrement();
    _bytesCredit.add(-msgSize);
    return true;
  }

  @Override
  public boolean isNotBytesLimitedAndHighPrefetch() {
    return _bytesCreditLimit == 0L && _messageCreditLimit > _highPrefetchLimit;
  }

  @Override
  public boolean isBytesLimited() {
    return _bytesCredit.longValue() != 0;
  }

  @Override
  public boolean isCreditOverBatchLimit() {
    return _messageCredit.longValue() > _batchLimit;
  }
}
