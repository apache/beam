/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import java.util.BitSet;

/**
 * A {@link FinishedTriggers} implementation based on an underlying {@link BitSet}.
 */
public class FinishedTriggersBitSet implements FinishedTriggers {

  private final BitSet bitSet;

  private FinishedTriggersBitSet(BitSet bitSet) {
    this.bitSet = bitSet;
  }

  public static FinishedTriggersBitSet emptyWithCapacity(int capacity) {
    return new FinishedTriggersBitSet(new BitSet(capacity));
  }

  public static FinishedTriggersBitSet fromBitSet(BitSet bitSet) {
    return new FinishedTriggersBitSet(bitSet);
  }

  /**
   * Returns the underlying {@link BitSet} for this {@link FinishedTriggersBitSet}.
   */
  public BitSet getBitSet() {
    return bitSet;
  }

  @Override
  public boolean isFinished(ExecutableTrigger trigger) {
    return bitSet.get(trigger.getTriggerIndex());
  }

  @Override
  public void setFinished(ExecutableTrigger trigger, boolean value) {
    bitSet.set(trigger.getTriggerIndex(), value);
  }

  @Override
  public void clearRecursively(ExecutableTrigger trigger) {
    bitSet.clear(trigger.getTriggerIndex(), trigger.getFirstIndexAfterSubtree());
  }

  @Override
  public FinishedTriggersBitSet copy() {
    return new FinishedTriggersBitSet((BitSet) bitSet.clone());
  }
}
