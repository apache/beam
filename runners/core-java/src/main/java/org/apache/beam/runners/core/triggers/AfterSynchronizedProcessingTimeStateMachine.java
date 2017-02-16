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
package org.apache.beam.runners.core.triggers;

import com.google.common.base.Objects;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.TimeDomain;
import org.joda.time.Instant;

// This should not really have the superclass https://issues.apache.org/jira/browse/BEAM-1486
class AfterSynchronizedProcessingTimeStateMachine extends AfterDelayFromFirstElementStateMachine {

  public static AfterSynchronizedProcessingTimeStateMachine ofFirstElement() {
    return new AfterSynchronizedProcessingTimeStateMachine();
  }

  @Override
  @Nullable
  public Instant getCurrentTime(TriggerStateMachine.TriggerContext context) {
    return context.currentSynchronizedProcessingTime();
  }

  private AfterSynchronizedProcessingTimeStateMachine() {
    super(TimeDomain.SYNCHRONIZED_PROCESSING_TIME,
        Collections.<SerializableFunction<Instant, Instant>>emptyList());
  }

  @Override
  public String toString() {
    return "AfterSynchronizedProcessingTime.pastFirstElementInPane()";
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || obj instanceof AfterSynchronizedProcessingTimeStateMachine;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(AfterSynchronizedProcessingTimeStateMachine.class);
  }

  @Override
  protected AfterSynchronizedProcessingTimeStateMachine
      newWith(List<SerializableFunction<Instant, Instant>> transforms) {
    // ignore transforms
    return this;
  }

}
