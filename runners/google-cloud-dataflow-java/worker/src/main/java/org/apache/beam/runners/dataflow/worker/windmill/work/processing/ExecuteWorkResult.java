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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;

@Internal
@Immutable
@AutoValue
public abstract class ExecuteWorkResult {
  public static ExecuteWorkResult create(
      List<Windmill.WorkItemCommitRequest> workItemCommits,
      List<Windmill.OutputMessageBundle> bundleOutputMessages,
      List<Windmill.PubSubMessageBundle> bundlePubsubMessages,
      Map<Long, Pair<Instant, Runnable>> finalizationCallbacks,
      long stateBytesRead) {
    return new AutoValue_ExecuteWorkResult(
        workItemCommits,
        bundleOutputMessages,
        bundlePubsubMessages,
        finalizationCallbacks,
        stateBytesRead);
  }

  public abstract List<Windmill.WorkItemCommitRequest> workItemCommits();

  public abstract List<Windmill.OutputMessageBundle> bundleOutputMessages();

  public abstract List<Windmill.PubSubMessageBundle> bundlePubsubMessages();

  // Map<finalizerId, Pair<callbackExpiration, callback>>
  public abstract Map<Long, Pair<Instant, Runnable>> finalizationCallbacks();

  public abstract long stateBytesRead();
}
