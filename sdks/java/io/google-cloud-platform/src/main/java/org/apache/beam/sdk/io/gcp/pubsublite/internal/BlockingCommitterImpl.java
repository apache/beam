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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.wire.Committer;
import java.util.concurrent.TimeUnit;

public class BlockingCommitterImpl implements BlockingCommitter {

  private final Committer committer;

  BlockingCommitterImpl(Committer committer) {
    if (!committer.isRunning()) {
      throw new IllegalStateException(
          "Committer passed to BlockingCommitter which is not running.", committer.failureCause());
    }
    this.committer = committer;
  }

  @Override
  public void commitOffset(Offset offset) {
    if (!committer.isRunning()) {
      throw new IllegalStateException(
          "Committer not running when commitOffset called.", committer.failureCause());
    }
    try {
      committer.commitOffset(offset).get(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw toCanonical(e).underlying;
    }
  }

  @Override
  public void close() throws Exception {
    committer.stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
  }
}
