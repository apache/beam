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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Bare-bones class for using sharded files.
 *
 * <p>For internal use only; used only in SDK tests. Must be {@link Serializable} so it can be
 * shipped as a {@link org.apache.beam.sdk.testing.SerializableMatcher}.
 */
@Internal
public interface ShardedFile extends Serializable {

  /**
   * Reads the lines from all shards of this file using the provided {@link Sleeper} and {@link
   * BackOff}.
   */
  List<String> readFilesWithRetries(Sleeper sleeper, BackOff backOff)
      throws IOException, InterruptedException;
}
