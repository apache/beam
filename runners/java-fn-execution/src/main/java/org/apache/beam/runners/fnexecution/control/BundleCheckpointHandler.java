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
package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;

/**
 * A handler which is invoked when the SDK returns {@link BeamFnApi.DelayedBundleApplication}s as
 * part of the bundle completion.
 *
 * <p>These bundle applications must be resumed otherwise data loss will occur.
 *
 * <p>See <a href="https://s.apache.org/beam-breaking-fusion">breaking the fusion barrier</a> for
 * further details.
 */
public interface BundleCheckpointHandler {
  void onCheckpoint(BeamFnApi.ProcessBundleResponse response);
}
