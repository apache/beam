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
package org.apache.beam.sdk.transforms.windowing;

/** A TriggerVisitor. */
public interface TriggerVisitor<OutputT> {
  OutputT visit(DefaultTrigger trigger);

  OutputT visit(AfterWatermark.FromEndOfWindow trigger);

  OutputT visit(AfterWatermark.AfterWatermarkEarlyAndLate trigger);

  OutputT visit(Never.NeverTrigger trigger);

  OutputT visit(ReshuffleTrigger<?> trigger);

  OutputT visit(AfterProcessingTime trigger);

  OutputT visit(AfterSynchronizedProcessingTime trigger);

  OutputT visit(AfterFirst trigger);

  OutputT visit(AfterAll trigger);

  OutputT visit(AfterEach trigger);

  OutputT visit(AfterPane trigger);

  OutputT visit(Repeatedly trigger);

  OutputT visit(OrFinallyTrigger trigger);
}
