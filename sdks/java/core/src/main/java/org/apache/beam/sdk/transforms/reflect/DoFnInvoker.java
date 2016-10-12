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
package org.apache.beam.sdk.transforms.reflect;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

/**
 * Interface for invoking the {@code DoFn} processing methods.
 *
 * <p>Instantiating a {@link DoFnInvoker} associates it with a specific {@link DoFn} instance,
 * referred to as the bound {@link DoFn}.
 */
public interface DoFnInvoker<InputT, OutputT> {
  /** Invoke the {@link DoFn.Setup} method on the bound {@link DoFn}. */
  void invokeSetup();

  /** Invoke the {@link DoFn.StartBundle} method on the bound {@link DoFn}. */
  void invokeStartBundle(DoFn<InputT, OutputT>.Context c);

  /** Invoke the {@link DoFn.FinishBundle} method on the bound {@link DoFn}. */
  void invokeFinishBundle(DoFn<InputT, OutputT>.Context c);

  /** Invoke the {@link DoFn.Teardown} method on the bound {@link DoFn}. */
  void invokeTeardown();

  /**
   * Invoke the {@link DoFn.ProcessElement} method on the bound {@link DoFn}.
   *
   * @param c The {@link DoFn.ProcessContext} to invoke the fn with.
   * @param extra Factory for producing extra parameter objects (such as window), if necessary.
   * @return The {@link DoFn.ProcessContinuation} returned by the underlying method, or {@link
   *     DoFn.ProcessContinuation#stop()} if it returns {@code void}.
   */
  DoFn.ProcessContinuation invokeProcessElement(
      DoFn<InputT, OutputT>.ProcessContext c, DoFn.ExtraContextFactory<InputT, OutputT> extra);

  /** Invoke the {@link DoFn.GetInitialRestriction} method on the bound {@link DoFn}. */
  <RestrictionT> RestrictionT invokeGetInitialRestriction(InputT element);

  /**
   * Invoke the {@link DoFn.GetRestrictionCoder} method on the bound {@link DoFn}. Called only
   * during pipeline construction time.
   */
  <RestrictionT> Coder<RestrictionT> invokeGetRestrictionCoder(CoderRegistry coderRegistry);

  /** Invoke the {@link DoFn.SplitRestriction} method on the bound {@link DoFn}. */
  <RestrictionT> void invokeSplitRestriction(
      InputT element,
      RestrictionT restriction,
      DoFn.OutputReceiver<RestrictionT> restrictionReceiver);

  /** Invoke the {@link DoFn.NewTracker} method on the bound {@link DoFn}. */
  <RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>> TrackerT invokeNewTracker(
      RestrictionT restriction);
}
