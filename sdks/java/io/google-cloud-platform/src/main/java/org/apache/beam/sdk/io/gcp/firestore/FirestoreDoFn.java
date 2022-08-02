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
package org.apache.beam.sdk.io.gcp.firestore;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Base class for all stateful {@link DoFn} defined in the Firestore Connector.
 *
 * <p>This class defines all of the lifecycle events as abstract methods, ensuring each is accounted
 * for in any implementing function.
 *
 * <p>This base class also serves as an upper bound for the unit tests where all DoFn are checked to
 * ensure they are serializable and adhere to specific lifecycle events.
 *
 * @param <InT> The type of the previous stage of the pipeline
 * @param <OutT> The type output to the next stage of the pipeline
 */
abstract class FirestoreDoFn<InT, OutT> extends DoFn<InT, OutT> {

  @Override
  public abstract void populateDisplayData(DisplayData.Builder builder);

  /** @see org.apache.beam.sdk.transforms.DoFn.Setup */
  @Setup
  public abstract void setup() throws Exception;

  /** @see org.apache.beam.sdk.transforms.DoFn.StartBundle */
  @StartBundle
  public abstract void startBundle(DoFn<InT, OutT>.StartBundleContext context) throws Exception;

  /**
   * This class defines a common parent class for those DoFn which rely on the implicit window for
   * emitting values while processing a bundle.
   */
  abstract static class ImplicitlyWindowedFirestoreDoFn<InT, OutT>
      extends FirestoreDoFn<InT, OutT> {
    /**
     * {@link ProcessContext#element() context.element()} must be non-null, otherwise a
     * NullPointerException will be thrown.
     *
     * @param context Context to source element from, and output to
     * @see org.apache.beam.sdk.transforms.DoFn.ProcessElement
     */
    @ProcessElement
    public abstract void processElement(DoFn<InT, OutT>.ProcessContext context) throws Exception;

    /** @see org.apache.beam.sdk.transforms.DoFn.FinishBundle */
    @FinishBundle
    public abstract void finishBundle() throws Exception;
  }

  /**
   * This class defines a common parent class for those DoFn which must explicitly track the window
   * for emitting values while processing bundles. This is primarily necessary to support the
   * ability to emit values during {@link #finishBundle(DoFn.FinishBundleContext)} where an output
   * value must be explicitly correlated to a window.
   */
  abstract static class ExplicitlyWindowedFirestoreDoFn<InT, OutT>
      extends FirestoreDoFn<InT, OutT> {
    /**
     * {@link ProcessContext#element() context.element()} must be non-null, otherwise a
     * NullPointerException will be thrown.
     *
     * @param context Context to source element from, and output to
     * @see org.apache.beam.sdk.transforms.DoFn.ProcessElement
     */
    @ProcessElement
    public abstract void processElement(
        DoFn<InT, OutT>.ProcessContext context, BoundedWindow window) throws Exception;

    /** @see org.apache.beam.sdk.transforms.DoFn.FinishBundle */
    @FinishBundle
    public abstract void finishBundle(FinishBundleContext context) throws Exception;
  }
}
