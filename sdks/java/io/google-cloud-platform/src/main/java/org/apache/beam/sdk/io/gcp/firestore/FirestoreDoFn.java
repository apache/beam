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

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Base class for all {@link DoFn} defined in the Firestore Connector.
 * <p/>
 * This class defines all of the lifecycle events as abstract methods, ensuring each is accounted
 * for in any implementing function.
 * @param <In> The type of the previous stage of the pipeline
 * @param <Out> The type output to the next stage of the pipeline
 */
abstract class FirestoreDoFn<In, Out> extends DoFn<In, Out> {

  @Override
  public abstract void populateDisplayData(@NonNull DisplayData.Builder builder);

  /**
   * @see org.apache.beam.sdk.transforms.DoFn.Setup
   */
  @Setup
  public abstract void setup() throws Exception;

  /**
   * @see org.apache.beam.sdk.transforms.DoFn.StartBundle
   */
  @StartBundle
  public abstract void startBundle(DoFn<In, Out>.StartBundleContext context) throws Exception;

  static abstract class NonWindowAwareDoFn<In, Out> extends FirestoreDoFn<In, Out> {
    /**
     * {@link ProcessContext#element() context.element()} must be non-null, otherwise a
     * NullPointerException will be thrown.
     *
     * @param context Context to source element from, and output to
     * @see org.apache.beam.sdk.transforms.DoFn.ProcessElement
     */
    @ProcessElement
    public abstract void processElement(DoFn<In, Out>.ProcessContext context) throws Exception;

    /**
     * @see org.apache.beam.sdk.transforms.DoFn.FinishBundle
     */
    @FinishBundle
    public abstract void finishBundle() throws Exception;
  }

  static abstract class WindowAwareDoFn<In, Out> extends FirestoreDoFn<In, Out> {
    /**
     * {@link ProcessContext#element() context.element()} must be non-null, otherwise a
     * NullPointerException will be thrown.
     *
     * @param context Context to source element from, and output to
     * @see org.apache.beam.sdk.transforms.DoFn.ProcessElement
     */
    @ProcessElement
    public abstract void processElement(DoFn<In, Out>.ProcessContext context, BoundedWindow window) throws Exception;

    /**
     * @see org.apache.beam.sdk.transforms.DoFn.FinishBundle
     */
    @FinishBundle
    public abstract void finishBundle(FinishBundleContext context) throws Exception;
  }
}
