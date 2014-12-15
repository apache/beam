/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Defines {@link com.google.cloud.dataflow.sdk.values.PCollection} and other classes for
 * representing data in a {@link com.google.cloud.dataflow.sdk.Pipeline}.
 *
 * <p> A {@link com.google.cloud.dataflow.sdk.values.PCollection} is an immutable collection of
 * values of type {@code T} and is the main representation for data.
 * A {@link com.google.cloud.dataflow.sdk.values.PCollectionTuple} is a tuple of PCollections
 * used in cases where PTransforms take or return multiple PCollections.
 *
 * <p> A {@link com.google.cloud.dataflow.sdk.values.PCollectionTuple} is an immutable tuple of
 * heterogeneously-typed {@link com.google.cloud.dataflow.sdk.values.PCollection}s, "keyed" by
 * {@link com.google.cloud.dataflow.sdk.values.TupleTag}s.
 * A PCollectionTuple can be used as the input or
 * output of a
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform} taking
 * or producing multiple PCollection inputs or outputs that can be of
 * different types, for instance a
 * {@link com.google.cloud.dataflow.sdk.transforms.ParDo} with side
 * outputs.
 *
 * <p> A {@link com.google.cloud.dataflow.sdk.values.PCollectionView} is an immutable view of a
 * PCollection that can be accessed from a DoFn and other user Fns
 * as a side input.
 *
 */
package com.google.cloud.dataflow.sdk.values;
