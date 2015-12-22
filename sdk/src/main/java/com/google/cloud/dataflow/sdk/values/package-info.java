/*
 * Copyright (C) 2015 Google Inc.
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
 * <p>In particular, see these collection abstractions:
 *
 * <ul>
 *   <li>{@link com.google.cloud.dataflow.sdk.values.PCollection} - an immutable collection of
 *     values of type {@code T} and the main representation for data in Dataflow.</li>
 *   <li>{@link com.google.cloud.dataflow.sdk.values.PCollectionView} - an immutable view of a
 *     {@link com.google.cloud.dataflow.sdk.values.PCollection} that can be accessed as a
 *     side input of a {@link com.google.cloud.dataflow.sdk.transforms.ParDo}
 *     {@link com.google.cloud.dataflow.sdk.transforms.PTransform}.</li>
 *   <li>{@link com.google.cloud.dataflow.sdk.values.PCollectionTuple} - a heterogeneous tuple of
 *     {@link com.google.cloud.dataflow.sdk.values.PCollection PCollections}
 *     used in cases where a {@link com.google.cloud.dataflow.sdk.transforms.PTransform} takes
 *     or returns multiple
 *     {@link com.google.cloud.dataflow.sdk.values.PCollection PCollections}.</li>
 *   <li>{@link com.google.cloud.dataflow.sdk.values.PCollectionList} - a homogeneous list of
 *     {@link com.google.cloud.dataflow.sdk.values.PCollection PCollections} used, for example,
 *     as input to {@link com.google.cloud.dataflow.sdk.transforms.Flatten}.</li>
 * </ul>
 *
 * <p>And these classes for individual values play particular roles in Dataflow:
 *
 * <ul>
 *   <li>{@link com.google.cloud.dataflow.sdk.values.KV} - a key/value pair that is used by
 *     keyed transforms, most notably {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey}.
 *     </li>
 *   <li>{@link com.google.cloud.dataflow.sdk.values.TimestampedValue} - a timestamp/value pair
 *     that is used for windowing and handling out-of-order data in streaming execution.</li>
 * </ul>
 *
 * <p>For further details, see the documentation for each class in this package.
 */
package com.google.cloud.dataflow.sdk.values;
