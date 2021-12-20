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
package org.apache.beam.sdk.io.gcp.pubsub;

import java.io.Serializable;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.beam.sdk.values.TypeDescriptors.extractFromTypeParameters;

public abstract class PubsubDynamicDestinations<T, DestinationT> implements Serializable {

  private transient @Nullable PipelineOptions options;

  /** Get the current PipelineOptions if set. */
  @Nullable
  PipelineOptions getPipelineOptions() {
    return options;
  }

  /**
   * Returns an object that represents at a high level which topic is being written to. May not
   * return null.
   */
  public abstract DestinationT getDestination(ValueInSingleWindow<T> element);

  /**
   * Returns a {@link TableDestination} object for the destination. May not return null. Return
   * value needs to be unique to each destination: may not return the same {@link TableDestination}
   * for different destinations.
   */
  public abstract PubsubIO.PubsubTopic getTopic(DestinationT destination);
}
