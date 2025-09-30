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
package org.apache.beam.runners.spark.translation;

import java.io.Serializable;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.WindowedValue;

/**
 * Metadata class for side inputs in Spark runner. Contains serialized data, type information and
 * coder for side input processing.
 */
public class SideInputMetadata implements Serializable {
  private final byte[] data;
  private final SparkPCollectionView.Type type;
  private final Coder<Iterable<WindowedValue<?>>> coder;

  /**
   * Constructor for SideInputMetadata.
   *
   * @param data The serialized side input data as byte array
   * @param type The type of the SparkPCollectionView
   * @param coder The coder for iterables of windowed values
   */
  SideInputMetadata(
      byte[] data, SparkPCollectionView.Type type, Coder<Iterable<WindowedValue<?>>> coder) {
    this.data = data;
    this.type = type;
    this.coder = coder;
  }

  /**
   * Creates a new instance of SideInputMetadata.
   *
   * @param data The serialized side input data as byte array
   * @param type The type of the SparkPCollectionView
   * @param coder The coder for iterables of windowed values
   * @return A new SideInputMetadata instance
   */
  public static SideInputMetadata create(
      byte[] data, SparkPCollectionView.Type type, Coder<Iterable<WindowedValue<?>>> coder) {
    return new SideInputMetadata(data, type, coder);
  }

  /**
   * Converts this metadata to a {@link SideInputBroadcast} instance.
   *
   * @return A new {@link SideInputBroadcast} instance created from this metadata
   */
  @SuppressWarnings("rawtypes")
  public SideInputBroadcast toSideInputBroadcast() {
    return SideInputBroadcast.create(this.data, this.type, this.coder);
  }
}
