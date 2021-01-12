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
package org.apache.beam.runners.spark.io;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.UnboundedSource;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Passing null values to Spark's Java API may cause problems because of Guava preconditions. See:
 * {@link org.apache.spark.api.java.JavaUtils#optionToOptional}
 */
public class EmptyCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
  private static final EmptyCheckpointMark INSTANCE = new EmptyCheckpointMark();
  private static final int ID = 2654265; // some constant to serve as identifier.

  private EmptyCheckpointMark() {}

  public static EmptyCheckpointMark get() {
    return INSTANCE;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {}

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj instanceof EmptyCheckpointMark;
  }

  @Override
  public int hashCode() {
    return ID;
  }
}
