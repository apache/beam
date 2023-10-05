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
package org.apache.beam.runners.dataflow.worker.graph;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.auto.value.AutoValue;

/** Container class for different types of graph edges. All edges only have reference equality. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class Edges {
  /** Base class for graph edges. All edges only have reference equality. */
  public abstract static class Edge implements Cloneable {
    @Override
    public final boolean equals(Object obj) {
      return this == obj;
    }

    @Override
    public final int hashCode() {
      return super.hashCode();
    }

    @Override
    public Edge clone() {
      try {
        return (Edge) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /** An edge with no parameters. */
  @AutoValue
  public abstract static class DefaultEdge extends Edge {
    public static DefaultEdge create() {
      return new AutoValue_Edges_DefaultEdge();
    }

    @Override
    public DefaultEdge clone() {
      return (DefaultEdge) super.clone();
    }
  }

  /** An edge that stores a {@link MultiOutputInfo}. */
  @AutoValue
  public abstract static class MultiOutputInfoEdge extends Edge {
    public static MultiOutputInfoEdge create(MultiOutputInfo multiOutputInfo) {
      checkNotNull(multiOutputInfo);
      return new AutoValue_Edges_MultiOutputInfoEdge(multiOutputInfo);
    }

    public abstract MultiOutputInfo getMultiOutputInfo();

    @Override
    public MultiOutputInfoEdge clone() {
      return (MultiOutputInfoEdge) super.clone();
    }
  }

  /**
   * An edge which represents an ordering relationship where the predecessor must occur before the
   * successor.
   */
  @AutoValue
  public abstract static class HappensBeforeEdge extends Edge {
    public static HappensBeforeEdge create() {
      return new AutoValue_Edges_HappensBeforeEdge();
    }

    @Override
    public HappensBeforeEdge clone() {
      return (HappensBeforeEdge) super.clone();
    }
  }

  // Hide visibility to prevent instantiation
  private Edges() {}
}
