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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A Read.Bounded place holder {@link Operation} during pipeline translation.
 */
class SourceOperation<T> extends Operation<T> {
  private final TaggedSource source;

  SourceOperation(BoundedSource<?> boundedSource, TupleTag<?> tupleTag) {
    super(1);
    checkNotNull(boundedSource, "boundedSource");
    checkNotNull(tupleTag, "tupleTag");
    this.source = TaggedSource.of(boundedSource, tupleTag);
  }

  @Override
  public void process(WindowedValue elem) {
    throw new IllegalStateException(
        String.format("%s should not in execution graph.", this.getClass().getSimpleName()));
  }

  TaggedSource getTaggedSource() {
    return source;
  }

  @AutoValue
  abstract static class TaggedSource implements Serializable {
    abstract BoundedSource<?> getSource();
    abstract TupleTag<?> getTag();

    static TaggedSource of(BoundedSource<?> boundedSource, TupleTag<?> tupleTag) {
      return new org.apache.beam.runners.mapreduce.translation
          .AutoValue_SourceOperation_TaggedSource(boundedSource, tupleTag);
    }
  }
}
