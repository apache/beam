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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.conf.Configuration;

/**
 * A Read.Bounded place holder {@link Operation} during pipeline translation.
 */
abstract class ReadOperation<T> extends Operation<T> {

  public ReadOperation() {
    super(1);
  }

  @Override
  public void process(WindowedValue elem) {
    throw new IllegalStateException(
        String.format("%s should not in execution graph.", this.getClass().getSimpleName()));
  }

  /**
   * Returns a TaggedSource during pipeline construction time.
   */
  abstract TaggedSource getTaggedSource(Configuration conf);

  @AutoValue
  abstract static class TaggedSource implements Serializable {
    abstract BoundedSource<?> getSource();
    abstract TupleTag<?> getTag();

    static TaggedSource of(BoundedSource<?> boundedSource, TupleTag<?> tupleTag) {
      return new org.apache.beam.runners.mapreduce.translation
          .AutoValue_ReadOperation_TaggedSource(boundedSource, tupleTag);
    }
  }
}
