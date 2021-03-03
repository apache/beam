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
package org.apache.beam.sdk.extensions.sql.impl.udaf;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.udaf.StringAgg.Aggregation;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * {@link CombineFn} for aggregating strings or bytes with an optional delimiter.
 *
 * The return type is either byte[] or String depending on the type of the first input argument.
 */
@Experimental
public class StringAgg extends CombineFn<Row, Aggregation, Object> {
  enum AggType {
    BYTES,
    STRING
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  static abstract class Aggregation {
    abstract List<byte[]> getBytestrings();
    @SuppressWarnings("mutable")
    abstract @Nullable byte[] getSeparator();
    abstract @Nullable AggType getAggType();

    static Aggregation of() {
      return new AutoValue_StringAgg_Aggregation(ImmutableList.of(), null, null);
    }

    static Aggregation of(List<byte[]> bytestrings, byte[] separator, AggType aggType) {
      return new AutoValue_StringAgg_Aggregation(bytestrings, checkArgumentNotNull(separator), checkArgumentNotNull(aggType));
    }

    boolean isEmpty() {
      return getSeparator() == null;
    }
  }

  @Override
  public Aggregation createAccumulator() {
    return Aggregation.of();
  }

  private byte[] getSeparator(Row next) {
    if (next.getFieldCount() < 2) {
      return ",".getBytes(UTF_8);
    }
    Object object = next.getValue(1);
    if (object instanceof byte[]) {
      return (byte[]) object;
    }
    return checkArgumentNotNull((String) object).getBytes(UTF_8);
  }

  private Aggregation getAggregation(Row next) {
    byte[] separator = getSeparator(next);
    Object object = next.getValue(0);
    if (object instanceof byte[]) {
      return Aggregation.of(ImmutableList.of((byte[]) object), separator, AggType.BYTES);
    }
    String string = (String) object;
    return Aggregation.of(ImmutableList.of(checkArgumentNotNull(string).getBytes(UTF_8)), separator, AggType.STRING);
  }

  private void checkCompatible(Aggregation agg1, Aggregation agg2) {
    checkArgument(!agg1.isEmpty());
    checkArgument(Arrays.equals(agg1.getSeparator(), agg2.getSeparator()), "STRING_AGG requires a separator that does not change based on the input. %s does not match %s.", agg1.getSeparator(), agg2.getSeparator());
    checkArgument(checkArgumentNotNull(agg1.getAggType()).equals(agg2.getAggType()), "STRING_AGG requires every item to be either STRING or BYTES, not a mix.");
  }

  @Override
  public Aggregation addInput(Aggregation current, @Nullable Row next) {
    if (next == null) {
      return current;
    }
    return mergeAccumulators(ImmutableList.of(current, getAggregation(next)));
  }

  @Override
  public Aggregation mergeAccumulators(Iterable<Aggregation> accumulators) {
    List<Aggregation> nonEmpty = StreamSupport.stream(accumulators.spliterator(), false)
        .filter(aggregation -> !aggregation.isEmpty()).collect(Collectors.toList());
    if (nonEmpty.isEmpty()) {
      return Aggregation.of();
    }
    Aggregation first = nonEmpty.get(0);
    List<byte[]> accumulated = nonEmpty.stream().flatMap(aggregation -> {
      checkCompatible(first, aggregation);
      return aggregation.getBytestrings().stream();
    }).collect(Collectors.toList());
    return Aggregation.of(accumulated, checkArgumentNotNull(first.getSeparator()), checkArgumentNotNull(first.getAggType()));
  }

  private byte[] join(List<byte[]> entries, byte[] sep) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      for (int i = 0; i < entries.size(); i++) {
        os.write(entries.get(i));
        if (i != entries.size() - 1) {
          os.write(sep);
        }
      }
      return os.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("override.return.invalid")
  public @Nullable Object extractOutput(Aggregation aggregation) {
    if (aggregation == null || aggregation.isEmpty()) {
      return null;
    }
    byte[] joined = join(aggregation.getBytestrings(), checkArgumentNotNull(aggregation.getSeparator()));
    if (aggregation.getAggType() == AggType.BYTES) {
      return joined;
    }
    return new String(joined, UTF_8);
  }
}
