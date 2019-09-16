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
package org.apache.beam.runners.dataflow.worker.counters;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.DistributionUpdate;
import com.google.api.services.dataflow.model.FloatingPointMean;
import com.google.api.services.dataflow.model.Histogram;
import com.google.api.services.dataflow.model.IntegerMean;
import com.google.api.services.dataflow.model.NameAndKind;
import com.google.api.services.dataflow.model.SplitInt64;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterMean;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Factory methods for extracting {@link CounterUpdate} updates from counters. */
public class DataflowCounterUpdateExtractor implements CounterUpdateExtractor<CounterUpdate> {

  public static final DataflowCounterUpdateExtractor INSTANCE =
      new DataflowCounterUpdateExtractor();

  /** Should not be instantiated */
  private DataflowCounterUpdateExtractor() {}

  private CounterUpdate initUpdate(CounterName name, boolean delta, String kind) {
    CounterUpdate counterUpdate = new CounterUpdate();
    if (name.isStructured()) {
      counterUpdate.setStructuredNameAndMetadata(getStructuredName(name, kind));
    } else {
      counterUpdate.setNameAndKind(getUnstructuredName(name, kind));
    }
    counterUpdate.setCumulative(!delta);
    return counterUpdate;
  }

  private CounterStructuredNameAndMetadata getStructuredName(CounterName name, String kind) {
    CounterMetadata metadata = new CounterMetadata();
    metadata.setKind(kind);

    CounterStructuredName structuredName = new CounterStructuredName();
    structuredName.setName(name.name());
    if (name.usesContextOriginalName()) {
      structuredName.setOriginalStepName(name.contextOriginalName());
    } else if (name.usesContextSystemName()) {
      structuredName.setComponentStepName(name.contextSystemName());
    }
    if (name.originalRequestingStepName() != null) {
      structuredName.setOriginalRequestingStepName(name.originalRequestingStepName());
    }
    if (name.inputIndex() != null && name.inputIndex() > 0) {
      structuredName.setInputIndex(name.inputIndex());
    }
    CounterStructuredNameAndMetadata nameAndMetadata = new CounterStructuredNameAndMetadata();
    nameAndMetadata.setMetadata(metadata);
    nameAndMetadata.setName(structuredName);
    return nameAndMetadata;
  }

  private NameAndKind getUnstructuredName(CounterName name, String kind) {
    NameAndKind nameAndKind = new NameAndKind();
    nameAndKind.setName(name.name());
    nameAndKind.setKind(kind);
    return nameAndKind;
  }

  private CounterUpdate longUpdate(CounterName name, boolean delta, String kind, long value) {
    return initUpdate(name, delta, kind).setInteger(longToSplitInt(value));
  }

  private CounterUpdate doubleUpdate(CounterName name, boolean delta, String kind, Double value) {
    return initUpdate(name, delta, kind).setFloatingPoint(value);
  }

  private CounterUpdate boolUpdate(CounterName name, boolean delta, String kind, Boolean value) {
    return initUpdate(name, delta, kind).setBoolean(value);
  }

  @Override
  public CounterUpdate longSum(CounterName name, boolean delta, Long value) {
    return longUpdate(name, delta, "SUM", value);
  }

  @Override
  public CounterUpdate longMin(CounterName name, boolean delta, Long value) {
    return longUpdate(name, delta, "MIN", value);
  }

  @Override
  public CounterUpdate longMax(CounterName name, boolean delta, Long value) {
    return longUpdate(name, delta, "MAX", value);
  }

  @Override
  public CounterUpdate longMean(CounterName name, boolean delta, CounterMean<Long> value) {
    if (value.getCount() <= 0) {
      return null;
    }

    return initUpdate(name, delta, "MEAN")
        .setIntegerMean(
            new IntegerMean()
                .setSum(longToSplitInt(value.getAggregate()))
                .setCount(longToSplitInt(value.getCount())));
  }

  @Override
  public CounterUpdate intSum(CounterName name, boolean delta, Integer value) {
    return longUpdate(name, delta, "SUM", value);
  }

  @Override
  public CounterUpdate intMin(CounterName name, boolean delta, Integer value) {
    return longUpdate(name, delta, "MIN", value);
  }

  @Override
  public CounterUpdate intMax(CounterName name, boolean delta, Integer value) {
    return longUpdate(name, delta, "MAX", value);
  }

  @Override
  public CounterUpdate intMean(CounterName name, boolean delta, CounterMean<Integer> value) {
    if (value.getCount() <= 0) {
      return null;
    }

    return initUpdate(name, delta, "MEAN")
        .setIntegerMean(
            new IntegerMean()
                .setSum(longToSplitInt(value.getAggregate()))
                .setCount(longToSplitInt(value.getCount())));
  }

  @Override
  public CounterUpdate doubleSum(CounterName name, boolean delta, Double value) {
    return doubleUpdate(name, delta, "SUM", value);
  }

  @Override
  public CounterUpdate doubleMin(CounterName name, boolean delta, Double value) {
    return doubleUpdate(name, delta, "MIN", value);
  }

  @Override
  public CounterUpdate doubleMax(CounterName name, boolean delta, Double value) {
    return doubleUpdate(name, delta, "MAX", value);
  }

  @Override
  public CounterUpdate doubleMean(CounterName name, boolean delta, CounterMean<Double> value) {
    if (value.getCount() <= 0) {
      return null;
    }

    return initUpdate(name, delta, "MEAN")
        .setFloatingPointMean(
            new FloatingPointMean()
                .setSum(value.getAggregate())
                .setCount(longToSplitInt(value.getCount())));
  }

  @Override
  public CounterUpdate boolOr(CounterName name, boolean delta, Boolean value) {
    return boolUpdate(name, delta, "OR", value);
  }

  @Override
  public CounterUpdate boolAnd(CounterName name, boolean delta, Boolean value) {
    return boolUpdate(name, delta, "AND", value);
  }

  @Override
  public CounterUpdate distribution(
      CounterName name, boolean delta, CounterFactory.CounterDistribution value) {

    DistributionUpdate distributionUpdate =
        new DistributionUpdate()
            .setMin(longToSplitInt(value.getMin()))
            .setMax(longToSplitInt(value.getMax()))
            .setCount(longToSplitInt(value.getCount()))
            .setSum(longToSplitInt(value.getSum()))
            .setSumOfSquares(value.getSumOfSquares())
            .setHistogram(
                new Histogram()
                    .setFirstBucketOffset(value.getFirstBucketOffset())
                    .setBucketCounts(ImmutableList.copyOf(value.getBuckets())));

    return initUpdate(name, delta, "DISTRIBUTION").setDistribution(distributionUpdate);
  }

  /** Takes a long and returns a {@link SplitInt64}. */
  public static SplitInt64 longToSplitInt(long num) {
    SplitInt64 result = new SplitInt64();
    result.setLowBits(num & 0xffffffffL);
    result.setHighBits((int) (num >> 32));
    return result;
  }

  /**
   * Takes a {@link SplitInt64} and returns a long. A SplintInt64 is composed of a uint32_t low_bits
   * and an int32_t high_bits.
   */
  public static long splitIntToLong(SplitInt64 splitInt) {
    return ((long) splitInt.getHighBits() << 32) | splitInt.getLowBits();
  }
}
