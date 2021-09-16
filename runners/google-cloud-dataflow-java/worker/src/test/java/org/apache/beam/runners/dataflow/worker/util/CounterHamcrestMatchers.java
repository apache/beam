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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.DistributionUpdate;
import com.google.api.services.dataflow.model.NameAndKind;
import com.google.api.services.dataflow.model.SplitInt64;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** Matchers for {@link Counter} and {@link CounterUpdate}. */
public final class CounterHamcrestMatchers {
  private CounterHamcrestMatchers() {}

  /** Matcher for {@link Counter} name. */
  public static class CounterNameMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final String name;

    public CounterNameMatcher(String name) {
      this.name = name;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counter) {
      NameAndKind nameAndKind = counter.getNameAndKind();
      return nameAndKind != null && name.equals(nameAndKind.getName());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterName " + name);
    }

    public static TypeSafeMatcher<CounterUpdate> hasName(String name) {
      return new CounterNameMatcher(name);
    }
  }

  /** Matcher for {@link CounterUpdate} structured name. */
  public static class CounterStructuredNameMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final @Nullable CounterName name;
    private final @Nullable String kind;

    private CounterStructuredNameMatcher(@Nullable CounterName name, @Nullable String kind) {
      this.name = name;
      this.kind = kind;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      CounterStructuredNameAndMetadata extractedBlob = counterUpdate.getStructuredNameAndMetadata();
      if (extractedBlob == null) {
        return false;
      }

      // Check for presence of both CounterStructuredName and CounterMetadata, and that they have
      // the expected name and kind
      CounterStructuredName extractedName = extractedBlob.getName();
      CounterMetadata extractedMetadata = extractedBlob.getMetadata();
      if (extractedName == null) {
        return false; // CounterStructuredName is missing
      } else if (this.name != null && !this.name.name().equals(extractedName.getName())) {
        return false; // CounterStructuredName is mismatched
      } else if (extractedMetadata == null) {
        return false; // CounterMetadata is missing
      } else if (this.kind != null && !this.kind.equals(extractedMetadata.getKind())) {
        return false; // CounterMetadata is mismatched
      }

      // We either use the contextOriginalName, in which case componentStepName must be null, or
      // we use componentStepName and contextOriginalName must be null.
      if (name == null) {
        // No name specified
        return true;
      }

      boolean toRet = true;
      if (name.usesContextOriginalName()) {
        toRet &=
            name.contextOriginalName().equals(extractedName.getOriginalStepName())
                && extractedName.getComponentStepName() == null;
      } else if (name.usesContextSystemName()) {
        return name.contextSystemName().equals(extractedName.getComponentStepName())
            && extractedName.getOriginalStepName() == null;
      } else if (name.originalRequestingStepName() != null) {
        return (name.originalRequestingStepName()
                .equals(extractedName.getOriginalRequestingStepName())
            && (name.inputIndex() != null
                || name.inputIndex().equals(extractedName.getInputIndex())));
      } else {
        throw new IllegalStateException(
            "Name is structured but does not use the original or optimized name");
      }

      if (name.usesOriginalRequestingStepName()) {
        toRet &=
            name.originalRequestingStepName().equals(extractedName.getOriginalRequestingStepName());
      }
      return toRet;
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("StructuredName: ")
          .appendValue(name)
          .appendText(" Kind:")
          .appendValue(kind);
    }

    public static TypeSafeMatcher<CounterUpdate> hasStructuredName() {
      return new CounterStructuredNameMatcher(null, null);
    }

    public static TypeSafeMatcher<CounterUpdate> hasStructuredName(CounterName name, String kind) {
      checkArgument(name.isStructured(), "Expected CounterName must be structured");
      return new CounterStructuredNameMatcher(name, kind);
    }
  }

  /** Matcher for {@link Counter} aggregation kind. */
  public static class CounterKindMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final String kind;

    public CounterKindMatcher(String kind) {
      this.kind = kind;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counter) {
      return kind.equals(counter.getNameAndKind().getKind());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterKind " + kind);
    }

    public static TypeSafeMatcher<CounterUpdate> hasKind(String kind) {
      return new CounterKindMatcher(kind);
    }
  }

  private static int splitIntToIntValue(SplitInt64 i) {
    return (int) DataflowCounterUpdateExtractor.splitIntToLong(i);
  }

  /** Matcher for {@link CounterUpdate} integer value. */
  public static class CounterUpdateIntegerValueMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final Integer value;

    public CounterUpdateIntegerValueMatcher(Integer value) {
      this.value = value;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      return value.equals(splitIntToIntValue(counterUpdate.getInteger()));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterIntegerValue " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasIntegerValue(Integer value) {
      return new CounterUpdateIntegerValueMatcher(value);
    }
  }

  /** Matcher for {@link CounterUpdate} double value. */
  public static class CounterUpdateDoubleValueMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final Double value;

    public CounterUpdateDoubleValueMatcher(Double value) {
      this.value = value;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      return value.equals(counterUpdate.getFloatingPoint());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterDoubleValue " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasDoubleValue(Double value) {
      return new CounterUpdateDoubleValueMatcher(value);
    }
  }

  /** Matcher for {@link CounterUpdate} boolean value. */
  public static class CounterUpdateBooleanValueMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final Boolean value;

    public CounterUpdateBooleanValueMatcher(Boolean value) {
      this.value = value;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      return value.equals(counterUpdate.getBoolean());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterBooleanValue " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasBooleanValue(Boolean value) {
      return new CounterUpdateBooleanValueMatcher(value);
    }
  }

  /** Matcher for {@link CounterUpdate} integer sum. */
  public static class CounterUpdateIntegerSumMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final Integer value;

    public CounterUpdateIntegerSumMatcher(Integer value) {
      this.value = value;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      return value.equals(splitIntToIntValue(counterUpdate.getIntegerMean().getSum()));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterIntegerMeanSumValue " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasIntegerSum(Integer value) {
      return new CounterUpdateIntegerSumMatcher(value);
    }
  }

  /** Matcher for {@link CounterUpdate} integer count. */
  public static class CounterUpdateIntegerCountMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final Integer value;

    public CounterUpdateIntegerCountMatcher(Integer value) {
      this.value = value;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      return value.equals(splitIntToIntValue(counterUpdate.getIntegerMean().getCount()));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterIntegerMeanSumValue " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasIntegerCount(Integer value) {
      return new CounterUpdateIntegerCountMatcher(value);
    }
  }

  /** Matcher for {@link CounterUpdate} double sum. */
  public static class CounterUpdateDoubleSumMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final Double value;

    public CounterUpdateDoubleSumMatcher(Double value) {
      this.value = value;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      return value.equals(counterUpdate.getFloatingPointMean().getSum());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterDoubleMeanSum " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasDoubleSum(Double value) {
      return new CounterUpdateDoubleSumMatcher(value);
    }
  }

  /** Matcher for {@link CounterUpdate} double count. */
  public static class CounterUpdateDoubleCountMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final Integer value;

    public CounterUpdateDoubleCountMatcher(Integer value) {
      this.value = value;
    }

    @Override
    public boolean matchesSafely(CounterUpdate counterUpdate) {
      return value.equals(splitIntToIntValue(counterUpdate.getFloatingPointMean().getCount()));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterDoubleMeanSumValue " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasDoubleCount(Integer value) {
      return new CounterUpdateDoubleCountMatcher(value);
    }
  }

  /** Matcher for {@link CounterUpdate} distributions. */
  public static class CounterUpdateDistributionMatcher extends TypeSafeMatcher<CounterUpdate> {
    private final CounterFactory.CounterDistribution value;

    public CounterUpdateDistributionMatcher(CounterFactory.CounterDistribution value) {
      Preconditions.checkNotNull(value, "value must be non-null");
      this.value = value;
    }

    @Override
    protected boolean matchesSafely(CounterUpdate counterUpdate) {
      DistributionUpdate distribution = counterUpdate.getDistribution();
      // Complex conditional broken up to enable individual breakpoints for debugging.
      if (distribution == null) {
        return false;
      } else if (value.getMin() != splitIntToIntValue(distribution.getMin())) {
        return false;
      } else if (value.getMax() != splitIntToIntValue(distribution.getMax())) {
        return false;
      } else if (value.getCount() != splitIntToIntValue(distribution.getCount())) {
        return false;
      } else if (value.getSum() != splitIntToIntValue(distribution.getSum())) {
        return false;
      } else if (value.getSumOfSquares() != distribution.getSumOfSquares()) {
        return false;
      } else if (value.getFirstBucketOffset()
          != distribution.getHistogram().getFirstBucketOffset()) {
        return false;
      } else if (!value.getBuckets().equals(distribution.getHistogram().getBucketCounts())) {
        return false;
      }

      return true;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("CounterDistribution " + value);
    }

    public static TypeSafeMatcher<CounterUpdate> hasDistribution(
        CounterFactory.CounterDistribution value) {
      return new CounterUpdateDistributionMatcher(value);
    }
  }
}
