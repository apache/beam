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
package org.apache.beam.runners.core.metrics;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.dataflow.model.Base2Exponent;
import com.google.api.services.dataflow.model.BucketOptions;
import com.google.api.services.dataflow.model.DataflowHistogramValue;
import com.google.api.services.dataflow.model.Linear;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;

// TODO(#33093): Refactor out DataflowHistogramValue to be runner agnostic, and rename to
// remove Dataflow reference.

/** A set of functions used to encode and decode common monitoring info types. */
public class MonitoringInfoEncodings {
  private static final Coder<Long> VARINT_CODER = VarLongCoder.of();
  private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();
  private static final IterableCoder<String> STRING_SET_CODER =
      IterableCoder.of(StringUtf8Coder.of());

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#DISTRIBUTION_INT64_TYPE}. */
  public static ByteString encodeInt64Distribution(DistributionData data) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(data.count(), output);
      VARINT_CODER.encode(data.sum(), output);
      VARINT_CODER.encode(data.min(), output);
      VARINT_CODER.encode(data.max(), output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#DISTRIBUTION_INT64_TYPE}. */
  public static DistributionData decodeInt64Distribution(ByteString payload) {
    InputStream input = payload.newInput();
    try {
      long count = VARINT_CODER.decode(input);
      long sum = VARINT_CODER.decode(input);
      long min = VARINT_CODER.decode(input);
      long max = VARINT_CODER.decode(input);
      return DistributionData.create(sum, count, min, max);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#DISTRIBUTION_DOUBLE_TYPE}. */
  // TODO(BEAM-4374): Implement decodeDoubleDistribution(...)
  public static ByteString encodeDoubleDistribution(
      long count, double sum, double min, double max) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(count, output);
      DOUBLE_CODER.encode(sum, output);
      DOUBLE_CODER.encode(min, output);
      DOUBLE_CODER.encode(max, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#LATEST_INT64_TYPE}. */
  public static ByteString encodeInt64Gauge(GaugeData data) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(data.timestamp().getMillis(), output);
      VARINT_CODER.encode(data.value(), output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#LATEST_INT64_TYPE}. */
  public static GaugeData decodeInt64Gauge(ByteString payload) {
    InputStream input = payload.newInput();
    try {
      Instant timestamp = new Instant(VARINT_CODER.decode(input));
      return GaugeData.create(VARINT_CODER.decode(input), timestamp);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#SET_STRING_TYPE}. */
  public static ByteString encodeStringSet(StringSetData data) {
    try (ByteStringOutputStream output = new ByteStringOutputStream()) {
      STRING_SET_CODER.encode(data.stringSet(), output);
      return output.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#SET_STRING_TYPE}. */
  public static StringSetData decodeStringSet(ByteString payload) {
    try (InputStream input = payload.newInput()) {
      Set<String> elements = Sets.newHashSet(STRING_SET_CODER.decode(input));
      return StringSetData.create(elements);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#SUM_INT64_TYPE}. */
  public static ByteString encodeInt64Counter(long value) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      VARINT_CODER.encode(value, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#SUM_INT64_TYPE}. */
  public static long decodeInt64Counter(ByteString payload) {
    try {
      return VarLongCoder.of().decode(payload.newInput());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#SUM_DOUBLE_TYPE}. */
  public static ByteString encodeDoubleCounter(double value) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      DOUBLE_CODER.encode(value, output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output.toByteString();
  }

  /** Decodes from {@link MonitoringInfoConstants.TypeUrns#SUM_DOUBLE_TYPE}. */
  public static double decodeDoubleCounter(ByteString payload) {
    try {
      return DOUBLE_CODER.decode(payload.newInput());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Encodes to {@link MonitoringInfoConstants.TypeUrns#PER_WORKER_HISTOGRAM}. */
  public static ByteString encodeInt64Histogram(HistogramData inputHistogram) {
    try {
      int numberOfBuckets = inputHistogram.getBucketType().getNumBuckets();

      DataflowHistogramValue outputHistogram2 = new DataflowHistogramValue();

      if (inputHistogram.getBucketType() instanceof HistogramData.LinearBuckets) {
        HistogramData.LinearBuckets buckets =
            (HistogramData.LinearBuckets) inputHistogram.getBucketType();
        Linear linear = new Linear();
        linear.setNumberOfBuckets(numberOfBuckets);
        linear.setWidth(buckets.getWidth());
        linear.setStart(buckets.getStart());
        outputHistogram2.setBucketOptions(new BucketOptions().setLinear(linear));
      } else if (inputHistogram.getBucketType() instanceof HistogramData.ExponentialBuckets) {
        HistogramData.ExponentialBuckets buckets =
            (HistogramData.ExponentialBuckets) inputHistogram.getBucketType();
        Base2Exponent base2Exp = new Base2Exponent();
        base2Exp.setNumberOfBuckets(numberOfBuckets);
        base2Exp.setScale(buckets.getScale());
        outputHistogram2.setBucketOptions(new BucketOptions().setExponential(base2Exp));
      } else {
        throw new RuntimeException("Unable to parse histogram, bucket is not recognized");
      }

      outputHistogram2.setCount(inputHistogram.getTotalCount());

      List<Long> bucketCounts = new ArrayList<>();

      Arrays.stream(inputHistogram.getBucketCount())
          .forEach(
              val -> {
                bucketCounts.add(val);
              });

      outputHistogram2.setBucketCounts(bucketCounts);

      ObjectMapper objectMapper = new ObjectMapper();
      String jsonString = objectMapper.writeValueAsString(outputHistogram2);

      return ByteString.copyFromUtf8(jsonString);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Decodes to {@link MonitoringInfoConstants.TypeUrns#PER_WORKER_HISTOGRAM}. */
  public static HistogramData decodeInt64Histogram(ByteString payload) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      JsonNode jsonNode = objectMapper.readTree(payload.toStringUtf8()); // parse afterwards
      DataflowHistogramValue newHist = new DataflowHistogramValue();
      newHist.setCount(jsonNode.get("count").asLong());

      List<Long> bucketCounts = new ArrayList<>();
      Iterator<JsonNode> itr = jsonNode.get("bucketCounts").iterator();
      while (itr.hasNext()) {
        Long item = itr.next().asLong();
        bucketCounts.add(item);
      }
      newHist.setBucketCounts(bucketCounts);

      if (jsonNode.get("bucketOptions").has("linear")) {
        Linear linear = new Linear();
        JsonNode linearNode = jsonNode.get("bucketOptions").get("linear");
        linear.setNumberOfBuckets(linearNode.get("numberOfBuckets").asInt());
        linear.setWidth(linearNode.get("width").asDouble());
        linear.setStart(linearNode.get("start").asDouble());
        newHist.setBucketOptions(new BucketOptions().setLinear(linear));
      } else if (jsonNode.get("bucketOptions").has("exponential")) {
        Base2Exponent base2Exp = new Base2Exponent();
        JsonNode expNode = jsonNode.get("bucketOptions").get("exponential");
        base2Exp.setNumberOfBuckets(expNode.get("numberOfBuckets").asInt());
        base2Exp.setScale(expNode.get("scale").asInt());
        newHist.setBucketOptions(new BucketOptions().setExponential(base2Exp));
      } else {
        throw new RuntimeException("Unable to parse histogram, bucket is not recognized");
      }
      return new HistogramData(newHist);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
