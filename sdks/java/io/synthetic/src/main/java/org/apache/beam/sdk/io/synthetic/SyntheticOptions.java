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
package org.apache.beam.sdk.io.synthetic;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.commons.math3.distribution.ConstantRealDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * This {@link SyntheticOptions} class provides common parameterizable synthetic options that are
 * used by {@link SyntheticBoundedSource} and {@link SyntheticUnboundedSource}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SyntheticOptions implements Serializable {
  private static final long serialVersionUID = 0;

  /**
   * The type of Delay that will be produced.
   *
   * <p>CPU delay produces a CPU-busy delay. SLEEP delay makes the process sleep.
   */
  public enum DelayType {
    SLEEP,
    CPU,
    MIXED,
  }

  /**
   * Wrapper over a distribution. Unfortunately commons-math does not provide a common interface
   * over both RealDistribution and IntegerDistribution, and we sometimes need one and sometimes the
   * other.
   */
  public interface Sampler extends Serializable {
    double sample(long seed);

    // Make this class a bean, so Jackson can serialize it during SyntheticOptions.toString().
    Object getDistribution();
  }

  public static Sampler fromRealDistribution(final RealDistribution dist) {
    return new Sampler() {
      private static final long serialVersionUID = 0L;

      @Override
      public double sample(long seed) {
        dist.reseedRandomGenerator(seed);
        return dist.sample();
      }

      @Override
      public Object getDistribution() {
        return dist;
      }
    };
  }

  public static Sampler fromIntegerDistribution(final IntegerDistribution dist) {
    return new Sampler() {
      private static final long serialVersionUID = 0L;

      @Override
      public double sample(long seed) {
        dist.reseedRandomGenerator(seed);
        return dist.sample();
      }

      @Override
      public Object getDistribution() {
        return dist;
      }
    };
  }

  private static Sampler scaledSampler(final Sampler sampler, final double multiplier) {
    return new Sampler() {
      private static final long serialVersionUID = 0L;

      @Override
      public double sample(long seed) {
        return multiplier * sampler.sample(seed);
      }

      @Override
      public Object getDistribution() {
        return sampler.getDistribution();
      }
    };
  }

  /** The key size in bytes. */
  @JsonProperty public long keySizeBytes = 1;

  /** The value size in bytes. */
  @JsonProperty public long valueSizeBytes = 1;

  /**
   * The size of a single record used for size estimation in bytes. If less than zero, keySizeBytes
   * + valueSizeBytes is used.
   */
  @JsonProperty public final long bytesPerRecord;

  /** The number of distinct "hot" keys. */
  @JsonProperty public long numHotKeys;

  /**
   * The fraction of records associated with "hot" keys, which are uniformly distributed over a
   * fixed number of hot keys.
   */
  @JsonProperty public double hotKeyFraction;

  /** The fraction of keys that should be larger than others. */
  @JsonProperty public double largeKeyFraction = 0.0;

  /** The size of large keys. */
  @JsonProperty public double largeKeySizeBytes = 1000;

  /** The seed is used for generating a hash function implementing the 128-bit murmur3 algorithm. */
  @JsonIgnore public int seed = 1;

  /**
   * The hash function is used to generate seeds that are fed into the random number generators and
   * the sleep time distributions.
   */
  @JsonIgnore private transient HashFunction hashFunction;

  /**
   * SyntheticOptions supports several delay distributions including uniform, normal, exponential,
   * and constant delay per record. The delay is either sleep or CPU spinning for the duration.
   *
   * <ul>
   *   <li>The uniform delay distribution is specified through
   *       "delayDistribution":{"type":"uniform","lower":lower_bound,"upper":upper_bound}, where
   *       lower_bound and upper_bound are non-negative numbers representing the delay range in
   *       milliseconds.
   *   <li>The normal delay distribution is specified through
   *       "delayDistribution":{"type":"normal","mean":mean,"stddev":stddev}, where mean is a
   *       non-negative number representing the mean of this normal distributed delay in
   *       milliseconds and stddev is a positive number representing its standard deviation.
   *   <li>The exponential delay distribution is specified through
   *       "delayDistribution":{"type":"exp","mean":mean}, where mean is a positive number
   *       representing the mean of this exponentially distributed delay in milliseconds.
   *   <li>The zipf distribution is specified through
   *       "delayDistribution":{"type":"zipf","param":param,"multiplier":multiplier}, where param is
   *       a number &gt; 1 and multiplier just scales the output of the distribution. By default,
   *       the multiplier is 1. Parameters closer to 1 produce dramatically more skewed results.
   *       E.g. given 100 samples, the min will almost always be 1, while max with param 3 will
   *       usually be below 10; with param 2 max will usually be between several dozen and several
   *       hundred; with param 1.5, thousands to millions.
   *   <li>The constant sleep time per record is specified through
   *       "delayDistribution":{"type":"const","const":const} where const is a non-negative number
   *       representing the constant sleep time in milliseconds.
   * </ul>
   *
   * <p>The field delayDistribution is not used in the synthetic unbounded source. The synthetic
   * unbounded source uses RateLimiter to control QPS.
   */
  @JsonDeserialize(using = SamplerDeserializer.class)
  Sampler delayDistribution = fromRealDistribution(new ConstantRealDistribution(0));

  /**
   * When 'delayDistribution' is configured, this indicates how the delay enforced ("SLEEP", "CPU",
   * or "MIXED").
   */
  @JsonProperty public final DelayType delayType = DelayType.SLEEP;

  /**
   * CPU utilization when delayType is 'MIXED'. This determines the fraction of processing time
   * spent spinning. The remaining time is spent sleeping. For each millisecond of processing time
   * we choose to spin with probability equal to this fraction.
   */
  @JsonProperty public final double cpuUtilizationInMixedDelay;

  SyntheticOptions() {
    cpuUtilizationInMixedDelay = 0.1;
    bytesPerRecord = -1;
  }

  @JsonDeserialize
  public void setSeed(int seed) {
    this.seed = seed;
  }

  public HashFunction hashFunction() {
    // due to field's transiency initialize when null.
    if (hashFunction == null) {
      this.hashFunction = Hashing.murmur3_128(seed);
    }

    return hashFunction;
  }

  static class SamplerDeserializer extends JsonDeserializer<Sampler> {
    @Override
    public Sampler deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);
      String type = node.get("type").asText();
      switch (type) {
        case "uniform":
          {
            double lowerBound = node.get("lower").asDouble();
            double upperBound = node.get("upper").asDouble();
            checkArgument(
                lowerBound >= 0,
                "The lower bound of uniform distribution should be a non-negative number, "
                    + "but found %s.",
                lowerBound);
            return fromRealDistribution(new UniformRealDistribution(lowerBound, upperBound));
          }
        case "exp":
          {
            double mean = node.get("mean").asDouble();
            return fromRealDistribution(new ExponentialDistribution(mean));
          }
        case "normal":
          {
            double mean = node.get("mean").asDouble();
            double stddev = node.get("stddev").asDouble();
            checkArgument(
                mean >= 0,
                "The mean of normal distribution should be a non-negative number, but found %s.",
                mean);
            return fromRealDistribution(new NormalDistribution(mean, stddev));
          }
        case "const":
          {
            double constant = node.get("const").asDouble();
            checkArgument(
                constant >= 0,
                "The value of constant distribution should be a non-negative number, but found %s.",
                constant);
            return fromRealDistribution(new ConstantRealDistribution(constant));
          }
        case "zipf":
          {
            double param = node.get("param").asDouble();
            final double multiplier =
                node.has("multiplier") ? node.get("multiplier").asDouble() : 1.0;
            checkArgument(
                param > 1,
                "The parameter of the Zipf distribution should be > 1, but found %s.",
                param);
            checkArgument(
                multiplier >= 0,
                "The multiplier of the Zipf distribution should be >= 0, but found %s.",
                multiplier);
            final ZipfDistribution dist = new ZipfDistribution(100, param);
            return scaledSampler(fromIntegerDistribution(dist), multiplier);
          }
        default:
          {
            throw new IllegalArgumentException("Unknown distribution type: " + type);
          }
      }
    }
  }

  public void validate() {
    checkArgument(
        keySizeBytes > 0, "keySizeBytes should be a positive number, but found %s", keySizeBytes);
    checkArgument(
        valueSizeBytes >= 0,
        "valueSizeBytes should be a non-negative number, but found %s",
        valueSizeBytes);
    checkArgument(
        numHotKeys >= 0, "numHotKeys should be a non-negative number, but found %s", numHotKeys);
    checkArgument(
        hotKeyFraction >= 0,
        "hotKeyFraction should be a non-negative number, but found %s",
        hotKeyFraction);
    if (hotKeyFraction > 0) {
      int intBytes = Integer.SIZE / 8;
      checkArgument(
          keySizeBytes >= intBytes,
          "Allowing hot keys (hotKeyFraction=%s) requires keySizeBytes "
              + "to be at least %s, but found %s",
          hotKeyFraction,
          intBytes,
          keySizeBytes);
    }
  }

  @Override
  public String toString() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long nextDelay(long seed) {
    return (long) delayDistribution.sample(seed);
  }

  public KV<byte[], byte[]> genKvPair(long seed) {
    Random random = new Random(seed);

    byte[] key = new byte[(int) keySizeBytes];
    // Set the user-key to contain characters other than ordered-code escape characters
    // (specifically '\0' or '\xff'). The user-key is encoded into the shuffle-key using
    // ordered-code, and the shuffle-key is then checked for size limit violations. A user-key
    // consisting of '\0' keySizeBytes would produce a shuffle-key encoding double in size,
    // which would go over the shuffle-key limit (see b/28770924).
    for (int i = 0; i < keySizeBytes; ++i) {
      key[i] = 42;
    }
    // Determines whether to generate hot key or not.
    if (random.nextDouble() < hotKeyFraction) {
      // Generate hot key.
      // An integer is randomly selected from the range [0, numHotKeys-1] with equal probability.
      int randInt = random.nextInt((int) numHotKeys);
      ByteBuffer.wrap(key).putInt(hashFunction().hashInt(randInt).asInt());
    } else {
      // Note that the random generated key might be a hot key.
      // But the probability of being a hot key is very small.
      random.nextBytes(key);
    }

    byte[] val = new byte[(int) valueSizeBytes];
    random.nextBytes(val);
    return KV.of(key, val);
  }

  public static <T extends SyntheticOptions> T fromJsonString(String json, Class<T> type)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    T result = mapper.readValue(json, type);
    result.validate();
    return result;
  }
}
