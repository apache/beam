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
package org.apache.beam.sdk.io.redis;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * An IO to manipulate Redis key/value database.
 *
 * <h3>Reading Redis key/value pairs</h3>
 *
 * <p>{@link #read()} provides a source which returns a bounded {@link PCollection} containing
 * key/value pairs as {@code KV<String, String>}.
 *
 * <p>To configure a Redis source, you have to provide Redis server hostname and port number.
 * Optionally, you can provide a key pattern (to filter the keys). The following example illustrates
 * how to configure a source:
 *
 * <pre>{@code
 * pipeline.apply(RedisIO.read()
 *   .withEndpoint("::1", 6379)
 *   .withKeyPattern("foo*"))
 *
 * }</pre>
 *
 * <p>It's also possible to specify Redis authentication and connection timeout with the
 * corresponding methods:
 *
 * <pre>{@code
 * pipeline.apply(RedisIO.read()
 *   .withEndpoint("::1", 6379)
 *   .withAuth("authPassword")
 *   .withTimeout(60000)
 *   .withKeyPattern("foo*"))
 *
 * }</pre>
 *
 * <p>{@link #readKeyPatterns()} can be used to request Redis server using input PCollection
 * elements as key pattern (as String).
 *
 * <pre>{@code
 * pipeline.apply(...)
 *    // here we have a PCollection<String> with the key patterns
 *    .apply(RedisIO.readKeyPatterns().withEndpoint("::1", 6379))
 *   // here we have a PCollection<KV<String,String>>
 *
 * }</pre>
 *
 * <h3>Writing Redis key/value pairs</h3>
 *
 * <p>{@link #write()} provides a sink to write key/value pairs represented as {@link KV} from an
 * incoming {@link PCollection}.
 *
 * <p>To configure the target Redis server, you have to provide Redis server hostname and port
 * number. The following example illustrates how to configure a sink:
 *
 * <pre>{@code
 * pipeline.apply(...)
 *   // here we a have a PCollection<String, String> with key/value pairs
 *   .apply(RedisIO.write().withEndpoint("::1", 6379))
 *
 * }</pre>
 *
 * <h3>Writing Redis Streams</h3>
 *
 * <p>{@link #writeStreams()} appends the entries of a {@link PCollection} of key/value pairs
 * represented as {@link KV} to the Redis stream at the specified key using the <a
 * href='https://redis.io/commands/XADD'>XADD</a> API.
 *
 * <p>To configure the target Redis server, you have to provide a Redis server hostname and port
 * number. The following example illustrates how to configure a sink:
 *
 * <pre>{@code
 * pipeline.apply(...)
 *   // here we have a PCollection<KV<String, Map<String, String>>>
 *   .apply(RedisIO.writeStreams().withEndpoint("::1", 6379))
 * }</pre>
 *
 * <p>Redis Streams optionally can be capped to a specific or exact length (see the documentation
 * for the <a href="https://redis.io/commands/xtrim">XTRIM</a> API); {@link #writeStreams()} lets
 * you specify MAXLEN using the {@code withMaxLen()} option.
 *
 * <p>Trimming a stream to an exact length is noted in the Redis documentation as being inefficient;
 * the {@code withApproximateTrim()} boolean option will add the {@code ~} prefix to {@code MAXLEN},
 * which tells Redis to use "almost exact" trimming.
 *
 * <p>See the <a href="https://redis.io/topics/streams-intro">Redis Streams documentation</a> for a
 * deeper discussion of the issues involved.
 *
 * <p>The following example illustrates how to configure a sink with trimming:
 *
 * <pre>{@code
 * pipeline.apply(...)
 *   // here we have a PCollection<KV<String, Map<String, String>>>
 *   .apply(RedisIO.writeStreams()
 *     .withEndpoint("::1", 6379)
 *     .withMaxLen(1024L)
 *     .withApproximateTrim(true)
 *    )
 * }</pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RedisIO {

  /** Read data from a Redis server. */
  public static Read read() {
    return new AutoValue_RedisIO_Read.Builder()
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .setKeyPattern("*")
        .setBatchSize(1000)
        .setOutputParallelization(true)
        .build();
  }

  /**
   * Like {@link #read()} but executes multiple instances of the Redis query substituting each
   * element of a {@link PCollection} as key pattern.
   */
  public static ReadKeyPatterns readKeyPatterns() {
    return new AutoValue_RedisIO_ReadKeyPatterns.Builder()
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .setBatchSize(1000)
        .setOutputParallelization(true)
        .build();
  }

  /** Write data to a Redis server. */
  public static Write write() {
    return new AutoValue_RedisIO_Write.Builder()
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .setMethod(Write.Method.APPEND)
        .build();
  }

  /** Write stream data to a Redis server. */
  public static WriteStreams writeStreams() {
    return new AutoValue_RedisIO_WriteStreams.Builder()
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .setMaxLen(0L)
        .setApproximateTrim(true)
        .build();
  }

  private RedisIO() {}

  /** Implementation of {@link #read()}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KV<String, String>>> {

    abstract @Nullable RedisConnectionConfiguration connectionConfiguration();

    abstract @Nullable String keyPattern();

    abstract int batchSize();

    abstract boolean outputParallelization();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract @Nullable Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);

      abstract @Nullable Builder setKeyPattern(String keyPattern);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setOutputParallelization(boolean outputParallelization);

      abstract Read build();
    }

    public Read withEndpoint(String host, int port) {
      checkArgument(host != null, "host can not be null");
      checkArgument(0 < port && port < 65536, "port must be a positive integer less than 65536");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withHost(host).withPort(port))
          .build();
    }

    public Read withAuth(String auth) {
      checkArgument(auth != null, "auth can not be null");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withAuth(auth))
          .build();
    }

    public Read withTimeout(int timeout) {
      checkArgument(timeout >= 0, "timeout can not be negative");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withTimeout(timeout))
          .build();
    }

    public Read withKeyPattern(String keyPattern) {
      checkArgument(keyPattern != null, "keyPattern can not be null");
      return toBuilder().setKeyPattern(keyPattern).build();
    }

    public Read withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return toBuilder().setConnectionConfiguration(connection).build();
    }

    public Read withBatchSize(int batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    /**
     * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
     * default is to parallelize and should only be changed if this is known to be unnecessary.
     */
    public Read withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connectionConfiguration().populateDisplayData(builder);
    }

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
      checkArgument(connectionConfiguration() != null, "withConnectionConfiguration() is required");

      return input
          .apply(Create.of(keyPattern()))
          .apply(
              RedisIO.readKeyPatterns()
                  .withConnectionConfiguration(connectionConfiguration())
                  .withBatchSize(batchSize())
                  .withOutputParallelization(outputParallelization()));
    }
  }

  /** Implementation of {@link #readKeyPatterns()}. */
  @AutoValue
  public abstract static class ReadKeyPatterns
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {

    abstract @Nullable RedisConnectionConfiguration connectionConfiguration();

    abstract int batchSize();

    abstract boolean outputParallelization();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract @Nullable Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setOutputParallelization(boolean outputParallelization);

      abstract ReadKeyPatterns build();
    }

    public ReadKeyPatterns withEndpoint(String host, int port) {
      checkArgument(host != null, "host can not be null");
      checkArgument(port > 0, "port can not be negative or 0");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withHost(host).withPort(port))
          .build();
    }

    public ReadKeyPatterns withAuth(String auth) {
      checkArgument(auth != null, "auth can not be null");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withAuth(auth))
          .build();
    }

    public ReadKeyPatterns withTimeout(int timeout) {
      checkArgument(timeout >= 0, "timeout can not be negative");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withTimeout(timeout))
          .build();
    }

    public ReadKeyPatterns withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return toBuilder().setConnectionConfiguration(connection).build();
    }

    public ReadKeyPatterns withBatchSize(int batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    /**
     * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
     * default is to parallelize and should only be changed if this is known to be unnecessary.
     */
    public ReadKeyPatterns withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> input) {
      checkArgument(connectionConfiguration() != null, "withConnectionConfiguration() is required");
      PCollection<KV<String, String>> output =
          input
              .apply(ParDo.of(new ReadFn(connectionConfiguration())))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
      if (outputParallelization()) {
        output = output.apply(new Reparallelize());
      }
      return output;
    }
  }

  @DoFn.BoundedPerElement
  private static class ReadFn extends DoFn<String, KV<String, String>> {

    protected final RedisConnectionConfiguration connectionConfiguration;
    transient Jedis jedis;

    ReadFn(RedisConnectionConfiguration connectionConfiguration) {
      this.connectionConfiguration = connectionConfiguration;
    }

    @Setup
    public void setup() {
      jedis = connectionConfiguration.connect();
    }

    @Teardown
    public void teardown() {
      jedis.close();
    }

    @GetInitialRestriction
    public ByteKeyRange getInitialRestriction() {
      return ByteKeyRange.of(ByteKey.of(0x00), ByteKey.EMPTY);
    }

    @ProcessElement
    public void processElement(
        ProcessContext c, RestrictionTracker<ByteKeyRange, ByteKey> tracker) {
      ByteKey cursor = tracker.currentRestriction().getStartKey();
      RedisCursor redisCursor = RedisCursor.byteKeyToRedisCursor(cursor, jedis.dbSize(), true);
      ScanParams scanParams = new ScanParams();
      scanParams.match(c.element());
      while (tracker.tryClaim(cursor)) {
        ScanResult<String> scanResult = jedis.scan(redisCursor.getCursor(), scanParams);
        if (scanResult.getResult().size() > 0) {
          String[] keys = scanResult.getResult().toArray(new String[scanResult.getResult().size()]);
          List<String> results = jedis.mget(keys);
          for (int i = 0; i < results.size(); i++) {
            if (results.get(i) != null) {
              c.output(KV.of(keys[i], results.get(i)));
            }
          }
        }
        redisCursor = RedisCursor.of(scanResult.getCursor(), jedis.dbSize(), false);
        cursor = RedisCursor.redisCursorToByteKey(redisCursor);
      }
    }
  }

  private static class Reparallelize
      extends PTransform<PCollection<KV<String, String>>, PCollection<KV<String, String>>> {

    @Override
    public PCollection<KV<String, String>> expand(PCollection<KV<String, String>> input) {
      // reparallelize mimics the same behavior as in JdbcIO, used to break fusion
      PCollectionView<Iterable<KV<String, String>>> empty =
          input
              .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
              .apply(View.asIterable());
      PCollection<KV<String, String>> materialized =
          input.apply(
              "Identity",
              ParDo.of(
                      new DoFn<KV<String, String>, KV<String, String>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          c.output(c.element());
                        }
                      })
                  .withSideInputs(empty));
      /* Redis Scan may return a given element multiple times, so we use the Latest.perKey() transform to remove duplicates,
      see "Scan guarantees" in https://redis.io/commands/scan */
      return materialized.apply(Latest.perKey());
    }
  }

  /** A {@link PTransform} to write to a Redis server. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<KV<String, String>>, PDone> {

    /** Determines the method used to insert data in Redis. */
    public enum Method {

      /**
       * Use APPEND command. If key already exists and is a string, this command appends the value
       * at the end of the string.
       */
      APPEND,

      /** Use SET command. If key already holds a value, it is overwritten. */
      SET,

      /**
       * Use LPUSH command. Insert value at the head of the list stored at key. If key does not
       * exist, it is created as empty list before performing the push operations. When key holds a
       * value that is not a list, an error is returned.
       */
      LPUSH,

      /**
       * Use RPUSH command. Insert value at the tail of the list stored at key. If key does not
       * exist, it is created as empty list before performing the push operations. When key holds a
       * value that is not a list, an error is returned.
       */
      RPUSH,

      /** Use SADD command. Insert value in a set. Duplicated values are ignored. */
      SADD,

      /** Use PFADD command. Insert value in a HLL structure. Create key if it doesn't exist */
      PFADD,

      /** Use INCBY command. Increment counter value of a key by a given value. */
      INCRBY,

      /** Use DECRBY command. Decrement counter value of a key by given value. */
      DECRBY,
    }

    abstract @Nullable RedisConnectionConfiguration connectionConfiguration();

    abstract @Nullable Method method();

    abstract @Nullable Long expireTime();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setConnectionConfiguration(
          RedisConnectionConfiguration connectionConfiguration);

      abstract Builder setMethod(Method method);

      abstract Builder setExpireTime(Long expireTimeMillis);

      abstract Write build();
    }

    public Write withEndpoint(String host, int port) {
      checkArgument(host != null, "host can not be null");
      checkArgument(port > 0, "port can not be negative or 0");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withHost(host).withPort(port))
          .build();
    }

    public Write withAuth(String auth) {
      checkArgument(auth != null, "auth can not be null");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withAuth(auth))
          .build();
    }

    public Write withTimeout(int timeout) {
      checkArgument(timeout >= 0, "timeout can not be negative");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withTimeout(timeout))
          .build();
    }

    public Write withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return toBuilder().setConnectionConfiguration(connection).build();
    }

    public Write withMethod(Method method) {
      checkArgument(method != null, "method can not be null");
      return toBuilder().setMethod(method).build();
    }

    public Write withExpireTime(Long expireTimeMillis) {
      checkArgument(expireTimeMillis != null, "expireTimeMillis can not be null");
      checkArgument(expireTimeMillis > 0, "expireTimeMillis can not be negative or 0");
      return toBuilder().setExpireTime(expireTimeMillis).build();
    }

    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
      checkArgument(connectionConfiguration() != null, "withConnectionConfiguration() is required");

      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriteFn extends DoFn<KV<String, String>, Void> {

      private static final int DEFAULT_BATCH_SIZE = 1000;

      private final Write spec;

      private transient Jedis jedis;
      private transient @Nullable Transaction transaction;

      private int batchCount;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        jedis = spec.connectionConfiguration().connect();
      }

      @StartBundle
      public void startBundle() {
        transaction = jedis.multi();
        batchCount = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, String> record = c.element();

        writeRecord(record);

        batchCount++;

        if (batchCount >= DEFAULT_BATCH_SIZE) {
          transaction.exec();
          transaction.multi();
          batchCount = 0;
        }
      }

      private void writeRecord(KV<String, String> record) {
        Method method = spec.method();
        Long expireTime = spec.expireTime();

        if (Method.APPEND == method) {
          writeUsingAppendCommand(record, expireTime);
        } else if (Method.SET == method) {
          writeUsingSetCommand(record, expireTime);
        } else if (Method.LPUSH == method || Method.RPUSH == method) {
          writeUsingListCommand(record, method, expireTime);
        } else if (Method.SADD == method) {
          writeUsingSaddCommand(record, expireTime);
        } else if (Method.PFADD == method) {
          writeUsingHLLCommand(record, expireTime);
        } else if (Method.INCRBY == method) {
          writeUsingIncrBy(record, expireTime);
        } else if (Method.DECRBY == method) {
          writeUsingDecrBy(record, expireTime);
        }
      }

      private void writeUsingAppendCommand(KV<String, String> record, Long expireTime) {
        String key = record.getKey();
        String value = record.getValue();

        transaction.append(key, value);

        setExpireTimeWhenRequired(key, expireTime);
      }

      private void writeUsingSetCommand(KV<String, String> record, Long expireTime) {
        String key = record.getKey();
        String value = record.getValue();

        if (expireTime != null) {
          transaction.psetex(key, expireTime, value);
        } else {
          transaction.set(key, value);
        }
      }

      private void writeUsingListCommand(
          KV<String, String> record, Method method, Long expireTime) {

        String key = record.getKey();
        String value = record.getValue();

        if (Method.LPUSH == method) {
          transaction.lpush(key, value);
        } else if (Method.RPUSH == method) {
          transaction.rpush(key, value);
        }

        setExpireTimeWhenRequired(key, expireTime);
      }

      private void writeUsingSaddCommand(KV<String, String> record, Long expireTime) {
        String key = record.getKey();
        String value = record.getValue();

        transaction.sadd(key, value);

        setExpireTimeWhenRequired(key, expireTime);
      }

      private void writeUsingHLLCommand(KV<String, String> record, Long expireTime) {
        String key = record.getKey();
        String value = record.getValue();

        transaction.pfadd(key, value);

        setExpireTimeWhenRequired(key, expireTime);
      }

      private void writeUsingIncrBy(KV<String, String> record, Long expireTime) {
        String key = record.getKey();
        String value = record.getValue();
        long inc = Long.parseLong(value);
        transaction.incrBy(key, inc);

        setExpireTimeWhenRequired(key, expireTime);
      }

      private void writeUsingDecrBy(KV<String, String> record, Long expireTime) {
        String key = record.getKey();
        String value = record.getValue();
        long decr = Long.parseLong(value);
        transaction.decrBy(key, decr);

        setExpireTimeWhenRequired(key, expireTime);
      }

      private void setExpireTimeWhenRequired(String key, Long expireTime) {
        if (expireTime != null) {
          transaction.pexpire(key, expireTime);
        }
      }

      @FinishBundle
      public void finishBundle() {
        if (batchCount > 0) {
          transaction.exec();
        }
        if (transaction != null) {
          transaction.close();
        }
        transaction = null;
        batchCount = 0;
      }

      @Teardown
      public void teardown() {
        jedis.close();
      }
    }
  }

  /**
   * A {@link PTransform} to write stream key pairs (https://redis.io/topics/streams-intro) to a
   * Redis server.
   */
  @AutoValue
  public abstract static class WriteStreams
      extends PTransform<PCollection<KV<String, Map<String, String>>>, PDone> {

    abstract RedisConnectionConfiguration connectionConfiguration();

    abstract long maxLen();

    abstract boolean approximateTrim();

    abstract Builder toBuilder();

    /** Set the hostname and port of the Redis server to connect to. */
    public WriteStreams withEndpoint(String host, int port) {
      checkArgument(host != null, "host can not be null");
      checkArgument(port > 0, "port can not be negative or 0");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withHost(host).withPort(port))
          .build();
    }

    /**
     * Use the redis AUTH command when connecting to the server; the format of the string can be
     * either just a password or a username and password separated by a space. See
     * https://redis.io/commands/auth for details
     */
    public WriteStreams withAuth(String auth) {
      checkArgument(auth != null, "auth can not be null");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withAuth(auth))
          .build();
    }

    /** Set the connection timeout for the Redis server connection. */
    public WriteStreams withTimeout(int timeout) {
      checkArgument(timeout >= 0, "timeout can not be negative");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withTimeout(timeout))
          .build();
    }

    /** Predefine a {@link RedisConnectionConfiguration} and pass it to the builder. */
    public WriteStreams withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return toBuilder().setConnectionConfiguration(connection).build();
    }

    /** When appending (XADD) to a stream, set a MAXLEN option. */
    public WriteStreams withMaxLen(long maxLen) {
      checkArgument(maxLen >= 0L, "maxLen must be positive if set");
      return toBuilder().setMaxLen(maxLen).build();
    }

    /**
     * If {@link #withMaxLen(long)} is used, set the "~" prefix to the MAXLEN value, indicating to
     * the server that it should use "close enough" trimming.
     */
    public WriteStreams withApproximateTrim(boolean approximateTrim) {
      return toBuilder().setApproximateTrim(approximateTrim).build();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setConnectionConfiguration(
          RedisConnectionConfiguration connectionConfiguration);

      abstract Builder setMaxLen(long maxLen);

      abstract Builder setApproximateTrim(boolean approximateTrim);

      abstract WriteStreams build();
    }

    @Override
    public PDone expand(PCollection<KV<String, Map<String, String>>> input) {
      checkArgument(connectionConfiguration() != null, "withConnectionConfiguration() is required");

      input.apply(ParDo.of(new WriteStreamFn(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriteStreamFn extends DoFn<KV<String, Map<String, String>>, Void> {

      private static final int DEFAULT_BATCH_SIZE = 1000;

      private final WriteStreams spec;

      private transient Jedis jedis;
      private transient @Nullable Transaction transaction;

      private int batchCount;

      public WriteStreamFn(WriteStreams spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        jedis = spec.connectionConfiguration().connect();
      }

      @StartBundle
      public void startBundle() {
        transaction = jedis.multi();
        batchCount = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, Map<String, String>> record = c.element();

        writeRecord(record);

        batchCount++;

        if (batchCount >= DEFAULT_BATCH_SIZE) {
          transaction.exec();
          transaction.multi();
          batchCount = 0;
        }
      }

      private void writeRecord(KV<String, Map<String, String>> record) {
        String key = record.getKey();
        Map<String, String> value = record.getValue();
        final XAddParams params = new XAddParams().id(StreamEntryID.NEW_ENTRY);
        if (spec.maxLen() > 0L) {
          params.maxLen(spec.maxLen());
          if (spec.approximateTrim()) {
            params.approximateTrimming();
          }
        }
        transaction.xadd(key, params, value);
      }

      @FinishBundle
      public void finishBundle() {
        if (batchCount > 0) {
          transaction.exec();
        }
        if (transaction != null) {
          transaction.close();
        }
        transaction = null;
        batchCount = 0;
      }

      @Teardown
      public void teardown() {
        jedis.close();
      }
    }
  }
}
