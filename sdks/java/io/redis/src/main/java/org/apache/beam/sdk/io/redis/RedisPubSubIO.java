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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

@Experimental(Experimental.Kind.SOURCE_SINK)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RedisPubSubIO {
  private RedisPubSubIO() {}

  public static Subscribe subscribe() {
    return new AutoValue_RedisPubSubIO_Subscribe.Builder()
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .setChannels(Collections.emptyList())
        .build();
  }

  @AutoValue
  public abstract static class Subscribe
      extends PTransform<PBegin, PCollection<TimestampedValue<KV<String, String>>>> {
    abstract @Nullable RedisConnectionConfiguration connectionConfiguration();

    abstract List<String> channels();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract @Nullable Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);

      abstract @Nullable Builder setChannels(List<String> channels);

      abstract Subscribe build();
    }

    public Subscribe withEndpoint(String host, int port) {
      checkArgument(host != null, "host can not be null");
      checkArgument(0 < port && port < 65536, "port must be a positive integer < 65536");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withHost(host).withPort(port))
          .build();
    }

    public Subscribe withAuth(String auth) {
      checkArgument(auth != null, "auth can not be null.");
      return toBuilder()
          .setConnectionConfiguration(connectionConfiguration().withAuth(auth))
          .build();
    }

    public Subscribe toChannels(List<String> channels) {
      return toBuilder().setChannels(channels).build();
    }

    public Subscribe toChannels(String... channels) {
      return toChannels(Arrays.asList(channels));
    }

    @Override
    public PCollection<TimestampedValue<KV<String, String>>> expand(PBegin input) {
      return input.getPipeline().apply(Read.from(new RedisSource(this)));
    }

    static class RedisSource
        extends UnboundedSource<TimestampedValue<KV<String, String>>, RedisCheckpointMark> {
      final Subscribe spec;

      RedisSource(Subscribe spec) {
        this.spec = spec;
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized List<
              ? extends
                  @UnknownKeyFor @NonNull @Initialized UnboundedSource<
                      TimestampedValue<KV<String, String>>, RedisCheckpointMark>>
          split(
              @UnknownKeyFor @NonNull @Initialized int desiredNumSplits,
              @UnknownKeyFor @NonNull @Initialized PipelineOptions options)
              throws @UnknownKeyFor @NonNull @Initialized Exception {
        // TODO: If we have multiple channels we can have multiple consumers, but for right now just
        // use a
        // single consumer
        checkArgument(desiredNumSplits > 0);
        return ImmutableList.of(this);
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized UnboundedReader<
              TimestampedValue<KV<String, String>>>
          createReader(
              @UnknownKeyFor @NonNull @Initialized PipelineOptions options,
              @org.checkerframework.checker.nullness.qual.Nullable
                  RedisCheckpointMark checkpointMark)
              throws @UnknownKeyFor @NonNull @Initialized IOException {
        return new RedisUnboundedReader(this, checkpointMark);
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized Coder<TimestampedValue<KV<String, String>>>
          getOutputCoder() {
        return TimestampedValue.TimestampedValueCoder.of(
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized Coder<RedisCheckpointMark>
          getCheckpointMarkCoder() {
        return SerializableCoder.of(RedisCheckpointMark.class);
      }

      Subscribe getSpec() {
        return spec;
      }
    }

    private static class RedisCheckpointMark
        implements UnboundedSource.CheckpointMark, Serializable {

      Instant latestTimestamp = Instant.now();

      public void advanceWatermark(Instant time) {
        if (time.isAfter(latestTimestamp)) {
          latestTimestamp = time;
        }
      }

      @Override
      public void finalizeCheckpoint() throws IOException {
        latestTimestamp = Instant.now();
      }
    }

    private static class RedisUnboundedReader
        extends UnboundedSource.UnboundedReader<TimestampedValue<KV<String, String>>> {
      private static final Logger LOG = LoggerFactory.getLogger(RedisUnboundedReader.class);
      private final RedisSource source;
      private final RedisCheckpointMark checkpointMark;

      private final Counter elementsRead = SourceMetrics.elementsRead();
      private final Gauge backlog = SourceMetrics.backlogElements();

      private transient Jedis jedis;
      private transient JedisPubSub jedisPubSub =
          new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
              super.onMessage(channel, message);
              Instant timestamp = Instant.now();
              LOG.debug("(" + channel + ":" + timestamp + ") " + message);
              elementsRead.inc();
              waiting.add(TimestampedValue.of(KV.of(channel, message), timestamp));
              backlog.set(waiting.size());
            }
          };

      private transient ConcurrentLinkedQueue<TimestampedValue<KV<String, String>>> waiting =
          new ConcurrentLinkedQueue<>();
      private transient TimestampedValue<KV<String, String>> current;

      private RedisUnboundedReader(RedisSource source, RedisCheckpointMark checkpointMark) {
        this.source = source;
        this.checkpointMark = checkpointMark != null ? checkpointMark : new RedisCheckpointMark();
        this.current = null;
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized boolean start()
          throws @UnknownKeyFor @NonNull @Initialized IOException {
        jedis = source.getSpec().connectionConfiguration().connect();
        Thread subscriber =
            new Thread() {
              @Override
              public void run() {
                super.run();
                String[] channels = source.getSpec().channels().toArray(new String[0]);
                LOG.info("Starting subscriber for channels: " + String.join(",", channels));
                jedis.subscribe(jedisPubSub, channels);
                LOG.info("Subscription ended.");
              }
            };
        subscriber.start();
        return false;
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized boolean advance()
          throws @UnknownKeyFor @NonNull @Initialized IOException {
        current = waiting.poll();
        if (current == null) {
          checkpointMark.advanceWatermark(Instant.now());
          return false;
        }
        LOG.debug(
            "Advancing ("
                + current.getValue().getKey()
                + ":"
                + current.getTimestamp()
                + ") "
                + current.getValue().getValue());
        checkpointMark.advanceWatermark(current.getTimestamp());
        return true;
      }

      @Override
      public TimestampedValue<KV<String, String>> getCurrent()
          throws @UnknownKeyFor @NonNull @Initialized NoSuchElementException {
        if (current == null) {
          throw new NoSuchElementException();
        }
        return current;
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized Instant getCurrentTimestamp()
          throws @UnknownKeyFor @NonNull @Initialized NoSuchElementException {
        if (current == null) {
          throw new NoSuchElementException();
        }
        return current.getTimestamp();
      }

      @Override
      public void close() throws @UnknownKeyFor @NonNull @Initialized IOException {
        jedisPubSub.unsubscribe();
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized Instant getWatermark() {
        return checkpointMark.latestTimestamp;
      }

      @Override
      public UnboundedSource.@UnknownKeyFor @NonNull @Initialized CheckpointMark
          getCheckpointMark() {
        return checkpointMark;
      }

      @Override
      public @UnknownKeyFor @NonNull @Initialized UnboundedSource<
              TimestampedValue<KV<String, String>>, @UnknownKeyFor @NonNull @Initialized ?>
          getCurrentSource() {
        return source;
      }
    }
  }
}
