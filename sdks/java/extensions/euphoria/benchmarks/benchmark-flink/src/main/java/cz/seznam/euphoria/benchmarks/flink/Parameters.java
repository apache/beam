/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.benchmarks.flink;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;

@Builder
@Getter
class Parameters {

  @Builder
  @Getter
  static class Stream {

    @Builder
    @Getter
    static class Source {
      private String kafkaBrokers;
      private String kafkaTopic;
    }

    private Source source;

    private URI rocksDbCheckpoint;
    private Time watermarkInterval;
    private Time allowedLateness;
  }

  @Builder
  @Getter
  static class Batch {
    private URI sourceHdfsUri;
    private URI sinkHdfsBaseUri;
  }

  enum RequireSection {
    STREAM, BATCH;
  }

  private Time longStats;
  private Time shortStats;
  private int rankSmoothness;
  private double rankThreshold;

  private Stream stream;
  private Batch batch;

  static Parameters fromFile(String file, RequireSection require) {
    Config cfg = ConfigFactory.defaultOverrides()
                     .withFallback(ConfigFactory.parseFileAnySyntax(
                         new File(file), ConfigParseOptions.defaults().setAllowMissing(false)))
                     .withFallback(ConfigFactory.defaultReference())
                     .resolve();

    cfg = cfg.getConfig("trends");
    Stream stream = require == RequireSection.STREAM || cfg.hasPath("stream")
                        ? Stream.builder()
                              .source(
                                  cfg.hasPath("stream.source")
                                      ? Stream.Source.builder()
                                            .kafkaBrokers(cfg.getString("stream.source.kafka-brokers"))
                                            .kafkaTopic(cfg.getString("stream.source.kafka-topic"))
                                            .build()
                                      : null)
                              .rocksDbCheckpoint(
                                  cfg.hasPath("stream.rocksdb-checkpoint")
                                      ? URI.create(cfg.getString("stream.rocksdb-checkpoint"))
                                      : null)
                              .allowedLateness(cfgDuration(cfg, "stream.watermark-interval"))
                              .watermarkInterval(cfgDuration(cfg, "stream.watermark-interval"))
                              .build()
                        : null;

    Batch batch = require == RequireSection.BATCH || cfg.hasPath("batch")
                      ? Batch.builder()
                            .sourceHdfsUri(cfg.hasPath("batch.source-hdfs-uri")
                                               ? URI.create(cfg.getString("batch.source-hdfs-uri"))
                                               : null)
                            .sinkHdfsBaseUri(URI.create(cfg.getString("batch.sink-hdfs-base-uri")))
                            .build()
                      : null;

    return Parameters.builder()
               .longStats(cfgDuration(cfg, "long-stats-duration"))
               .shortStats(cfgDuration(cfg, "short-stats-duration"))
               .rankSmoothness(cfg.getInt("rank-smoothness"))
               .rankThreshold(cfg.getDouble("rank-threshold"))
               .stream(stream)
               .batch(batch)
               .build();
  }

  private static Time cfgDuration(Config cfg, String path) {
    return Time.milliseconds(cfg.getDuration(path, TimeUnit.MILLISECONDS));
  }
}
