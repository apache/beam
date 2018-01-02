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
package cz.seznam.euphoria.benchmarks.beam;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import lombok.Builder;
import lombok.Getter;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Builder
@Getter
class Parameters {

  @Builder
  @Getter
  static class Batch {
    private URI sourceHdfsUri;
    private URI sinkHdfsBaseUri;
  }

  private Duration longStats;
  private Duration shortStats;
  private int rankSmoothness;
  private double rankThreshold;

  private Batch batch;

  static Parameters fromFile(String file) {
    Config cfg = ConfigFactory.defaultOverrides()
                     .withFallback(ConfigFactory.parseFileAnySyntax(
                         new File(file), ConfigParseOptions.defaults().setAllowMissing(false)))
                     .withFallback(ConfigFactory.defaultReference())
                     .resolve();

    cfg = cfg.getConfig("trends");

    Batch batch = cfg.hasPath("batch")
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
               .batch(batch)
               .build();
  }

  private static Duration cfgDuration(Config cfg, String path) {
    return Duration.ofMillis(cfg.getDuration(path, TimeUnit.MILLISECONDS));
  }
}
