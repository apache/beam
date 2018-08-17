/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.benchmarks.euphoria.common.trends;

import cz.seznam.euphoria.core.util.Settings;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.net.URI;
import java.time.Duration;

@Builder
@Getter
@Setter
public class Parameters {
  private Duration longStats;
  private Duration shortStats;
  private int rankSmoothness;
  private double rankThreshold;

  private URI sourceUri;
  private URI sinkUri;

  private int parallelism;

  private Settings euphoriaSettings;
}
