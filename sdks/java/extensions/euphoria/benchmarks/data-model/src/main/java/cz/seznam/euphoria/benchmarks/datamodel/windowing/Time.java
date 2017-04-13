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
package cz.seznam.euphoria.benchmarks.datamodel.windowing;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class Time implements Windowing {
  
  public static Time of(Duration duration) {
    return new Time(duration.toMillis());
  }
  
  private final long durationMillis;
  
  private Time(long durationMillis) {
    this.durationMillis = durationMillis;
  }

  @Override
  public List<Long> generate(long stamp) {
    long start = stamp - (stamp + durationMillis) % durationMillis;
    long end = start + durationMillis;
    return Collections.singletonList(end);
  }
}
