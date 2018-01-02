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
package cz.seznam.euphoria.benchmarks.model;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;

public class Benchmarks {
  
  public static double trendsRank(long longInterval, int longCount,
                                  long shortInterval, int shortCount,
                                  int smooth) {
    return ((double) shortCount / (longCount + smooth)) * ((double) longInterval/ shortInterval);
  }

  public static <P> List<P> testInput(BiFunction<Long, String, P> pairCtor) {
    List<P> partitions = new ArrayList<>();
    for(int i = 0; i < 15; i++) {
      int group = i%3;
      long time = i * 1000;
      partitions.add(pairCtor.apply(time, "G-" + group));
    }
    return partitions;
  }

  public static String createOutputPath(String outputUri, String runner) {
    outputUri = outputUri.endsWith("/") ? outputUri : outputUri + "/";
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HHmmss");
    return outputUri + runner + "/" +  format.format(new Date());
  }
}
