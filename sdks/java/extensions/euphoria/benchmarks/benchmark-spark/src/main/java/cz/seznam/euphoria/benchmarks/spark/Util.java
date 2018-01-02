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
package cz.seznam.euphoria.benchmarks.spark;

import cz.seznam.euphoria.benchmarks.model.Benchmarks;
import cz.seznam.euphoria.benchmarks.model.SearchEventsParser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URI;

class Util {

  static JavaRDD<Pair<Long, String>> getTestInput(JavaSparkContext sparkCtx) {
    return sparkCtx.parallelize(Benchmarks.testInput(Pair::of));
  }

  static JavaRDD<Pair<Long, String>> getHdfsSource(JavaSparkContext sc, URI inputPath)
  throws IOException {
    JavaRDD<String> input = sc.textFile(inputPath.toString());
    SearchEventsParser parser = new SearchEventsParser();
    return input.map(parser::parse)
        .filter(q -> q != null && q.query != null && !q.query.isEmpty())
        .map(q -> Pair.of(q.timestamp, q.query));
  }

  private Util() {}
}
