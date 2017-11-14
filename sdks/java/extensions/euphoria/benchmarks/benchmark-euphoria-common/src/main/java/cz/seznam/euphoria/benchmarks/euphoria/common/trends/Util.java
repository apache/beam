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

import cz.seznam.euphoria.benchmarks.model.Benchmarks;
import cz.seznam.euphoria.benchmarks.model.SearchEventsParser;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.io.VoidSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.input.SimpleHadoopTextFileSource;
import cz.seznam.euphoria.hadoop.output.SimpleHadoopTextFileSink;
import cz.seznam.euphoria.kafka.KafkaSource;

import java.net.URI;

class Util {

  static Dataset<Pair<Long, String>> getTestInput(Flow flow) {
    ListDataSource<Pair<Long, String>> source = ListDataSource.bounded(
        Benchmarks.testInput(Pair::of));
    return flow.createInput(source);
  }

  static Dataset<Pair<Long, String>> getInput(
      URI uri, Settings settings, Flow flow)
      throws Exception {

    switch (uri.getScheme()) {
      case "kafka": {
        Dataset<Pair<byte[], byte[]>> input = flow.createInput(
            new KafkaSource(uri.getAuthority(),
                uri.getPath().substring(1), settings));
        return FlatMap.of(input)
            .using(new UnaryFunctor<Pair<byte[], byte[]>, Pair<Long, String>>() {
              private final SearchEventsParser parser = new SearchEventsParser();
              @Override
              public void apply(Pair<byte[], byte[]> pair, Collector<Pair<Long, String>> context) {
                try {
                  SearchEventsParser.Query q = parser.parse(pair.getSecond());
                  if (q != null && q.query != null && !q.query.isEmpty()) {
                    context.collect(Pair.of(q.timestamp, q.query));
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            })
            .output();
      }
      case "hdfs":
      case "file":
      case "hftp": {
        DataSource<String> datasource = new SimpleHadoopTextFileSource(uri.toString());
        Dataset<String> in = flow.createInput(datasource);
        return FlatMap.named("PARSE-INPUT")
            .of(in)
            .using(new UnaryFunctor<String, Pair<Long, String>>() {
              SearchEventsParser parser = new SearchEventsParser();
              @Override
              public void apply(String line, Collector<Pair<Long, String>> context) {
                try {
                  SearchEventsParser.Query q = parser.parse(line);
                  if (q != null && q.query != null && !q.query.isEmpty()) {
                    context.collect(Pair.of(q.timestamp, q.query));
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            })
            .output();
      }
      default: {
        throw new IllegalArgumentException("Unknown scheme: " + uri.getScheme());
      }
    }
  }

  static DataSink<String> getStringSink(Parameters params) {
    if (params.getSinkUri() == null) {
      return new VoidSink<>();
    }

    switch (params.getSinkUri().getScheme()) {
      case "stdout":
        return new StdoutSink<>();

      case "file":
      case "hftp":
      case "hdfs":
        return new SimpleHadoopTextFileSink<>(params.getSinkUri().toString());
    }

    throw new IllegalArgumentException(
        "Unsopported scheme in " + params.getSinkUri());
  }

}
