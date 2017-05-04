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
package cz.seznam.euphoria.benchmarks.flink;

import cz.seznam.euphoria.benchmarks.datamodel.Benchmarks;
import cz.seznam.euphoria.benchmarks.datamodel.SearchEventsParser;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.UUID;

class Util {

  static class TimeAssigner<T> extends BoundedOutOfOrdernessTimestampExtractor<Tuple2<Long, T>> {
    TimeAssigner(Time maxOutOfOrderness) {
      super(maxOutOfOrderness);
    }
    @Override
    public long extractTimestamp(Tuple2<Long, T> element) {
      return element.f0;
    }
  }

  static DataStream<Tuple2<Long, String>> addTestStreamSource(StreamExecutionEnvironment env) {
    return env.fromCollection(Benchmarks.testInput(Tuple2::of));
  }

  static DataSource<Tuple2<Long, String>> addTestBatchSource(ExecutionEnvironment env) {
    return env.fromCollection(Benchmarks.testInput(Tuple2::of));
  }

  static DataSet<Tuple2<Long, String>> getHdfsSource(ExecutionEnvironment env, URI inputPath)
  throws IOException {
    SearchEventsParser parser = new SearchEventsParser();
    return env.readHadoopFile(new TextInputFormat(), LongWritable.class, Text.class, inputPath.toString())
            .map(t -> t.f1.toString())
            .returns(TypeInformation.of(String.class))
            .map(parser::parse)
            .filter(q -> q != null && q.query != null && !q.query.isEmpty())
            .map(q -> Tuple2.of(q.timestamp, q.query))
            .returns(new TypeHint<Tuple2<Long, String>>() {});
  }

  static DataStream<Tuple2<Long, String>> addKafkaSource(
          StreamExecutionEnvironment env, String brokers, String topic) {
    // configure Kafka consumer
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", brokers); // host:port
    props.setProperty("group.id", "flink-stream-test" + UUID.randomUUID()); // unique group
    props.setProperty("auto.offset.reset", "earliest");// Read topic from start

    // create a Kafka consumer
    FlinkKafkaConsumer010<SearchEventsParser.Query> consumer =
            new FlinkKafkaConsumer010<>(topic, new ParseSchema(), props);
    return env.addSource(consumer)
            .filter(q -> q != null && q.query != null && !q.query.isEmpty())
            .map(q -> Tuple2.of(q.timestamp, q.query))
            .returns(new TypeHint<Tuple2<Long, String>>() {});
  }

  private static class ParseSchema implements DeserializationSchema<SearchEventsParser.Query> {
    private final SearchEventsParser parser = new SearchEventsParser();

    @Override
    public TypeInformation<SearchEventsParser.Query> getProducedType() {
      return TypeInformation.of(new TypeHint<SearchEventsParser.Query>(){});
    }

    @Override
    public SearchEventsParser.Query deserialize(byte[] message) throws IOException {
      try {
        return parser.parse(message);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean isEndOfStream(SearchEventsParser.Query nextElement) {
      return false;
    }
  }

  private Util() {}
}
