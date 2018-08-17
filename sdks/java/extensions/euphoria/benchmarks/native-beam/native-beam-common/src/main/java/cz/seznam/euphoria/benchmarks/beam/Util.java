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
package cz.seznam.euphoria.benchmarks.beam;

import cz.seznam.euphoria.benchmarks.datamodel.Benchmarks;
import cz.seznam.euphoria.benchmarks.datamodel.SearchEventsParser;
import org.apache.beam.runners.flink.translation.types.FlinkCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.hdfs.HDFSFileSink;
import org.apache.beam.sdk.io.hdfs.HDFSFileSource;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.Instant;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Util {

  static PCollection<Tuple2<Long, String>> createInput(Pipeline ppl, Parameters params) {
    boolean local = params.getBatch().getSourceHdfsUri() == null;
    boolean batch = true;

    if (local) {
      List<Tuple2<Long, String>> localInput = Benchmarks.testInput(Tuple2::of);
      return ppl.apply(Create.of(localInput)
          .withCoder(
              new FlinkCoder<>(new TypeHint<Tuple2<Long, String>>() {}.getTypeInfo(), new ExecutionConfig())))
          .apply(ParDo.of(new DoFn<Tuple2<Long, String>, Tuple2<Long, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              Tuple2<Long, String> t = c.element();
              c.outputWithTimestamp(Tuple2.of(t.f0, t.f1), new Instant(t.f0));
            }
          }))
          .setCoder(
              new FlinkCoder<>(new TypeHint<Tuple2<Long, String>>() {}.getTypeInfo(), new ExecutionConfig()));
    } else if (batch) {
      String inputUri = params.getBatch().getSourceHdfsUri().toString();
      Configuration conf = new Configuration();
      return ppl.apply(Read.from(HDFSFileSource.fromText(inputUri)))
          .apply("MapSource", ParDo.of(new DoFn<String, Tuple2<Long, String>>() {
            SearchEventsParser parser = new SearchEventsParser();
            @ProcessElement
            public void processElement(ProcessContext c) {
              try {
                SearchEventsParser.Query q = parser.parse(c.element());
                if (q != null && q.query != null && !q.query.isEmpty()) {
                  c.output(Tuple2.of(q.timestamp, q.query));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          }))
          .setCoder(
              new FlinkCoder<>(new TypeHint<Tuple2<Long, String>>() {}.getTypeInfo(), new ExecutionConfig()));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @SuppressWarnings("unchecked")
  static void output(PCollection<KV<Long, Tuple2<String, Double>>> max, Parameters params, String runner) {
    boolean batch = true;
    String path = createOutputPath(params, runner);
    SerializableFunction outputConverter =
        new SerializableFunction() {
          @Override
          public Object apply(Object input) {
            return KV.of(NullWritable.get(), new Text(input.toString()));
          }
        };
    if (batch) {
      HDFSFileSink sink = HDFSFileSink.builder()
        .setPath(path)
        .setFormatClass(TextOutputFormat.class)
        .setKeyClass(NullWritable.class)
        .setValueClass(Text.class)
        .setOutputConverter(outputConverter)
        .setConfiguration(new Configuration())
        .setUsername(null)
        .setValidate(true)
        .build();
      max.apply(Write.to(sink));
    } else {
      // print result
      max.apply(ParDo.of(new DoFn<KV<Long, Tuple2<String, Double>>, Void>() {
        @ProcessElement
        public void processElements(ProcessContext c) {
          System.out.println(c.element());
        }
      }));
    }
  }
  
  private static String createOutputPath(Parameters params, String runner) {
    String base = params.getBatch().getSinkHdfsBaseUri().toString();
    base = base.endsWith("/") ? base : base + "/";
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    return base + runner + "/" +  format.format(new Date());
  }
}
