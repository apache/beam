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

package cz.seznam.euphoria.examples;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import static cz.seznam.euphoria.examples.Utils.getPath;
import static cz.seznam.euphoria.examples.Utils.getZnodeParent;
import static cz.seznam.euphoria.examples.Utils.toCell;
import cz.seznam.euphoria.hadoop.input.SequenceFileSource;
import cz.seznam.euphoria.hbase.HBaseSource;
import cz.seznam.euphoria.hbase.HFileSink;
import cz.seznam.euphoria.hbase.util.ResultUtil;
import cz.seznam.euphoria.kafka.KafkaSource;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import static cz.seznam.euphoria.examples.Utils.getHBaseSource;
import cz.seznam.euphoria.spark.SparkExecutor;
import java.util.stream.Stream;

/**
 * Load data to HBase from various sources and in various bulks.
 */
public class HFileLoader {

  public static void main(String[] args) throws URISyntaxException, IOException {
    Preconditions.checkArgument(
        args.length >= 3, "Please specify <input_uri> <output_uri> <executor> [<tmp_dir>]");

    URI input = new URI(args[0]);
    URI output = new URI(args[1]);
    Executor executor = Executors.createExecutor(
        args[2],
        KeyValue.class);
    String tmp = "/tmp/hfileloader";
    if (args.length > 3) {
      tmp = args[2];
    }
    HFileLoader app = new HFileLoader(input, output, tmp, executor);
    app.run();
  }

  private final Configuration conf = HBaseConfiguration.create();
  private final String table;
  private final String outputQuorum;
  private final String tmpDir;
  private final URI input;
  private final URI output;
  private final Executor executor;
  private final Flow flow;

  HFileLoader(URI input, URI output, String tmpDir, Executor executor) {
    this.table = getPath(output);
    this.outputQuorum = output.getAuthority();
    this.tmpDir = tmpDir;
    this.input = input;
    this.output = output;
    this.executor = executor;
    this.flow = Flow.create();
  }

  @SuppressWarnings("unchecked")
  private void run() {
    final Dataset<Cell> ds;
    flow.getSettings().setInt(
        "euphoria.flink.batch.list-storage.max-memory-elements", 100);
    switch (input.getScheme()) {
      case "kafka":
      {
        Settings settings = new Settings();
        settings.setInt(KafkaSource.CFG_RESET_OFFSET_TIMESTAMP_MILLIS, 0);
        Dataset<Pair<byte[], byte[]>> raw = flow.createInput(
            new KafkaSource(input.getAuthority(),
                getPath(input),
                settings));
        ds = MapElements.of(raw)
            .using(p -> toCell(p.getSecond()))
            .output();
        break;
      }
      case "hdfs":
      case "file":
      {
        Dataset<Pair<ImmutableBytesWritable, Cell>> raw = flow.createInput(
            new SequenceFileSource<>(ImmutableBytesWritable.class,
                (Class) KeyValue.class, input.toString()));
        ds = MapElements.of(raw)
            .using(Pair::getSecond)
            .output();
        break;
      }
      case "hbase":
      {
        HBaseSource source = getHBaseSource(input, conf);
        Dataset<Pair<ImmutableBytesWritable, Result>> raw = flow.createInput(
            source);

        Dataset<Cell> tmp = FlatMap.of(raw)
            .using(ResultUtil.toCells())
            .output();

        if (executor instanceof SparkExecutor) {
          tmp = ReduceByKey.of(tmp)
              .keyBy(Object::hashCode)
              .reduceBy((Stream<Cell> s, Collector<Cell> ctx) -> s.forEach(ctx::collect))
              .outputValues();
        }

        ds = Filter.of(tmp)
            .by(c -> c.getValueLength() < 1024 * 1024)
            .output();

        break;
      }

      default:
        throw new IllegalArgumentException("Don't know how to load " + input);
    }

    ds.persist(HFileSink.newBuilder()
        .withConfiguration(conf)
        .withZookeeperQuorum(outputQuorum)
        .withZnodeParent(getZnodeParent(output))
        .withTable(table)
        .withOutputPath(new Path(tmpDir))
        .applyIf(ds.isBounded(),
            b -> b.windowBy(GlobalWindowing.get(), w -> ""),
            b -> b.windowBy(
                Time.of(Duration.ofMinutes(5)), w -> String.valueOf(w.getStartMillis())))
        .build());

    executor.submit(flow).join();
  }


}
