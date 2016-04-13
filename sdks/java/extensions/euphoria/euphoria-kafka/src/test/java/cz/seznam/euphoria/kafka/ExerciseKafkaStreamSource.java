package cz.seznam.euphoria.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.ValueNode;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.PrintStreamSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Pair;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.util.Settings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class ExerciseKafkaStreamSource {

  public static void main(String[] args) throws Exception {
    Settings settings = new Settings();
    settings.setClass("euphoria.io.datasource.factory.kafka",
        KafkaStreamSource.Factory.class);

    Flow flow = Flow.create("Test", settings);

    // set-up our input source (a stream)
    Dataset<Pair<byte[], byte[]>> lines = flow.createInput(
        URI.create("kafka://ginkafka1.dev:9092/fulltext_robot_logs?groupId=quux?offset=latest"));

    // extract interesting words from the source
    Dataset<Pair<String, Long>> words = FlatMap.of(lines)
        .by((Pair<byte[], byte[]> p, Collector<Pair<String, Long>> c) -> {
          // ~ we're interested onlly
          Reader data = new InputStreamReader(
              new ByteArrayInputStream(p.getValue()),
              StandardCharsets.UTF_8);

          String out = null;
          try {
            JsonFactory jsonFactory = new MappingJsonFactory();
            JsonParser jsonParser = jsonFactory.createParser(data);
            TreeNode parsed = jsonParser.readValueAsTree();
            TreeNode typ_ = parsed.get("type");
            if (typ_ instanceof ValueNode) {
              out = ((ValueNode) typ_).asText();
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          if (out != null && !out.isEmpty()) {
            c.collect(Pair.of(out, 1L));
          }
        })
        .output();

    // reduce it to counts, use windowing
    Dataset<Pair<String, Long>> streamOutput = ReduceByKey
        .of(words)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy((Iterable<Long> values) -> {
          long s = 0;
          for (Long v : values) {
            s += v;
          }
          return s;
        })
        .windowBy(Windowing.Time.seconds(10).aggregating())
        .output();

    // produce the output
    streamOutput.persist(new PrintStreamSink<>(System.out, true));

    Executor executor = new InMemExecutor();
    executor.waitForCompletion(flow);
  }
}
