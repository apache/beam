package cz.seznam.euphoria.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.ValueNode;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.core.util.Settings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class ExerciseKafkaSink {
  public static void main(String[] args) throws Exception {
    final Settings s = new Settings();
    s.setClass("euphoria.io.datasource.factory.kafka", KafkaSource.Factory.class);
    s.setClass("euphoria.io.datasink.factory.stdout", StdoutSink.Factory.class);
    s.setClass("euphoria.io.datasink.factory.kafka", KafkaSink.Factory.class);

    s.setBoolean("stdout.params.dump-partition-id", true);
    s.setString("kafka.params.fetch.min.bytes", "1024");
    s.setString("kafka.params.check.crcs", "false");

    Flow flow = Flow.create("test", s);
    Dataset<Pair<byte[], byte[]>> input =
        flow.createInput(URI.create("kafka://ginkafka1.dev:9092/fulltext_robot_logs?cfg=kafka.params"));
    FlatMap.of(input)
        .using((elem, collector) -> {
          Reader data = new InputStreamReader(
              new ByteArrayInputStream(elem.getValue()),
              StandardCharsets.UTF_8);

          boolean reemit = false;
          try {
            JsonFactory jsonFactory = new MappingJsonFactory();
            JsonParser jsonParser = jsonFactory.createParser(data);
            TreeNode parsed = jsonParser.readValueAsTree();
            TreeNode typ_ = parsed.get("type");
            if (typ_ instanceof ValueNode) {
              reemit = ((ValueNode) typ_).asText().startsWith("rssbot");
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          if (reemit) {
            collector.collect(elem);
          }
        })
        .output()
        .persist(URI.create("kafka://localhost:9092/my-topic-10p"));

    InMemExecutor exec = new InMemExecutor();
    exec.waitForCompletion(flow);
  }
}
