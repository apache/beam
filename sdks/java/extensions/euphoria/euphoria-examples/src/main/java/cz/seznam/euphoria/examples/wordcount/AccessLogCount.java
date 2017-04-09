package cz.seznam.euphoria.examples.wordcount;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.examples.Executors;
import cz.seznam.euphoria.hadoop.input.SimpleHadoopTextFileSource;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple aggregation of logs in Apache log format.
 * Counts number of daily hits per client.
 * <p>
 *
 * Example usage on flink:
 * <pre>{@code
 *   $ flink run -m yarn-cluster \
 *      -yn 1 -ys 2 -ytm 800 \
 *      -c cz.seznam.euphoria.examples.wordcount.AccessLogCount \
 *      euphoria-examples/assembly/euphoria-examples.jar \
 *      "flink" \
 *      "hdfs:///tmp/access.log"
 *}</pre>
 *
 * Example usage on spark:
 * <pre>{@code
 *   $ spark-submit --verbose --deploy-mode cluster \
 *       --master yarn \
 *       --executor-memory 1g \
 *       --num-executors 1 \
 *       --class cz.seznam.euphoria.examples.wordcount.AccessLogCount \
 *       euphoria-examples/assembly/euphoria-examples.jar \
 *       "spark" \
 *       "hdfs:///tmp/access.log"
 * }</pre>
 */
public class AccessLogCount {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: " + AccessLogCount.class + " <executor-name> <input-path>");
      System.exit(1);
    }

    final String executorName = args[0];
    final String inputPath = args[1];

    DataSource<String> dataSource = new SimpleHadoopTextFileSource(inputPath);
    DataSink<String> dataSink = new StdoutSink<>();

    Flow flow = Flow.create("Access log processor");

    Dataset<String> input = flow.createInput(dataSource);

    Dataset<LogLine> parsed = MapElements.named("LOG-PARSER")
            .of(input)
            .using(LogParser::parseLine)
            .output();

    Dataset<Pair<String, Long>> aggregated = ReduceByKey.named("AGGREGATE")
            .of(parsed)
            .keyBy(LogLine::getIp)
            .valueBy(line -> 1L)
            .combineBy(Sums.ofLongs())
            .windowBy(Time.of(Duration.ofDays(1)), line -> line.getDate().getTime())
            .output();

    FlatMap.named("FORMAT-OUTPUT")
            .of(aggregated)
            .using(((Pair<String, Long> elem, Context<String> context) -> {
              Date d = new Date(((TimeInterval) context.getWindow()).getStartMillis());

              SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);
              context.collect(sdf.format(d) + "\t" + elem.getFirst() + "\t" + elem.getSecond());
            }))
            .output()
            .persist(dataSink);

    Executor executor = Executors.createExecutor(executorName);
    executor.submit(flow).get();
  }

  private static class LogParser {
    private static Pattern pattern = Pattern.compile("^([[0-9a-zA-z-].]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\].*");

    public static LogLine parseLine(String line) {

      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        try {
          SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

          String ip = matcher.group(1);
          Date date = sdf.parse(matcher.group(4));

          return new LogLine(ip, date);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }

      throw new IllegalStateException("Invalid log format: " + line);
    }
  }

  private static class LogLine {
    private final String ip;
    private final Date date;

    public LogLine(String ip, Date date) {
      this.ip = ip;
      this.date = date;
    }

    public String getIp() {
      return ip;
    }

    public Date getDate() {
      return date;
    }
  }
}
