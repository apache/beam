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
package cz.seznam.euphoria.examples.wordcount;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
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
 * Simple aggregation of logs in Apache log format. Counts the number of daily hits per client. This
 * is a word-count like program utilizing time windowing based on event time.
 *
 * <p>If newly coming the euphoria API, you are advised to first study the {@link SimpleWordCount}
 * program.
 *
 * <p>Example usage on flink:
 *
 * <pre>{@code
 * $ flink run -m yarn-cluster \
 *    -yn 1 -ys 2 -ytm 800 \
 *    -c cz.seznam.euphoria.examples.wordcount.AccessLogCount \
 *    euphoria-examples/assembly/euphoria-examples.jar \
 *    "flink" \
 *    "hdfs:///tmp/access.log"
 * }</pre>
 *
 * Example usage on spark:
 *
 * <pre>{@code
 * $ spark-submit --verbose --deploy-mode cluster \
 *     --master yarn \
 *     --executor-memory 1g \
 *     --num-executors 1 \
 *     --class cz.seznam.euphoria.examples.wordcount.AccessLogCount \
 *     euphoria-examples/assembly/euphoria-examples.jar \
 *     "spark" \
 *     "hdfs:///tmp/access.log"
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

    // As with the {@code SimpleWordCount} we define a source to read data from ...
    final DataSource<String> source = new SimpleHadoopTextFileSource(inputPath);

    // ... and a sink to write the business logic's output to. In this particular
    // case we use a sink that eventually writes out the data to the executors
    // standard output. This is rarely useful in production environments but
    // is handy in local executions.
    final DataSink<String> sink = new StdoutSink<>();

    //  We start by allocating a new flow, a container to encapsulates the
    // chain of transformations.
    final Flow flow = Flow.create("Access log processor");

    // From the data source describing the actual input data location and
    // physical form, we create an abstract data set to be processed in the
    // context of the created flow.
    //
    // As in other examples, reading the actual input source is deferred
    // until the flow's execution. The data itself is _not_ touched at this
    // point in time yet.
    final Dataset<String> input = flow.createInput(source);

    // Build flow, that transforms our input to final dataset.
    final Dataset<String> output = buildFlow(input);

    // Persist final output to data sink.
    output.persist(sink);

    // Finally, we allocate an executor and submit our flow for execution on it.
    final Executor executor = Executors.createExecutor(executorName);

    executor.submit(flow).get();
  }

  static Dataset<String> buildFlow(Dataset<String> lines) {

    // We assume the actual input data to have a particular format; in this
    // case, each element is expected to be a log line from the Apache's access
    // log. We "map" the "parseLine" function over each such line to transform
    // the raw log entry into a more structured object.
    //
    // Note: Using `MapElements` implies that for each input we generate an
    // output. In the context of this program it means, that we are not able
    // to naturally "skip" invalid log lines.
    //
    // Note: Generally, user defined functions must be thread-safe. If you
    // inspect the `parseLine` function, you'll see that it allocates a new
    // `SimpleDateFormat` instance for every input element since sharing such
    // an instance between threads without explicit synchronization is not
    // thread-safe. (In this example we have intentionally used the
    // `SimpleDateFormat` to make this point. In a read-world program you
    // would probably hand out to `DateTimeFormatter` which can be safely
    // be re-used across threads.)
    final Dataset<LogLine> parsed =
        MapElements.named("LOG-PARSER").of(lines).using(LogParser::parseLine).output();

    // Since our log lines represent events which happened at a particular
    // point in time, we want our system to treat them as such, no matter in
    // which particular order the lines happen to be in the read input files.
    //
    // We do so by applying a so-called event-time-extractor function. As of
    // this moment, euphoria will treat the element as if it happended in the
    // corresponding time since it gets to know the timestamp the event occurred.
    final Dataset<LogLine> parsedWithEventTime =
        AssignEventTime.of(parsed).using(line -> line.getDate().getTime()).output();

    // In the previous step we derived a data set specifying points in time
    // at which particular IPs accessed our web-server. Our goal is now to
    // count how often a particular IP accessed the web-server, per day. This
    // is, instead of deriving the count of a particular IP from the whole
    // input, we want to know the number of hits per IP for every day
    // distinctly (we're not interested in zero hit counts, of course.)
    //
    // Actually, this computation is merely a word-count problem explained
    // in the already mentioned {@link SimpleWordCount}. We just count the
    // number of occurrences of a particular IP. However, we also specify
    // how the input is to be "windowed."
    //
    // Windowing splits the input into fixed sections of chunks. Such as we
    // can divide a data set into chunks by a certain size, we can split
    // a data set into chunks defined by time, e.g. a chunk for day one,
    // another chunk for day two, etc. provided that elements of the data
    // set have a notion of time. Once the input data set is logically divided
    // into these "time windows", the computation takes place separately on
    // each of them, and, produces a results for each window separately.
    //
    // Here, we specify time based windowing using the `Time.of(..)` method
    // specifying the size of the windows, in particular "one day" in this
    // example. The assignment of an element to a particular time window,
    // will, by definition, utilize the processed elements assigned timestamp.
    // This is what we did in the previous step.
    //
    // Note: There are a few different windowing strategies and you can
    // investigate each by looking for classes implementing {@link Windowing}.
    //
    // Note: You might wonder why we didn't just a
    // "select ip, count(*) from input group by (ip, day)". First, windowing
    // as such is a separate concern to the actual computation; there is no
    // need to mix them up and further complicate the actual computation.
    // Being a separate concern it allows for easier exchange and
    // experimentation. Second, by specifying windowing as a separate concern,
    // we can make the computation work even on unbounded, i.e. endless, input
    // streams. Windowing strategies generally work together with the
    // executor and can define a point when a window is determined to be
    // "filled" at which point the windows data can be processed, calculated,
    // and the corresponding results emitted. This makes endless stream
    // processing work.
    final Dataset<Pair<String, Long>> aggregated =
        ReduceByKey.named("AGGREGATE")
            .of(parsedWithEventTime)
            .keyBy(LogLine::getIp)
            .valueBy(line -> 1L)
            .combineBy(Sums.ofLongs())
            .windowBy(Time.of(Duration.ofDays(1)))
            .output();

    // At the final stage of our flow, we nicely format the previously emitted
    // results before persisting them to a given data sink, e.g. external storage.
    //
    // The elements emitted from the previous operator specify the windowed
    // results of the "IP-count". This is, for each IP we get a count of the
    // number of its occurrences (within a window.) The window information
    // itself - if desired - can be accessed from the `FlatMap`'s context
    // parameter as demonstrated below.
    return FlatMap.named("FORMAT-OUTPUT")
        .of(aggregated)
        .using(
            ((Pair<String, Long> elem, Collector<String> context) -> {
              Date d = new Date(((TimeInterval) context.getWindow()).getStartMillis());

              SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);
              context.collect(sdf.format(d) + "\t" + elem.getFirst() + "\t" + elem.getSecond());
            }))
        .output();
  }

  private static class LogParser {

    private static Pattern pattern =
        Pattern.compile("^([[0-9a-zA-z-].]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\].*");

    static LogLine parseLine(String line) {

      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        try {
          // SDF is not thread-safe, so we need to allocate one here. Ideally,
          // we'd use `DateTimeFormatter` and re-use it across input elements.
          // see the corresponding note at the operator utilizing `parseLine`.
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

    LogLine(String ip, Date date) {
      this.ip = ip;
      this.date = date;
    }

    String getIp() {
      return ip;
    }

    Date getDate() {
      return date;
    }
  }
}
