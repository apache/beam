---
layout: default
title: "Beam Mobile Gaming Example"
permalink: /get-started/mobile-gaming-example/
redirect_from: /use/mobile-gaming-example/
---

# Apache Beam Mobile Gaming Pipeline Examples

* TOC
{:toc}

<nav class="language-switcher">
  <strong>Adapt for:</strong> 
  <ul>
    <li data-type="language-java">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>

This section provides a walkthrough of a series of example Apache Beam pipelines that demonstrate more complex functionality than the basic [WordCount]({{ site.baseurl }}/get-started/wordcount-example) examples. The pipelines in this section process data from a hypothetical game that users play on their mobile phones. The pipelines demonstrate processing at increasing levels of complexity; the first pipeline, for example, shows how to run a batch analysis job to obtain relatively simple score data, while the later pipelines use Beam's windowing and triggers features to provide low-latency data analysis and more complex intelligence about user's play patterns.

{:.language-java}
> **Note**: These examples assume some familiarity with the Beam programming model. If you haven't already, we recommend familiarizing yourself with the programming model documentation and running a basic example pipeline before continuing. Note also that these examples use the Java 8 lambda syntax, and thus require Java 8. However, you can create pipelines with equivalent functionality using Java 7. 

{:.language-py}
> **Note**: These examples assume some familiarity with the Beam programming model. If you haven't already, we recommend familiarizing yourself with the programming model documentation and running a basic example pipeline before continuing.

Every time a user plays an instance of our hypothetical mobile game, they generate a data event. Each data event consists of the following information:

- The unique ID of the user playing the game.
- The team ID for the team to which the user belongs.
- A score value for that particular instance of play.
- A timestamp that records when the particular instance of play happened--this is the event time for each game data event.

When the user completes an instance of the game, their phone sends the data event to a game server, where the data is logged and stored in a file. Generally the data is sent to the game server immediately upon completion. However, sometimes delays happen in the network or users play the game "offline", when their phones are out of contact with the server (such as on an airplane, or outside network coverage area). When the user's phone comes back into contact with the game server, the phone will send all accumulated game data. This means that some data events may arrive delayed and out of order. 

The following diagram shows the ideal situation vs reality. The X-axis represents event time: the actual time a game event occurred. The Y-axis represents processing time: the time at which a game event was processed. Ideally, events should be processed as they occur, depicted by the dotted line in the diagram. However, in reality that is not the case and reality looks more like what is depicted by the red squiggly line.

<figure id="fig1">
    <img src="{{ site.baseurl }}/images/gaming-example-basic.png"
         width="264" height="260"
         alt="Score data for three users.">
</figure>
Figure 1: Ideally, events are processed when they occur, with no delays.

The data events might be received by the game server significantly later than users generate them. This time difference (called **skew**) can have processing implications for pipelines that make calculations that consider when each score was generated. Such pipelines might track scores generated during each hour of a day, for example, or they calculate the length of time that users are continuously playing the game—both of which depend on each data record's event time.

Because some of our example pipelines use data files (like logs from the game server) as input, the event timestamp for each game might be embedded in the data--that is, it's a field in each data record. Those pipelines need to parse the event timestamp from each data record after reading it from the input file.

For pipelines that read unbounded game data from an unbounded source, the data source sets the intrinsic [timestamp]({{ site.baseurl }}/documentation/programming-guide/#pctimestamps) for each PCollection element to the appropriate event time.

The Mobile Game example pipelines vary in complexity, from simple batch analysis to more complex pipelines that can perform real-time analysis and abuse detection. This section walks you through each example and demonstrates how to use Beam features like windowing and triggers to expand your pipeline's capabilites.

## UserScore: Basic Score Processing in Batch

The `UserScore` pipeline is the simplest example for processing mobile game data. `UserScore` determines the total score per user over a finite data set (for example, one day's worth of scores stored on the game server). Pipelines like `UserScore` are best run periodically after all relevant data has been gathered. For example, `UserScore` could run as a nightly job over data gathered during that day.

{:.language-java}
> **Note:** See [UserScore on GitHub](https://github.com/apache/beam/blob/master/examples/java8/src/main/java/org/apache/beam/examples/complete/game/UserScore.java) for the complete example pipeline program.

{:.language-py}
> **Note:** See [UserScore on GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/user_score.py) for the complete example pipeline program.

### What Does UserScore Do?

In a day's worth of scoring data, each user ID may have multiple records (if the user plays more than one instance of the game during the analysis window), each with their own score value and timestamp. If we want to determine the total score over all the instances a user plays during the day, our pipeline will need to group all the records together per individual user.

As the pipeline processes each event, the event score gets added to the sum total for that particular user.

`UserScore` parses out only the data that it needs from each record, specifically the user ID and the score value. The pipeline doesn't consider the event time for any record; it simply processes all data present in the input files that you specify when you run the pipeline.

> **Note:** To use the `UserScore` pipeline effectively, you'd need to ensure that you supply input data that has already been grouped by the desired event time period — that is, that you specify an input file that only contains data from the day you care about.

`UserScore`'s basic pipeline flow does the following:

1. Read the day's score data from a file stored in a text file.
2. Sum the score values for each unique user by grouping each game event by user ID and combining the score values to get the total score for that particular user.
3. Write the result data to a [Google Cloud BigQuery](https://cloud.google.com/bigquery/) table.

The following diagram shows score data for several users over the pipeline analysis period. In the diagram, each data point is an event that results in one user/score pair:

<figure id="fig2">
    <img src="{{ site.baseurl }}/images/gaming-example.gif"
         width="900" height="263"
         alt="Score data for three users.">
</figure>
Figure 2: Score data for three users.

This example uses batch processing, and the diagram's Y axis represents processing time: the pipeline processes events lower on the Y-axis first, and events higher up the axis later. The diagram's X axis represents the event time for each game event, as denoted by that event's timestamp. Note that the individual events in the diagram are not processed by the pipeline in the same order as they occurred (according to their timestamps).

After reading the score events from the input file, the pipeline groups all of those user/score pairs together and sums the score values into one total value per unique user. `UserScore` encapsulates the core logic for that step as the [user-defined composite transform]({{ site.baseurl }}/documentation/programming-guide/#transforms-composite) `ExtractAndSumScore`:

```java
public static class ExtractAndSumScore
    extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

  private final String field;

  ExtractAndSumScore(String field) {
    this.field = field;
  }

  @Override
  public PCollection<KV<String, Integer>> expand(
      PCollection<GameActionInfo> gameInfo) {

    return gameInfo
      .apply(MapElements
          .into(
              TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
          .via((GameActionInfo gInfo) -> KV.of(gInfo.getKey(field), gInfo.getScore())))
      .apply(Sum.<String>integersPerKey());
  }
}
```

```py
class ExtractAndSumScore(beam.PTransform):
  """A transform to extract key/score information and sum the scores.
  The constructor argument `field` determines whether 'team' or 'user' info is
  extracted.
  """
  def __init__(self, field):
    super(ExtractAndSumScore, self).__init__()
    self.field = field

  def expand(self, pcoll):
    return (pcoll
            | beam.Map(lambda info: (info[self.field], info['score']))
            | beam.CombinePerKey(sum_ints))

def configure_bigquery_write():
  return [
      ('user', 'STRING', lambda e: e[0]),
      ('total_score', 'INTEGER', lambda e: e[1]),
  ]
```

`ExtractAndSumScore` is written to be more general, in that you can pass in the field by which you want to group the data (in the case of our game, by unique user or unique team). This means we can re-use `ExtractAndSumScore` in other pipelines that group score data by team, for example.

Here's the main method of `UserScore`, showing how we apply all three steps of the pipeline:

```java
public static void main(String[] args) throws Exception {
  // Begin constructing a pipeline configured by commandline flags.
  Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
  Pipeline pipeline = Pipeline.create(options);

  // Read events from a text file and parse them.
  pipeline.apply(TextIO.read().from(options.getInput()))
    .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
    // Extract and sum username/score pairs from the event data.
    .apply("ExtractUserScore", new ExtractAndSumScore("user"))
    .apply("WriteUserScoreSums",
        new WriteToBigQuery<KV<String, Integer>>(options.getTableName(),
                                                 configureBigQueryWrite()));

  // Run the batch pipeline.
  pipeline.run().waitUntilFinish();
}
```

```py
def run(argv=None):
  """Main entry point; defines and runs the user_score pipeline."""
  
  ...

  pipeline_options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=pipeline_options)

  (p  # pylint: disable=expression-not-assigned
   | ReadFromText(known_args.input) # Read events from a file and parse them.
   | UserScore()
   | WriteToBigQuery(
       known_args.table_name, known_args.dataset, configure_bigquery_write()))

  result = p.run()
  result.wait_until_finish()
```

### Working with the Results

`UserScore` writes the data to a BigQuery table (called `user_score` by default). With the data in the BigQuery table, we might perform a further interactive analysis, such as querying for a list of the N top-scoring users for a given day.

Let's suppose we want to interactively determine the top 10 highest-scoring users for a given day. In the BigQuery user interface, we can run the following query:

```
SELECT * FROM [MyGameProject:MyGameDataset.user_score] ORDER BY total_score DESC LIMIT 10
```

### Limitations

As written in the example, the `UserScore` pipeline has a few limitations:

* Because some score data may be generated by offline players and sent after the daily cutoff, for game data, the result data generated by the `UserScore` pipeline **may be incomplete**. `UserScore` only processes the fixed input set present in the input file(s) when the pipeline runs.

* `UserScore` processes all data events present in the input file at processing time, and **does not examine or otherwise error-check events based on event time**. Therefore, the results may include some values whose event times fall outside the relevant analysis period, such as late records from the previous day.

* Because `UserScore` runs only after all the data has been collected, it has **high latency** between when users generate data events (the event time) and when results are computed (the processing time).

* `UserScore` also only reports the total results for the entire day, and doesn't provide any finer-grained information about how the data accumulated during the day.

Starting with the next pipeline example, we'll discuss how you can use Beam's features to address these limitations.

## HourlyTeamScore: Advanced Processing in Batch with Windowing

The `HourlyTeamScore` pipeline expands on the basic batch analysis principles used in the `UserScore` pipeline and improves upon some of its limitations. `HourlyTeamScore` performs finer-grained analysis, both by using additional features in the Beam SDKs, and taking into account more aspects of the game data. For example, `HourlyTeamScore` can filter out data that isn't part of the relevant analysis period.

Like `UserScore`, `HourlyTeamScore` is best thought of as a job to be run periodically after all the relevant data has been gathered (such as once per day). The pipeline reads a fixed data set from a file, and writes the results to a Google Cloud BigQuery table, just like `UserScore`.

{:.language-java}
> **Note:** See [HourlyTeamScore on GitHub](https://github.com/apache/beam/blob/master/examples/java8/src/main/java/org/apache/beam/examples/complete/game/HourlyTeamScore.java) for the complete example pipeline program.

{:.language-py}
> **Note:** See [HourlyTeamScore on GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/hourly_team_score.py) for the complete example pipeline program.

### What Does HourlyTeamScore Do?

`HourlyTeamScore` calculates the total score per team, per hour, in a fixed data set (such as one day's worth of data).

* Rather than operating on the entire data set at once, `HourlyTeamScore` divides the input data into logical windows and performs calculations on those windows. This allows `HourlyUserScore` to provide information on scoring data per window, where each window represents the game score progress at fixed intervals in time (like once every hour).

* `HourlyTeamScore` filters data events based on whether their event time (as indicated by the embedded timestamp) falls within the relevant analysis period. Basically, the pipeline checks each game event's timestamp and ensures that it falls within the range we want to analyze (in this case the day in question). Data events from previous days are discarded and not included in the score totals. This makes `HourlyTeamScore` more robust and less prone to erroneous result data than `UserScore`. It also allows the pipeline to account for late-arriving data that has a timestamp within the relevant analysis period.

Below, we'll look at each of these enhancements in `HourlyTeamScore` in detail:

#### Fixed-Time Windowing

Using fixed-time windowing lets the pipeline provide better information on how events accumulated in the data set over the course of the analysis period. In our case, it tells us when in the day each team was active and how much the team scored at those times.

The following diagram shows how the pipeline processes a day's worth of a single team's scoring data after applying fixed-time windowing:

<figure id="fig3">
    <img src="{{ site.baseurl }}/images/gaming-example-team-scores-narrow.gif"
         width="900" height="390"
         alt="Score data for two teams.">
</figure>
Figure 3: Score data for two teams. Each team's scores are divided into logical windows based on when those scores occurred in event time.

Notice that as processing time advances, the sums are now _per window_; each window represents an hour of _event time_ during the day in which the scores occurred.

> **Note:** As is shown in the diagram above, using windowing produces an _independent total for every interval_ (in this case, each hour). `HourlyTeamScore` doesn't provide a running total for the entire data set at each hour--it provides the total score for all the events that occurred _only within that hour_.

Beam's windowing feature uses the [intrinsic timestamp information]({{ site.baseurl }}/documentation/programming-guide/#pctimestamps) attached to each element of a `PCollection`. Because we want our pipeline to window based on _event time_, we **must first extract the timestamp** that's embedded in each data record apply it to the corresponding element in the `PCollection` of score data. Then, the pipeline can **apply the windowing function** to divide the `PCollection` into logical windows.

{:.language-java}
`HourlyTeamScore` uses the [WithTimestamps](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/WithTimestamps.java) and [Window](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/Window.java) transforms to perform these operations.

{:.language-py}
`HourlyTeamScore` uses the `FixedWindows` transform, found in [window.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/window.py), to perform these operations.

The following code shows this: 

```java
// Add an element timestamp based on the event log, and apply fixed windowing.
    .apply("AddEventTimestamps",
           WithTimestamps.of((GameActionInfo i) -> new Instant(i.getTimestamp())))
    .apply("FixedWindowsTeam", Window.<GameActionInfo>into(
        FixedWindows.of(Duration.standardMinutes(options.getWindowDuration()))))
```

```py
# Add an element timestamp based on the event log, and apply fixed windowing.
# Convert element['timestamp'] into seconds as expected by TimestampedValue.
| 'AddEventTimestamps' >> beam.Map(
    lambda element: TimestampedValue(
        element, element['timestamp'] / 1000.0))
# Convert window_duration into seconds as expected by FixedWindows.
| 'FixedWindowsTeam' >> beam.WindowInto(FixedWindows(
    size=self.window_duration * 60))
```

Notice that the transforms the pipeline uses to specify the windowing are distinct from the actual data processing transforms (such as `ExtractAndSumScores`). This functionality provides you some flexibility in designing your Beam pipeline, in that you can run existing transforms over datasets with different windowing characteristics.

#### Filtering Based On Event Time

`HourlyTeamScore` uses **filtering** to remove any events from our dataset whose timestamps don't fall within the relevant analysis period (i.e. they weren't generated during the day that we're interested in). This keeps the pipeline from erroneously including any data that was, for example, generated offline during the previous day but sent to the game server during the current day.

It also lets the pipeline include relevant **late data**—data events with valid timestamps, but that arrived after our analysis period ended. If our pipeline cutoff time is 12:00 am, for example, we might run the pipeline at 2:00 am, but filter out any events whose timestamps indicate that they occurred after the 12:00 am cutoff. Data events that were delayed and arrived between 12:01 am and 2:00 am, but whose timestamps indicate that they occurred before the 12:00 am cutoff, would be included in the pipeline processing.

`HourlyTeamScore` uses the `Filter` transform to perform this operation. When you apply `Filter`, you specify a predicate to which each data record is compared. Data records that pass the comparison are included, while events that fail the comparison are excluded. In our case, the predicate is the cut-off time we specify, and we compare just one part of the data—the timestamp field.

The following code shows how `HourlyTeamScore` uses the `Filter` transform to filter events that occur either before or after the relevant analysis period:

```java
.apply("FilterStartTime", Filter.by(
    (GameActionInfo gInfo)
        -> gInfo.getTimestamp() > startMinTimestamp.getMillis()))
.apply("FilterEndTime", Filter.by(
    (GameActionInfo gInfo)
        -> gInfo.getTimestamp() < stopMinTimestamp.getMillis()))
```

```py
| 'FilterStartTime' >> beam.Filter(
    lambda element: element['timestamp'] > start_min_filter)
| 'FilterEndTime' >> beam.Filter(
    lambda element: element['timestamp'] < end_min_filter)
```

#### Calculating Score Per Team, Per Window

`HourlyTeamScore` uses the same `ExtractAndSumScores` transform as the `UserScore` pipeline, but passes a different key (team, as opposed to user). Also, because the pipeline applies `ExtractAndSumScores` _after_ applying fixed-time 1-hour windowing to the input data, the data gets grouped by both team _and_ window. You can see the full sequence of transforms in `HourlyTeamScore`'s main method:

```java
public static void main(String[] args) throws Exception {
  // Begin constructing a pipeline configured by commandline flags.
  Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
  Pipeline pipeline = Pipeline.create(options);

  final Instant stopMinTimestamp = new Instant(minFmt.parseMillis(options.getStopMin()));
  final Instant startMinTimestamp = new Instant(minFmt.parseMillis(options.getStartMin()));

  // Read 'gaming' events from a text file.
  pipeline.apply(TextIO.read().from(options.getInput()))
    // Parse the incoming data.
    .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))

    // Filter out data before and after the given times so that it is not included
    // in the calculations. As we collect data in batches (say, by day), the batch for the day
    // that we want to analyze could potentially include some late-arriving data from the previous
    // day. If so, we want to weed it out. Similarly, if we include data from the following day
    // (to scoop up late-arriving events from the day we're analyzing), we need to weed out events
    // that fall after the time period we want to analyze.
    // [START DocInclude_HTSFilters]
    .apply("FilterStartTime", Filter.by(
        (GameActionInfo gInfo)
            -> gInfo.getTimestamp() > startMinTimestamp.getMillis()))
    .apply("FilterEndTime", Filter.by(
        (GameActionInfo gInfo)
            -> gInfo.getTimestamp() < stopMinTimestamp.getMillis()))
    // [END DocInclude_HTSFilters]

    // [START DocInclude_HTSAddTsAndWindow]
    // Add an element timestamp based on the event log, and apply fixed windowing.
    .apply("AddEventTimestamps",
           WithTimestamps.of((GameActionInfo i) -> new Instant(i.getTimestamp())))
    .apply("FixedWindowsTeam", Window.<GameActionInfo>into(
        FixedWindows.of(Duration.standardMinutes(options.getWindowDuration()))))
    // [END DocInclude_HTSAddTsAndWindow]

    // Extract and sum teamname/score pairs from the event data.
    .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
    .apply("WriteTeamScoreSums",
      new WriteWindowedToBigQuery<KV<String, Integer>>(options.getTableName(),
          configureWindowedTableWrite()));

  pipeline.run().waitUntilFinish();
}
```

```py
class HourlyTeamScore(beam.PTransform):
  def __init__(self, start_min, stop_min, window_duration):
    super(HourlyTeamScore, self).__init__()
    self.start_min = start_min
    self.stop_min = stop_min
    self.window_duration = window_duration

  def expand(self, pcoll):
    start_min_filter = string_to_timestamp(self.start_min)
    end_min_filter = string_to_timestamp(self.stop_min)

    return (
        pcoll
        | 'ParseGameEvent' >> beam.ParDo(ParseEventFn())
        # Filter out data before and after the given times so that it is not
        # included in the calculations. As we collect data in batches (say, by
        # day), the batch for the day that we want to analyze could potentially
        # include some late-arriving data from the previous day. If so, we want
        # to weed it out. Similarly, if we include data from the following day
        # (to scoop up late-arriving events from the day we're analyzing), we
        # need to weed out events that fall after the time period we want to
        # analyze.
        | 'FilterStartTime' >> beam.Filter(
            lambda element: element['timestamp'] > start_min_filter)
        | 'FilterEndTime' >> beam.Filter(
            lambda element: element['timestamp'] < end_min_filter)
        # Add an element timestamp based on the event log, and apply fixed
        # windowing.
        # Convert element['timestamp'] into seconds as expected by
        # TimestampedValue.
        | 'AddEventTimestamps' >> beam.Map(
            lambda element: TimestampedValue(
                element, element['timestamp'] / 1000.0))
        # Convert window_duration into seconds as expected by FixedWindows.
        | 'FixedWindowsTeam' >> beam.WindowInto(FixedWindows(
            size=self.window_duration * 60))
        # Extract and sum teamname/score pairs from the event data.
        | 'ExtractTeamScore' >> ExtractAndSumScore('team'))


def run(argv=None):
  """Main entry point; defines and runs the hourly_team_score pipeline."""
  ...

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  p = beam.Pipeline(options=pipeline_options)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  (p  # pylint: disable=expression-not-assigned
   | ReadFromText(known_args.input)
   | HourlyTeamScore(
       known_args.start_min, known_args.stop_min, known_args.window_duration)
   | WriteWindowedToBigQuery(
       known_args.table_name, known_args.dataset, configure_bigquery_write()))

  result = p.run()
  result.wait_until_finish()
```

### Limitations

As written, `HourlyTeamScore` still has a limitation:

* `HourlyTeamScore` still has **high latency** between when data events occur (the event time) and when results are generated (the processing time), because, as a batch pipeline, it needs to wait to begin processing until all data events are present.


## LeaderBoard: Streaming Processing with Real-Time Game Data

> **Note:** This example currently exists in Java only.

One way we can help address the latency issue present in the `UserScore` and `HourlyTeamScore` pipelines is by reading the score data from an unbounded source. The `LeaderBoard` pipeline introduces streaming processing by reading the game score data from an unbounded source that produces an infinite amount of data, rather than from a file on the game server.

The `LeaderBoard` pipeline also demonstrates how to process game score data with respect to both _processing time_ and _event time_. `LeaderBoard` outputs data about both individual user scores and about team scores, each with respect to a different time frame.

Because the `LeaderBoard` pipeline reads the game data from an unbounded source as that data is generated, you can think of the pipeline as an ongoing job running concurrently with the game process. `LeaderBoard` can thus provide low-latency insights into how users are playing the game at any given moment—useful if, for example, we want to provide a live web-based scoreboard so that users can track their progress against other users as they play.

> **Note:** See [LeaderBoard on GitHub](https://github.com/apache/beam/blob/master/examples/java8/src/main/java/org/apache/beam/examples/complete/game/LeaderBoard.java) for the complete example pipeline program.

### What Does LeaderBoard Do?

The `LeaderBoard` pipeline reads game data published to an unbounded source that produces an infinite amount of data in near real-time, and uses that data to perform two separate processing tasks:

* `LeaderBoard` calculates the total score for every unique user and publishes speculative results for every ten minutes of _processing time_. That is, every ten minutes, the pipeline outputs the total score per user that the pipeline has processed to date. This calculation provides a running "leader board" in close to real time, regardless of when the actual game events were generated.

* `LeaderBoard` calculates the team scores for each hour that the pipeline runs. This is useful if we want to, for example, reward the top-scoring team for each hour of play. The team score calculation uses fixed-time windowing to divide the input data into hour-long finite windows based on the _event time_ (indicated by the timestamp) as data arrives in the pipeline.  

    In addition, the team score calculation uses Beam's trigger mechanisms to provide speculative results for each hour (which update every five minutes until the hour is up), and to also capture any late data and add it to the specific hour-long window to which it belongs.

Below, we'll look at both of these tasks in detail.

#### Calculating User Score based on Processing Time

We want our pipeline to output a running total score for each user for every ten minutes that the pipeline runs. This calculation doesn't consider _when_ the actual score was generated by the user's play instance; it simply outputs the sum of all the scores for that user that have arrived in the pipeline to date. Late data gets included in the calculation whenever it happens to arrive in the pipeline as it's running.

Because we want all the data that has arrived in the pipeline every time we update our calculation, we have the pipeline consider all of the user score data in a **single global window**. The single global window is unbounded, but we can specify a kind of temporary cut-off point for each ten-minute calculation by using a processing time [trigger]({{ site.baseurl }}/documentation/programming-guide/#triggers).

When we specify a ten-minute processing time trigger for the single global window, the pipeline effectively takes a "snapshot" of the contents of the window every time the trigger fires. This snapshot happens at ten-minute intervals as long as data has arrived. If no data has arrived, the pipeline will take its next "snapshot" 10 minutes past an element arriving. Since we're using a single global window, each snapshot contains all the data collected _to that point in time_. The following diagram shows the effects of using a processing time trigger on the single global window:

<figure id="fig4">
    <img src="{{ site.baseurl }}/images/gaming-example-proc-time-narrow.gif"
         width="900" height="263"
         alt="Score data for for three users.">
</figure>
Figure 4: Score data for for three users. Each user's scores are grouped together in a single global window, with a trigger that generates a snapshot for output every ten minutes.

As processing time advances and more scores are processed, the trigger outputs the updated sum for each user.

The following code example shows how `LeaderBoard` sets the processing time trigger to output the data for user scores:

```java
/**
 * Extract user/score pairs from the event stream using processing time, via global windowing.
 * Get periodic updates on all users' running scores.
 */
@VisibleForTesting
static class CalculateUserScores
    extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
  private final Duration allowedLateness;

  CalculateUserScores(Duration allowedLateness) {
    this.allowedLateness = allowedLateness;
  }

  @Override
  public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> input) {
    return input.apply("LeaderboardUserGlobalWindow",
        Window.<GameActionInfo>into(new GlobalWindows())
            // Get periodic results every ten minutes.
            .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(TEN_MINUTES)))
            .accumulatingFiredPanes()
            .withAllowedLateness(allowedLateness))
        // Extract and sum username/score pairs from the event data.
        .apply("ExtractUserScore", new ExtractAndSumScore("user"));
  }
}
```

Note that `LeaderBoard` uses an accumulating trigger for the user score calculation (by invoking `.accumulatingFiredPanes` when setting the trigger). Using an accumulating trigger causes the pipeline to accumulate the previously emitted data together with any new data that's arrived since the last trigger fire. This ensures that `LeaderBoard` a running sum for the user scores, rather than a collection of individual sums.

#### Calculating Team Score based on Event Time

We want our pipeline to also output the total score for each team during each hour of play. Unlike the user score calculation, for team scores, we care about when in _event_ time each score actually occurred, because we want to consider each hour of play individually. We also want to provide speculative updates as each individual hour progresses, and to allow any instances of late data—data that arrives after a given hour's data is considered complete—to be included in our calculation.

Because we consider each hour individually, we can apply fixed-time windowing to our input data, just like in `HourlyTeamScore`. To provide the speculative updates and updates on late data, we'll specify additional trigger parameters. The trigger will cause each window to calculate and emit results at an interval we specify (in this case, every five minutes), and also to keep triggering after the window is considered "complete" to account for late data. Just like the user score calculation, we'll set the trigger to accumulating mode to ensure that we get a running sum for each hour-long window.

The triggers for speculative updates and late data help with the problem of [time skew]({{ site.baseurl }}/documentation/programming-guide/#windowing). Events in the pipeline aren't necessarily processed in the order in which they actually occurred according to their timestamps; they may arrive in the pipeline out of order, or late (in our case, because they were generated while the user's phone was out of contact with a network). Beam needs a way to determine when it can reasonably assume that it has "all" of the data in a given window: this is called the _watermark_.

In an ideal world, all data would be processed immediately when it occurs, so the processing time would be equal to (or at least have a linear relationship to) the event time. However, because distributed systems contain some inherent inaccuracy (like our late-reporting phones), Beam often uses a heuristic watermark.

The following diagram shows the relationship between ongoing processing time and each score's event time for two teams:

<figure id="fig5">
    <img src="{{ site.baseurl }}/images/gaming-example-event-time-narrow.gif"
         width="900" height="390"
         alt="Score data by team, windowed by event time.">
</figure>
Figure 5: Score data by team, windowed by event time. A trigger based on processing time causes the window to emit speculative early results and include late results.

The dotted line in the diagram is the "ideal" **watermark**: Beam's notion of when all data in a given window can reasonably be considered to have arrived. The irregular solid line represents the actual watermark, as determined by the data source.

Data arriving above the solid watermark line is _late data_—this is a score event that was delayed (perhaps generated offline) and arrived after the window to which it belongs had closed. Our pipeline's late-firing trigger ensures that this late data is still included in the sum.

The following code example shows how `LeaderBoard` applies fixed-time windowing with the appropriate triggers to have our pipeline perform the calculations we want:

```java
// Extract team/score pairs from the event stream, using hour-long windows by default.
static class CalculateTeamScores
    extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
  private final Duration teamWindowDuration;
  private final Duration allowedLateness;

  CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
    this.teamWindowDuration = teamWindowDuration;
    this.allowedLateness = allowedLateness;
  }

  @Override
  public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> infos) {
    return infos.apply("LeaderboardTeamFixedWindows",
        Window.<GameActionInfo>into(FixedWindows.of(teamWindowDuration))
            // We will get early (speculative) results as well as cumulative
            // processing of late data.
            .triggering(AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(FIVE_MINUTES))
                .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(TEN_MINUTES)))
            .withAllowedLateness(allowedLateness)
            .accumulatingFiredPanes())
        // Extract and sum teamname/score pairs from the event data.
        .apply("ExtractTeamScore", new ExtractAndSumScore("team"));
  }
}
```

Taken together, these processing strategies let us address the latency and completeness issues present in the `UserScore` and `HourlyTeamScore` pipelines, while still using the same basic transforms to process the data—as a matter of fact, both calculations still use the same `ExtractAndSumScore` transform that we used in both the `UserScore` and `HourlyTeamScore` pipelines.

## GameStats: Abuse Detection and Usage Analysis

> **Note:** This example currently exists in Java only.

While `LeaderBoard` demonstrates how to use basic windowing and triggers to perform low-latency and flexible data analysis, we can use more advanced windowing techniques to perform more comprehensive analysis. This might include some calculations designed to detect system abuse (like spam) or to gain insight into user behavior. The `GameStats` pipeline builds on the low-latency functionality in `LeaderBoard` to demonstrate how you can use Beam to perform this kind of advanced analysis.

Like `LeaderBoard`, `GameStats` reads data from an unbounded source. It is best thought of as an ongoing job that provides insight into the game as users play.

> **Note:** See [GameStats on GitHub](https://github.com/apache/beam/blob/master/examples/java8/src/main/java/org/apache/beam/examples/complete/game/GameStats.java) for the complete example pipeline program.

### What Does GameStats Do?

Like `LeaderBoard`, `GameStats` calculates the total score per team, per hour. However, the pipeline also performs two kinds of more complex analysis:

* `GameStats` does **abuse detection** system that performs some simple statistical analysis on the score data to determine which users, if any, might be spammers or bots. It then uses the list of suspected spam/bot users to filter the bots out of the hourly team score calculation.
* `GameStats` **analyzes usage patterns** by grouping together game data that share similar event times using session windowing. This lets us gain some intelligence on how long users tend to play, and how game length changes over time.

Below, we'll look at these features in more detail.

#### Abuse Detection

Let's suppose scoring in our game depends on the speed at which a user can "click" on their phone. `GameStats`'s abuse detection analyzes each user's score data to detect if a user has an abnormally high "click rate" and thus an abnormally high score. This might indicate that the game is being played by a bot that operates significantly faster than a human could play.

To determine whether or not a score is "abnormally" high, `GameStats` calculates the average of every score in that fixed-time window, and then checks each score individual score against the average score multiplied by an arbitrary weight factor (in our case, 2.5). Thus, any score more than 2.5 times the average is deemed to be the product of spam. The `GameStats` pipeline tracks a list of "spam" users and filters those users out of the team score calculations for the team leader board.

Since the average depends on the pipeline data, we need to calculate it, and then use that calculated data in a subsequent `ParDo` transform that filters scores that exceed the weighted value. To do this, we can pass the calculated average to as a [side input]({{ site.baseurl }}/documentation/programming-guide/#transforms-sideio) to the filtering `ParDo`.

The following code example shows the composite transform that handles abuse detection. The transform uses the `Sum.integersPerKey` transform to sum all scores per user, and then the `Mean.globally` transform to determine the average score for all users. Once that's been calculated (as a `PCollectionView` singleton), we can pass it to the filtering `ParDo` using `.withSideInputs`:

```java
public static class CalculateSpammyUsers
    extends PTransform<PCollection<KV<String, Integer>>, PCollection<KV<String, Integer>>> {
  private static final Logger LOG = LoggerFactory.getLogger(CalculateSpammyUsers.class);
  private static final double SCORE_WEIGHT = 2.5;

  @Override
  public PCollection<KV<String, Integer>> expand(PCollection<KV<String, Integer>> userScores) {

    // Get the sum of scores for each user.
    PCollection<KV<String, Integer>> sumScores = userScores
        .apply("UserSum", Sum.<String>integersPerKey());

    // Extract the score from each element, and use it to find the global mean.
    final PCollectionView<Double> globalMeanScore = sumScores.apply(Values.<Integer>create())
        .apply(Mean.<Integer>globally().asSingletonView());

    // Filter the user sums using the global mean.
    PCollection<KV<String, Integer>> filtered = sumScores
        .apply("ProcessAndFilter", ParDo
            .of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
              private final Aggregator<Long, Long> numSpammerUsers =
                createAggregator("SpammerUsers", new Sum.SumLongFn());
              @ProcessElement
              public void processElement(ProcessContext c) {
                Integer score = c.element().getValue();
                Double gmc = c.sideInput(globalMeanScore);
                if (score > (gmc * SCORE_WEIGHT)) {
                  LOG.info("user " + c.element().getKey() + " spammer score " + score
                      + " with mean " + gmc);
                  numSpammerUsers.addValue(1L);
                  c.output(c.element());
                }
              }
            })
            // use the derived mean total score as a side input
            .withSideInputs(globalMeanScore));
    return filtered;
  }
}
```

The abuse-detection transform generates a view of users supected to be spambots. Later in the pipeline, we use that view to filter out any such users when we calculate the team score per hour, again by using the side input mechanism. The following code example shows where we insert the spam filter, between windowing the scores into fixed windows and extracting the team scores:

```java
// Calculate the total score per team over fixed windows,
// and emit cumulative updates for late data. Uses the side input derived above-- the set of
// suspected robots-- to filter out scores from those users from the sum.
// Write the results to BigQuery.
rawEvents
  .apply("WindowIntoFixedWindows", Window.<GameActionInfo>into(
      FixedWindows.of(Duration.standardMinutes(options.getFixedWindowDuration()))))
  // Filter out the detected spammer users, using the side input derived above.
  .apply("FilterOutSpammers", ParDo
          .of(new DoFn<GameActionInfo, GameActionInfo>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              // If the user is not in the spammers Map, output the data element.
              if (c.sideInput(spammersView).get(c.element().getUser().trim()) == null) {
                c.output(c.element());
              }
            }
          })
          .withSideInputs(spammersView))
  // Extract and sum teamname/score pairs from the event data.
  .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
```

#### Analyzing Usage Patterns

We can gain some insight on when users are playing our game, and for how long, by examining the event times for each game score and grouping scores with similar event times into _sessions_. `GameStats` uses Beam's built-in [session windowing](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/Sessions.java) function to group user scores into sessions based on the time they occurred.

When you set session windowing, you specify a _minimum gap duration_ between events. All events whose arrival times are closer together than the minimum gap duration are grouped into the same window. Events where the difference in arrival time is greater than the gap are grouped into separate windows. Depending on how we set our minimum gap duration, we can safely assume that scores in the same session window are part of the same (relatively) uninterrupted stretch of play. Scores in a different window indicate that the user stopped playing the game for at least the minimum gap time before returning to it later.

The following diagram shows how data might look when grouped into session windows. Unlike fixed windows, session windows are _different for each user_ and is dependent on each individual user's play pattern:

<figure id="fig6">
    <img src="{{ site.baseurl }}/images/gaming-example-session-windows.png"
         width="662" height="521"
         alt="A diagram representing session windowing."
         alt="User sessions, with a minimum gap duration.">
</figure>
Figure 6: User sessions, with a minimum gap duration. Note how each user has different sessions, according to how many instances they play and how long their breaks between instances are.

We can use the session-windowed data to determine the average length of uninterrupted play time for all of our users, as well as the total score they achieve during each session. We can do this in the code by first applying session windows, summing the score per user and session, and then using a transform to calculate the length of each individual session:

```java
// Detect user sessions-- that is, a burst of activity separated by a gap from further
// activity. Find and record the mean session lengths.
// This information could help the game designers track the changing user engagement
// as their set of games changes.
userEvents
  .apply("WindowIntoSessions", Window.<KV<String, Integer>>into(
      Sessions.withGapDuration(Duration.standardMinutes(options.getSessionGap())))
      .withOutputTimeFn(OutputTimeFns.outputAtEndOfWindow()))
  // For this use, we care only about the existence of the session, not any particular
  // information aggregated over it, so the following is an efficient way to do that.
  .apply(Combine.perKey(x -> 0))
  // Get the duration per session.
  .apply("UserSessionActivity", ParDo.of(new UserSessionInfoFn()))
```

This gives us a set of user sessions, each with an attached duration. We can then calculate the _average_ session length by re-windowing the data into fixed time windows, and then calculating the average for all sessions that end in each hour:

```java
// Re-window to process groups of session sums according to when the sessions complete.
.apply("WindowToExtractSessionMean", Window.<Integer>into(
    FixedWindows.of(Duration.standardMinutes(options.getUserActivityWindowDuration()))))
// Find the mean session duration in each window.
.apply(Mean.<Integer>globally().withoutDefaults())
// Write this info to a BigQuery table.
.apply("WriteAvgSessionLength",
       new WriteWindowedToBigQuery<Double>(
          options.getTablePrefix() + "_sessions", configureSessionWindowWrite()));
```

We can use the resulting information to find, for example, what times of day our users are playing the longest, or which stretches of the day are more likely to see shorter play sessions.

