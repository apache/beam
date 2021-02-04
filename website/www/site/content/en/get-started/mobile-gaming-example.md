---
title: "Beam Mobile Gaming Example"
aliases: /use/mobile-gaming-example/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Beam Mobile Gaming Pipeline Examples

{{< toc >}}

{{< language-switcher java py >}}

This section provides a walkthrough of a series of example Apache Beam pipelines that demonstrate more complex functionality than the basic [WordCount](/get-started/wordcount-example) examples. The pipelines in this section process data from a hypothetical game that users play on their mobile phones. The pipelines demonstrate processing at increasing levels of complexity; the first pipeline, for example, shows how to run a batch analysis job to obtain relatively simple score data, while the later pipelines use Beam's windowing and triggers features to provide low-latency data analysis and more complex intelligence about user's play patterns.

{{< paragraph class="language-java" >}}
> **Note**: These examples assume some familiarity with the Beam programming model. If you haven't already, we recommend familiarizing yourself with the programming model documentation and running a basic example pipeline before continuing. Note also that these examples use the Java 8 lambda syntax, and thus require Java 8. However, you can create pipelines with equivalent functionality using Java 7.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
> **Note**: These examples assume some familiarity with the Beam programming model. If you haven't already, we recommend familiarizing yourself with the programming model documentation and running a basic example pipeline before continuing.
{{< /paragraph >}}

> **Note**: MobileGaming is not yet available for the Go SDK. There is an open issue for this
([BEAM-4293](https://issues.apache.org/jira/browse/BEAM-4293)).

Every time a user plays an instance of our hypothetical mobile game, they generate a data event. Each data event consists of the following information:

- The unique ID of the user playing the game.
- The team ID for the team to which the user belongs.
- A score value for that particular instance of play.
- A timestamp that records when the particular instance of play happened--this is the event time for each game data event.

When the user completes an instance of the game, their phone sends the data event to a game server, where the data is logged and stored in a file. Generally the data is sent to the game server immediately upon completion. However, sometimes delays can happen in the network at various points. Another possible scenario involves users who play the game "offline", when their phones are out of contact with the server (such as on an airplane, or outside network coverage area). When the user's phone comes back into contact with the game server, the phone will send all accumulated game data. In these cases, some data events may arrive delayed and out of order.

The following diagram shows the ideal situation (events are processed as they occur) vs. reality (there is often a time delay before processing).

![There is often a time delay before processing events.](/images/gaming-example-basic.png)

*Figure 1: The X-axis represents event time: the actual time a game event
occurred. The Y-axis represents processing time: the time at which a game event
was processed. Ideally, events should be processed as they occur, depicted by
the dotted line in the diagram. However, in reality that is not the case and it
looks more like what is depicted by the red squiggly line above the ideal line.*

The data events might be received by the game server significantly later than users generate them. This time difference (called **skew**) can have processing implications for pipelines that make calculations that consider when each score was generated. Such pipelines might track scores generated during each hour of a day, for example, or they calculate the length of time that users are continuously playing the game—both of which depend on each data record's event time.

Because some of our example pipelines use data files (like logs from the game server) as input, the event timestamp for each game might be embedded in the data--that is, it's a field in each data record. Those pipelines need to parse the event timestamp from each data record after reading it from the input file.

For pipelines that read unbounded game data from an unbounded source, the data source sets the intrinsic [timestamp](/documentation/programming-guide/#element-timestamps) for each PCollection element to the appropriate event time.

The Mobile Gaming example pipelines vary in complexity, from simple batch analysis to more complex pipelines that can perform real-time analysis and abuse detection. This section walks you through each example and demonstrates how to use Beam features like windowing and triggers to expand your pipeline's capabilites.

## UserScore: Basic Score Processing in Batch

The `UserScore` pipeline is the simplest example for processing mobile game data. `UserScore` determines the total score per user over a finite data set (for example, one day's worth of scores stored on the game server). Pipelines like `UserScore` are best run periodically after all relevant data has been gathered. For example, `UserScore` could run as a nightly job over data gathered during that day.

{{< paragraph class="language-java" >}}
> **Note:** See [UserScore on GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/game/UserScore.java) for the complete example pipeline program.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
> **Note:** See [UserScore on GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/user_score.py) for the complete example pipeline program.
{{< /paragraph >}}

### What Does UserScore Do?

In a day's worth of scoring data, each user ID may have multiple records (if the user plays more than one instance of the game during the analysis window), each with their own score value and timestamp. If we want to determine the total score over all the instances a user plays during the day, our pipeline will need to group all the records together per individual user.

As the pipeline processes each event, the event score gets added to the sum total for that particular user.

`UserScore` parses out only the data that it needs from each record, specifically the user ID and the score value. The pipeline doesn't consider the event time for any record; it simply processes all data present in the input files that you specify when you run the pipeline.

> **Note:** To use the `UserScore` pipeline effectively, you'd need to ensure that you supply input data that has already been grouped by the desired event time period — that is, that you specify an input file that only contains data from the day you care about.

`UserScore`'s basic pipeline flow does the following:

1. Read the day's score data from a text file.
2. Sum the score values for each unique user by grouping each game event by user ID and combining the score values to get the total score for that particular user.
3. Write the result data to a text file.

The following diagram shows score data for several users over the pipeline analysis period. In the diagram, each data point is an event that results in one user/score pair.

<img src="/images/gaming-example.gif" alt="A pipeline processes score data for three users." width="850px">

*Figure 2: Score data for three users.*

This example uses batch processing, and the diagram's Y axis represents processing time: the pipeline processes events lower on the Y-axis first, and events higher up the axis later. The diagram's X axis represents the event time for each game event, as denoted by that event's timestamp. Note that the individual events in the diagram are not processed by the pipeline in the same order as they occurred (according to their timestamps).

After reading the score events from the input file, the pipeline groups all of those user/score pairs together and sums the score values into one total value per unique user. `UserScore` encapsulates the core logic for that step as the [user-defined composite transform](/documentation/programming-guide/#composite-transforms) `ExtractAndSumScore`:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/UserScore.java" DocInclude_USExtractXform >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/user_score.py" extract_and_sum_score >}}
{{< /highlight >}}

`ExtractAndSumScore` is written to be more general, in that you can pass in the field by which you want to group the data (in the case of our game, by unique user or unique team). This means we can re-use `ExtractAndSumScore` in other pipelines that group score data by team, for example.

Here's the main method of `UserScore`, showing how we apply all three steps of the pipeline:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/UserScore.java" DocInclude_USMain >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/user_score.py" main >}}
{{< /highlight >}}

### Limitations

As written in the example, the `UserScore` pipeline has a few limitations:

* Because some score data may be generated by offline players and sent after the daily cutoff, for game data, the result data generated by the `UserScore` pipeline **may be incomplete**. `UserScore` only processes the fixed input set present in the input file(s) when the pipeline runs.

* `UserScore` processes all data events present in the input file at processing time, and **does not examine or otherwise error-check events based on event time**. Therefore, the results may include some values whose event times fall outside the relevant analysis period, such as late records from the previous day.

* Because `UserScore` runs only after all the data has been collected, it has **high latency** between when users generate data events (the event time) and when results are computed (the processing time).

* `UserScore` also only reports the total results for the entire day, and doesn't provide any finer-grained information about how the data accumulated during the day.

Starting with the next pipeline example, we'll discuss how you can use Beam's features to address these limitations.

## HourlyTeamScore: Advanced Processing in Batch with Windowing

The `HourlyTeamScore` pipeline expands on the basic batch analysis principles used in the `UserScore` pipeline and improves upon some of its limitations. `HourlyTeamScore` performs finer-grained analysis, both by using additional features in the Beam SDKs, and taking into account more aspects of the game data. For example, `HourlyTeamScore` can filter out data that isn't part of the relevant analysis period.

Like `UserScore`, `HourlyTeamScore` is best thought of as a job to be run periodically after all the relevant data has been gathered (such as once per day). The pipeline reads a fixed data set from a file, and writes the results <span class="language-java">back to a text file</span><span class="language-py">to a Google Cloud BigQuery table</span>.

{{< paragraph class="language-java" >}}
> **Note:** See [HourlyTeamScore on GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/game/HourlyTeamScore.java) for the complete example pipeline program.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
> **Note:** See [HourlyTeamScore on GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/hourly_team_score.py) for the complete example pipeline program.
{{< /paragraph >}}

### What Does HourlyTeamScore Do?

`HourlyTeamScore` calculates the total score per team, per hour, in a fixed data set (such as one day's worth of data).

* Rather than operating on the entire data set at once, `HourlyTeamScore` divides the input data into logical windows and performs calculations on those windows. This allows `HourlyUserScore` to provide information on scoring data per window, where each window represents the game score progress at fixed intervals in time (like once every hour).

* `HourlyTeamScore` filters data events based on whether their event time (as indicated by the embedded timestamp) falls within the relevant analysis period. Basically, the pipeline checks each game event's timestamp and ensures that it falls within the range we want to analyze (in this case the day in question). Data events from previous days are discarded and not included in the score totals. This makes `HourlyTeamScore` more robust and less prone to erroneous result data than `UserScore`. It also allows the pipeline to account for late-arriving data that has a timestamp within the relevant analysis period.

Below, we'll look at each of these enhancements in `HourlyTeamScore` in detail:

#### Fixed-Time Windowing

Using fixed-time windowing lets the pipeline provide better information on how events accumulated in the data set over the course of the analysis period. In our case, it tells us when in the day each team was active and how much the team scored at those times.

The following diagram shows how the pipeline processes a day's worth of a single team's scoring data after applying fixed-time windowing:

<img src="/images/gaming-example-team-scores-narrow.gif" alt="A pipeline processes score data for two teams." width="800px">

*Figure 3: Score data for two teams. Each team's scores are divided into
logical windows based on when those scores occurred in event time.*

Notice that as processing time advances, the sums are now _per window_; each window represents an hour of _event time_ during the day in which the scores occurred.

> **Note:** As is shown in the diagram above, using windowing produces an _independent total for every interval_ (in this case, each hour). `HourlyTeamScore` doesn't provide a running total for the entire data set at each hour--it provides the total score for all the events that occurred _only within that hour_.

Beam's windowing feature uses the [intrinsic timestamp information](/documentation/programming-guide/#element-timestamps) attached to each element of a `PCollection`. Because we want our pipeline to window based on _event time_, we **must first extract the timestamp** that's embedded in each data record apply it to the corresponding element in the `PCollection` of score data. Then, the pipeline can **apply the windowing function** to divide the `PCollection` into logical windows.

{{< paragraph class="language-java" >}}
`HourlyTeamScore` uses the [WithTimestamps](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/WithTimestamps.java) and [Window](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/Window.java) transforms to perform these operations.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
`HourlyTeamScore` uses the `FixedWindows` transform, found in [window.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/window.py), to perform these operations.
{{< /paragraph >}}

The following code shows this:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/HourlyTeamScore.java" DocInclude_HTSAddTsAndWindow >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/hourly_team_score.py" add_timestamp_and_window >}}
{{< /highlight >}}

Notice that the transforms the pipeline uses to specify the windowing are distinct from the actual data processing transforms (such as `ExtractAndSumScores`). This functionality provides you some flexibility in designing your Beam pipeline, in that you can run existing transforms over datasets with different windowing characteristics.

#### Filtering Based On Event Time

`HourlyTeamScore` uses **filtering** to remove any events from our dataset whose timestamps don't fall within the relevant analysis period (i.e. they weren't generated during the day that we're interested in). This keeps the pipeline from erroneously including any data that was, for example, generated offline during the previous day but sent to the game server during the current day.

It also lets the pipeline include relevant **late data**—data events with valid timestamps, but that arrived after our analysis period ended. If our pipeline cutoff time is 12:00 am, for example, we might run the pipeline at 2:00 am, but filter out any events whose timestamps indicate that they occurred after the 12:00 am cutoff. Data events that were delayed and arrived between 12:01 am and 2:00 am, but whose timestamps indicate that they occurred before the 12:00 am cutoff, would be included in the pipeline processing.

`HourlyTeamScore` uses the `Filter` transform to perform this operation. When you apply `Filter`, you specify a predicate to which each data record is compared. Data records that pass the comparison are included, while events that fail the comparison are excluded. In our case, the predicate is the cut-off time we specify, and we compare just one part of the data—the timestamp field.

The following code shows how `HourlyTeamScore` uses the `Filter` transform to filter events that occur either before or after the relevant analysis period:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/HourlyTeamScore.java" DocInclude_HTSFilters >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/hourly_team_score.py" filter_by_time_range >}}
{{< /highlight >}}

#### Calculating Score Per Team, Per Window

`HourlyTeamScore` uses the same `ExtractAndSumScores` transform as the `UserScore` pipeline, but passes a different key (team, as opposed to user). Also, because the pipeline applies `ExtractAndSumScores` _after_ applying fixed-time 1-hour windowing to the input data, the data gets grouped by both team _and_ window. You can see the full sequence of transforms in `HourlyTeamScore`'s main method:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/HourlyTeamScore.java" DocInclude_HTSMain >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/hourly_team_score.py" main >}}
{{< /highlight >}}

### Limitations

As written, `HourlyTeamScore` still has a limitation:

* `HourlyTeamScore` still has **high latency** between when data events occur (the event time) and when results are generated (the processing time), because, as a batch pipeline, it needs to wait to begin processing until all data events are present.

## LeaderBoard: Streaming Processing with Real-Time Game Data

One way we can help address the latency issue present in the `UserScore` and `HourlyTeamScore` pipelines is by reading the score data from an unbounded source. The `LeaderBoard` pipeline introduces streaming processing by reading the game score data from an unbounded source that produces an infinite amount of data, rather than from a file on the game server.

The `LeaderBoard` pipeline also demonstrates how to process game score data with respect to both _processing time_ and _event time_. `LeaderBoard` outputs data about both individual user scores and about team scores, each with respect to a different time frame.

Because the `LeaderBoard` pipeline reads the game data from an unbounded source as that data is generated, you can think of the pipeline as an ongoing job running concurrently with the game process. `LeaderBoard` can thus provide low-latency insights into how users are playing the game at any given moment — useful if, for example, we want to provide a live web-based scoreboard so that users can track their progress against other users as they play.

{{< paragraph class="language-java" >}}
> **Note:** See [LeaderBoard on GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/game/LeaderBoard.java) for the complete example pipeline program.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
> **Note:** See [LeaderBoard on GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/leader_board.py) for the complete example pipeline program.
{{< /paragraph >}}

### What Does LeaderBoard Do?

The `LeaderBoard` pipeline reads game data published to an unbounded source that produces an infinite amount of data in near real-time, and uses that data to perform two separate processing tasks:

* `LeaderBoard` calculates the total score for every unique user and publishes speculative results for every ten minutes of _processing time_. That is, ten minutes after data is received, the pipeline outputs the total score per user that the pipeline has processed to date. This calculation provides a running "leader board" in close to real time, regardless of when the actual game events were generated.

* `LeaderBoard` calculates the team scores for each hour that the pipeline runs. This is useful if we want to, for example, reward the top-scoring team for each hour of play. The team score calculation uses fixed-time windowing to divide the input data into hour-long finite windows based on the _event time_ (indicated by the timestamp) as data arrives in the pipeline.

    In addition, the team score calculation uses Beam's trigger mechanisms to provide speculative results for each hour (which update every five minutes until the hour is up), and to also capture any late data and add it to the specific hour-long window to which it belongs.

Below, we'll look at both of these tasks in detail.

#### Calculating User Score based on Processing Time

We want our pipeline to output a running total score for each user for every ten minutes of processing time. This calculation doesn't consider _when_ the actual score was generated by the user's play instance; it simply outputs the sum of all the scores for that user that have arrived in the pipeline to date. Late data gets included in the calculation whenever it happens to arrive in the pipeline as it's running.

Because we want all the data that has arrived in the pipeline every time we update our calculation, we have the pipeline consider all of the user score data in a **single global window**. The single global window is unbounded, but we can specify a kind of temporary cut-off point for each ten-minute calculation by using a processing time [trigger](/documentation/programming-guide/#triggers).

When we specify a ten-minute processing time trigger for the single global window, the pipeline effectively takes a "snapshot" of the contents of the window every time the trigger fires. This snapshot happens after ten minutes have passed since data was received. If no data has arrived, the pipeline takes its next "snapshot" 10 minutes after an element arrives. Since we're using a single global window, each snapshot contains all the data collected _to that point in time_. The following diagram shows the effects of using a processing time trigger on the single global window:

<img src="/images/gaming-example-proc-time-narrow.gif" alt="A pipeline processes score data for three users." width="850px">


*Figure 4: Score data for three users. Each user's scores are grouped together
in a single global window, with a trigger that generates a snapshot for output
ten minutes after data is received.*

As processing time advances and more scores are processed, the trigger outputs the updated sum for each user.

The following code example shows how `LeaderBoard` sets the processing time trigger to output the data for user scores:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/LeaderBoard.java" DocInclude_ProcTimeTrigger >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/leader_board.py" processing_time_trigger >}}
{{< /highlight >}}

`LeaderBoard` sets the [window accumulation mode](/documentation/programming-guide/#window-accumulation-modes) to accumulate window panes as the trigger fires. This accumulation mode is set by <span class="language-java">invoking `.accumulatingFiredPanes`</span> <span class="language-py">using `accumulation_mode=trigger.AccumulationMode.ACCUMULATING`</span> when setting the trigger, and causes the pipeline to accumulate the previously emitted data together with any new data that's arrived since the last trigger fire. This ensures that `LeaderBoard` is a running sum for the user scores, rather than a collection of individual sums.

#### Calculating Team Score based on Event Time

We want our pipeline to also output the total score for each team during each hour of play. Unlike the user score calculation, for team scores, we care about when in _event_ time each score actually occurred, because we want to consider each hour of play individually. We also want to provide speculative updates as each individual hour progresses, and to allow any instances of late data — data that arrives after a given hour's data is considered complete — to be included in our calculation.

Because we consider each hour individually, we can apply fixed-time windowing to our input data, just like in `HourlyTeamScore`. To provide the speculative updates and updates on late data, we'll specify additional trigger parameters. The trigger will cause each window to calculate and emit results at an interval we specify (in this case, every five minutes), and also to keep triggering after the window is considered "complete" to account for late data. Just like the user score calculation, we'll set the trigger to accumulating mode to ensure that we get a running sum for each hour-long window.

The triggers for speculative updates and late data help with the problem of [time skew](/documentation/programming-guide/#windowing). Events in the pipeline aren't necessarily processed in the order in which they actually occurred according to their timestamps; they may arrive in the pipeline out of order, or late (in our case, because they were generated while the user's phone was out of contact with a network). Beam needs a way to determine when it can reasonably assume that it has "all" of the data in a given window: this is called the _watermark_.

In an ideal world, all data would be processed immediately when it occurs, so the processing time would be equal to (or at least have a linear relationship to) the event time. However, because distributed systems contain some inherent inaccuracy (like our late-reporting phones), Beam often uses a heuristic watermark.

The following diagram shows the relationship between ongoing processing time and each score's event time for two teams:

<img src="/images/gaming-example-event-time-narrow.gif" alt="A pipeline processes score data by team, windowed by event time." width="800px">

*Figure 5: Score data by team, windowed by event time. A trigger based on
processing time causes the window to emit speculative early results and include
late results.*

The dotted line in the diagram is the "ideal" **watermark**: Beam's notion of when all data in a given window can reasonably be considered to have arrived. The irregular solid line represents the actual watermark, as determined by the data source.

Data arriving above the solid watermark line is _late data_ — this is a score event that was delayed (perhaps generated offline) and arrived after the window to which it belongs had closed. Our pipeline's late-firing trigger ensures that this late data is still included in the sum.

The following code example shows how `LeaderBoard` applies fixed-time windowing with the appropriate triggers to have our pipeline perform the calculations we want:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/LeaderBoard.java" DocInclude_WindowAndTrigger >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/leader_board.py" window_and_trigger >}}
{{< /highlight >}}

Taken together, these processing strategies let us address the latency and completeness issues present in the `UserScore` and `HourlyTeamScore` pipelines, while still using the same basic transforms to process the data—as a matter of fact, both calculations still use the same `ExtractAndSumScore` transform that we used in both the `UserScore` and `HourlyTeamScore` pipelines.

## GameStats: Abuse Detection and Usage Analysis

While `LeaderBoard` demonstrates how to use basic windowing and triggers to perform low-latency and flexible data analysis, we can use more advanced windowing techniques to perform more comprehensive analysis. This might include some calculations designed to detect system abuse (like spam) or to gain insight into user behavior. The `GameStats` pipeline builds on the low-latency functionality in `LeaderBoard` to demonstrate how you can use Beam to perform this kind of advanced analysis.

Like `LeaderBoard`, `GameStats` reads data from an unbounded source. It is best thought of as an ongoing job that provides insight into the game as users play.

{{< paragraph class="language-java" >}}
> **Note:** See [GameStats on GitHub](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/game/GameStats.java) for the complete example pipeline program.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
> **Note:** See [GameStats on GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/game/game_stats.py) for the complete example pipeline program.
{{< /paragraph >}}

### What Does GameStats Do?

Like `LeaderBoard`, `GameStats` calculates the total score per team, per hour. However, the pipeline also performs two kinds of more complex analysis:

* `GameStats` does **abuse detection** system that performs some simple statistical analysis on the score data to determine which users, if any, might be spammers or bots. It then uses the list of suspected spam/bot users to filter the bots out of the hourly team score calculation.
* `GameStats` **analyzes usage patterns** by grouping together game data that share similar event times using session windowing. This lets us gain some intelligence on how long users tend to play, and how game length changes over time.

Below, we'll look at these features in more detail.

#### Abuse Detection

Let's suppose scoring in our game depends on the speed at which a user can "click" on their phone. `GameStats`'s abuse detection analyzes each user's score data to detect if a user has an abnormally high "click rate" and thus an abnormally high score. This might indicate that the game is being played by a bot that operates significantly faster than a human could play.

To determine whether or not a score is "abnormally" high, `GameStats` calculates the average of every score in that fixed-time window, and then checks each individual score against the average score multiplied by an arbitrary weight factor (in our case, 2.5). Thus, any score more than 2.5 times the average is deemed to be the product of spam. The `GameStats` pipeline tracks a list of "spam" users and filters those users out of the team score calculations for the team leader board.

Since the average depends on the pipeline data, we need to calculate it, and then use that calculated data in a subsequent `ParDo` transform that filters scores that exceed the weighted value. To do this, we can pass the calculated average to as a [side input](/documentation/programming-guide/#side-inputs) to the filtering `ParDo`.

The following code example shows the composite transform that handles abuse detection. The transform uses the `Sum.integersPerKey` transform to sum all scores per user, and then the `Mean.globally` transform to determine the average score for all users. Once that's been calculated (as a `PCollectionView` singleton), we can pass it to the filtering `ParDo` using `.withSideInputs`:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/GameStats.java" DocInclude_AbuseDetect >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/game_stats.py" abuse_detect >}}
{{< /highlight >}}

The abuse-detection transform generates a view of users supected to be spambots. Later in the pipeline, we use that view to filter out any such users when we calculate the team score per hour, again by using the side input mechanism. The following code example shows where we insert the spam filter, between windowing the scores into fixed windows and extracting the team scores:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/GameStats.java" DocInclude_FilterAndCalc >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/game_stats.py" filter_and_calc >}}
{{< /highlight >}}

#### Analyzing Usage Patterns

We can gain some insight on when users are playing our game, and for how long, by examining the event times for each game score and grouping scores with similar event times into _sessions_. `GameStats` uses Beam's built-in [session windowing](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/Sessions.java) function to group user scores into sessions based on the time they occurred.

When you set session windowing, you specify a _minimum gap duration_ between events. All events whose arrival times are closer together than the minimum gap duration are grouped into the same window. Events where the difference in arrival time is greater than the gap are grouped into separate windows. Depending on how we set our minimum gap duration, we can safely assume that scores in the same session window are part of the same (relatively) uninterrupted stretch of play. Scores in a different window indicate that the user stopped playing the game for at least the minimum gap time before returning to it later.

The following diagram shows how data might look when grouped into session windows. Unlike fixed windows, session windows are _different for each user_ and is dependent on each individual user's play pattern:

![User sessions with a minimum gap duration.](/images/gaming-example-session-windows.png)

*Figure 6: User sessions with a minimum gap duration. Each user has different
sessions, according to how many instances they play and how long their breaks
between instances are.*

We can use the session-windowed data to determine the average length of uninterrupted play time for all of our users, as well as the total score they achieve during each session. We can do this in the code by first applying session windows, summing the score per user and session, and then using a transform to calculate the length of each individual session:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/GameStats.java" DocInclude_SessionCalc >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/game_stats.py" session_calc >}}
{{< /highlight >}}

This gives us a set of user sessions, each with an attached duration. We can then calculate the _average_ session length by re-windowing the data into fixed time windows, and then calculating the average for all sessions that end in each hour:

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/complete/game/GameStats.java" DocInclude_Rewindow >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/complete/game/game_stats.py" rewindow >}}
{{< /highlight >}}

We can use the resulting information to find, for example, what times of day our users are playing the longest, or which stretches of the day are more likely to see shorter play sessions.

## Next Steps

* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/documentation/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
