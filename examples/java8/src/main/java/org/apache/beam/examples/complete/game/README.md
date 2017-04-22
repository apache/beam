<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# 'Gaming' examples


This directory holds a series of example Apache Beam pipelines in a simple 'mobile
gaming' domain. They all require Java 8.  Each pipeline successively introduces
new concepts, and gives some examples of using Java 8 syntax in constructing
Beam pipelines. Other than usage of Java 8 lambda expressions, the concepts
that are used apply equally well in Java 7.

In the gaming scenario, many users play, as members of different teams, over
the course of a day, and their actions are logged for processing. Some of the
logged game events may be late-arriving, if users play on mobile devices and go
transiently offline for a period.

The scenario includes not only "regular" users, but "robot users", which have a
higher click rate than the regular users, and may move from team to team.

The first two pipelines in the series use pre-generated batch data samples. The
second two pipelines read from a [PubSub](https://cloud.google.com/pubsub/)
topic input.  For these examples, you will also need to run the
`injector.Injector` program, which generates and publishes the gaming data to
PubSub. The javadocs for each pipeline have more detailed information on how to
run that pipeline.

All of these pipelines write their results to BigQuery table(s).


## The pipelines in the 'gaming' series

### UserScore

The first pipeline in the series is `UserScore`. This pipeline does batch
processing of data collected from gaming events. It calculates the sum of
scores per user, over an entire batch of gaming data (collected, say, for each
day). The batch processing will not include any late data that arrives after
the day's cutoff point.

### HourlyTeamScore

The next pipeline in the series is `HourlyTeamScore`. This pipeline also
processes data collected from gaming events in batch. It builds on `UserScore`,
but uses [fixed windows](https://beam.apache.org/documentation/programming-guide/#windowing), by
default an hour in duration. It calculates the sum of scores per team, for each
window, optionally allowing specification of two timestamps before and after
which data is filtered out. This allows a model where late data collected after
the intended analysis window can be included in the analysis, and any late-
arriving data prior to the beginning of the analysis window can be removed as
well.

By using windowing and adding element timestamps, we can do finer-grained
analysis than with the `UserScore` pipeline — we're now tracking scores for
each hour rather than over the course of a whole day. However, our batch
processing is high-latency, in that we don't get results from plays at the
beginning of the batch's time period until the complete batch is processed.

### LeaderBoard

The third pipeline in the series is `LeaderBoard`. This pipeline processes an
unbounded stream of 'game events' from a PubSub topic. The calculation of the
team scores uses fixed windowing based on event time (the time of the game play
event), not processing time (the time that an event is processed by the
pipeline). The pipeline calculates the sum of scores per team, for each window.
By default, the team scores are calculated using one-hour windows.

In contrast — to demo another windowing option — the user scores are calculated
using a global window, which periodically (every ten minutes) emits cumulative
user score sums.

In contrast to the previous pipelines in the series, which used static, finite
input data, here we're using an unbounded data source, which lets us provide
_speculative_ results, and allows handling of late data, at much lower latency.
E.g., we could use the early/speculative results to keep a 'leaderboard'
updated in near-realtime. Our handling of late data lets us generate correct
results, e.g. for 'team prizes'. We're now outputing window results as they're
calculated, giving us much lower latency than with the previous batch examples.

### GameStats

The fourth pipeline in the series is `GameStats`. This pipeline builds
on the `LeaderBoard` functionality — supporting output of speculative and late
data — and adds some "business intelligence" analysis: identifying abuse
detection. The pipeline derives the Mean user score sum for a window, and uses
that information to identify likely spammers/robots. (The injector is designed
so that the "robots" have a higher click rate than the "real" users). The robot
users are then filtered out when calculating the team scores.

Additionally, user sessions are tracked: that is, we find bursts of user
activity using session windows. Then, the mean session duration information is
recorded in the context of subsequent fixed windowing. (This could be used to
tell us what games are giving us greater user retention).

### Running the PubSub Injector

The `LeaderBoard` and `GameStats` example pipelines read unbounded data
from a PubSub topic.

Use the `injector.Injector` program to generate this data and publish to a
PubSub topic. See the `Injector`javadocs for more information on how to run the
injector. Set up the injector before you start one of these pipelines. Then,
when you start the pipeline, pass as an argument the name of that PubSub topic.
See the pipeline javadocs for the details.

## Viewing the results in BigQuery

All of the pipelines write their results to BigQuery.  `UserScore` and
`HourlyTeamScore` each write one table, and `LeaderBoard` and
`GameStats` each write two. The pipelines have default table names that
you can override when you start up the pipeline if those tables already exist.

Depending on the windowing intervals defined in a given pipeline, you may have
to wait for a while (more than an hour) before you start to see results written
to the BigQuery tables.
