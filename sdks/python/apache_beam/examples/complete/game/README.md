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

This directory holds a series of example Dataflow pipelines in a simple 'mobile
gaming' domain. Each pipeline successively introduces new concepts.

In the gaming scenario, many users play, as members of different teams, over
the course of a day, and their actions are logged for processing. Some of the
logged game events may be late-arriving, if users play on mobile devices and go
transiently offline for a period.

The scenario includes not only "regular" users, but "robot users", which have a
higher click rate than the regular users, and may move from team to team.

The first two pipelines in the series use pre-generated batch data samples.

All of these pipelines write their results to Google BigQuery table(s).

## The pipelines in the 'gaming' series

### user_score

The first pipeline in the series is `user_score`. This pipeline does batch
processing of data collected from gaming events. It calculates the sum of
scores per user, over an entire batch of gaming data (collected, say, for each
day). The batch processing will not include any late data that arrives after
the day's cutoff point.

### hourly_team_score

The next pipeline in the series is `hourly_team_score`. This pipeline also
processes data collected from gaming events in batch. It builds on `user_score`,
but uses [fixed windows](https://beam.apache.org/documentation/programming-guide/#windowing),
by default an hour in duration. It calculates the sum of scores per team, for
each window, optionally allowing specification of two timestamps before and
after which data is filtered out. This allows a model where late data collected
after the intended analysis window can be included in the analysis, and any
late-arriving data prior to the beginning of the analysis window can be removed
as well.

By using windowing and adding element timestamps, we can do finer-grained
analysis than with the `UserScore` pipeline — we're now tracking scores for
each hour rather than over the course of a whole day. However, our batch
processing is high-latency, in that we don't get results from plays at the
beginning of the batch's time period until the complete batch is processed.

## Viewing the results in BigQuery

All of the pipelines write their results to BigQuery. `user_score` and
`hourly_team_score` each write one table. The pipelines have default table names
that you can override when you start up the pipeline if those tables already
exist.
