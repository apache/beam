---
title: "Performance Glossary"
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

# Metrics Glossary

The metrics glossary defines various metrics presented in the visualizations,
measured using the [Beam Metrics API](/documentation/programming-guide/#metrics)
from a pipeline Job running on [Dataflow](/documentation/runners/dataflow/).

## AvgCpuUtilization

The mean CPU usage of the pipeline Job measured as a percentage.

## AvgInputThroughputBytesPerSec

The mean input throughput of the pipeline Job measured in bytes per second.

## AvgInputThroughputElementsPerSec

The mean elements input throughput per second of the pipeline Job.

## AvgOutputThroughputBytesPerSec

The mean output throughput of the pipeline Job measured in bytes per second.

## AvgOutputThroughputElementsPerSec

The mean elements output throughput per second of the pipeline Job.

## BillableShuffleDataProcessed

The amount of data processed using shuffle that is billable.

## ElapsedTime

The elapsed time of the pipeline Job.

## EstimatedCost

The estimated cost of the pipeline Job.

## EstimatedDataProcessedGB

The estimated amount of data processed by the pipeline Job, measured in
gigabytes (Gb).

## MaxCpuUtilization

The maximum CPU usage of the pipeline Job measured as a percentage.

## MaxInputThroughputBytesPerSec

The maximum input throughput of the pipeline Job measured in bytes per second.

## MaxInputThroughputElementsPerSec

The maximum number of elements throughput input per second of the pipeline Job.

## MaxOutputThroughputBytesPerSec

The maximum output throughput of the pipeline Job measured in bytes per second.

## MaxOutputThroughputElementsPerSec

The maximum number of elements throughput output per second of the pipeline Job.

## PeakShuffleSlotCount

The peak count of shuffle slots.

## RunTime

The time it took for the pipeline Job to run measured in seconds.

## TotalDcuUsage

Total Data Compute Unit (DCU) usage of the pipeline Job measure in percentage.

## TotalGpuTime

Total Graphics Processing Unit (GPU) time used by the pipeline Job.

## TotalMemoryUsage

Total memory usage of the pipeline Job.

## TotalPdUsage

Total Persistent Disk usage of the pipeline Job.

## TotalShuffleDataProcessed

The total amount of shuffled data processed by the pipeline Job.

## TotalSsdUsage

The total Solid State Disk usage of the pipeline Job.

## TotalStreamingDataProcessed

The total amount of streaming data processed by the pipeline Job.

## TotalVcpuTime

The total amount of virtual CPU time used by the pipeline Job.