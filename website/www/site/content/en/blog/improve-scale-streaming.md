---
layout: post
title:  "Scaling a Dataflow streaming pipeline to 1 million events per second and beyond"
date:   2023-12-01 00:00:01 -0800
categories:
  - blog
authors:
  - pabs
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

Worth to mention that the described progression in this article maps the evolution of a real-life workload, with lots of simplifications; after the business requirements for the pipeline was achieved, we focused on optimizing the performance and on reducing the resources needed for the execution.

This article demonstrate how different configuration settings affect a specific streaming pipeline, and illustrate the reasoning behind some of those configuration settings which help the workload to the desired scale and beyond.

## Test setup

### Local Environment Requirements

## Workload Description

## First Run : Default settings

## Second Run : Remove bottleneck

## Third Run : Unleash autoscale

## Fourth Run : In with the new'ish (StorageWrites)

## Fifth Run : A better write format

## Sixth Run : Reduce translation effort

## Seventh Run : Lets relax (some constrains)
