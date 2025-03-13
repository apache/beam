---
title:  "Big Improvements in Beam Go's 2.40 Release"
date:   2022-07-06 00:00:01 -0800
categories:
  - blog
  - go
authors:
  - damccorm
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

The 2.40 release is one of Beam Go's biggest yet, and we wanted to highlight
some of the biggest changes coming with this important release!

# Native Streaming Support

2.40 marks the release of one of our most anticipated feature sets yet:
native streaming Go pipelines. This includes adding support for:

- [Self Checkpointing](/documentation/programming-guide/#user-initiated-checkpoint)
- [Watermark Estimation](/documentation/programming-guide/#watermark-estimation)
- [Pipeline Drain/Truncation](/documentation/programming-guide/#truncating-during-drain)
- [Bundle Finalization](/documentation/programming-guide/#bundle-finalization) (added in 2.39)

With all of these features, it is now possible to write your own streaming
pipeline source DoFns in Go without relying on cross-language transforms
from Java or Python. We encourage you to try out all of these new features
in your streaming pipelines! The [programming guide](/documentation/programming-guide/#splittable-dofns)
has additional information on getting started with native Go streaming DoFns.

# Generic Registration (Make Your Pipelines 3x Faster)

The release of [Go Generics](https://go.dev/blog/intro-generics) in Go 1.18
unlocked significant performance improvements for Beam Go. With generics,
we were able to add simple registration functions that can massively reduce
your pipeline's runtime and resource consumption. For example, registering
the ParDo's in our load tests which are designed to simulate a basic pipeline
reduced execution time from around 25 minutes to around 7 minutes on average
- an over 70% reduction!

<img class="center-block"
     src="/images/blog/go-registration.png"
     alt="Beam Registration Load Tests ParDo Improvements">

To get started with registering your own DoFns and unlocking these performance
gains, check out the [registration doc page](https://pkg.go.dev/github.com/apache/beam/sdks/go/pkg/beam/register).

# What's Next?

Moving forward, we remain focused on improving the streaming experience and
leveraging generics to improve the SDK. Specific improvements we are considering
include adding [State & Timers](/documentation/programming-guide/#state-and-timers)
support, introducing a Go expansion service so that Go DoFns can be used in other
languages, and wrapping more Java and Python IOs so that they can be easily used
in Go. As always, please let us know what changes you would like to see by
[filing an issue](https://github.com/apache/beam/issues/new/choose),
[emailing the dev list](dev@beam.apache.org), or starting a [slack thread](https://app.slack.com/client/T4S1WH2J3/C9H0YNP3P)!
