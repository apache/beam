---
title:  "Strata+Hadoop World and Beam"
date:   2016-10-11 09:00:00 -0800
categories:
  - beam
  - update
aliases:
  - /beam/update/2016/10/11/strata-hadoop-world-and-beam.html
authors:
  - jesseanderson
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

Tyler Akidau and I gave a [three-hour tutorial](https://conferences.oreilly.com/strata/hadoop-big-data-ny/public/schedule/detail/52129) on Apache Beam at Strata+Hadoop World 2016. We had a plethora of help from our TAs: Kenn Knowles, Reuven Lax, Felipe Hoffa, Slava Chernyak, and Jamie Grier. There were a total of 66 people that attended the session.<!--more-->

<img src="/images/blog/IMG_20160927_170956.jpg" alt="Exercise time">

If you want to take a look at the tutorial materials, we’ve put them up [on GitHub](https://github.com/eljefe6a/beamexample). This includes the [actual slides](https://github.com/eljefe6a/beamexample/blob/master/BeamTutorial/slides.pdf) as well as the [exercises](https://github.com/eljefe6a/beamexample/tree/master/BeamTutorial/src/main/java/org/apache/beam/examples/tutorial/game) that we covered. If you’re looking to learn a little about Beam, this is a good way to start. The exercises are based on an imaginary mobile game where data needs processing and are based on code in the [Beam examples directory](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete/game). The code has TODOs for where you need to fill in code or there are full sample solutions to look over our code. You can run these examples on your own machine or on a cluster using a runner that Beam supports.

I want to share some of takeaways I had about Beam during the conference.

The Data Engineers are looking to Beam as a way to [future-proof](https://www.oreilly.com/ideas/future-proof-and-scale-proof-your-code), meaning that code is portable between the various Big Data frameworks. In fact, many of the attendees were still on Hadoop MapReduce and looking to transition to a new framework. They’re realizing that continually rewriting code isn’t the most productive approach.

Data Scientists are really interested in using Beam. They interested in having a single API for doing analysis instead of several different APIs. We talked about Beam’s progress on the Python API. If you want to take a peek, it’s being actively developed on a [feature branch](https://github.com/apache/beam/tree/master/sdks/python). As Beam matures, we’re looking to add other supported languages.

We heard [loud and clear](https://twitter.com/jessetanderson/status/781124173108305920) from Beam users that great runner support is crucial to adoption. We have great Apache Flink support. During the conference we had some more volunteers offer their help on the Spark runner.

On management and thought leader side, Beam went from “what’s Beam?” at previous conferences to “I’m interested in Beam.” or “I’ve formed an informed opinion on Beam.” at this conference. This is one of the metrics I look for in early technology adoption.

<img src="/images/blog/IMG_20160927_170455.jpg" alt="So much brainpower answering questions">

We rounded out the tutorial with live demonstrations of Beam running on Apache Spark, Apache Flink, the local runner, and DataFlow runner. Then, we brought in the big brainpower and had a Q and A session.

If you’re attending a conference, we encourage you to look for a Beam session. If you want to use these materials to give your own Beam talk or tutorial, we’re happy to help you. In addition to this tutorial, we have [other presentation materials](/contribute/presentation-materials/). You can reach out to us on the [user mailing list](/get-started/support/).

