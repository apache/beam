---
title:  "Contributor Spotlight: Johanna Öjeling"
date:   2023-11-11 15:00:00 -0800
categories:
  - blog
authors:
  - altay
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

Johanna Öjeling is a Senior Software Engineer at [Normative](https://normative.io/). She started using Apache Beam in 2020 at her previous company [Datatonic](http://datatonic.com) and began contributing in 2022 at a personal capacity. We interviewed Johanna to learn more about her interests and we hope that this will inspire new, future, diverse set of contributors to participate in OSS projects.

**What areas of interest are you passionate about in your career?**

My core interest lies in distributed and data-intensive systems, and I enjoy working on challenges related to performance, scalability and maintainability. I also feel strongly about developer experience, and like to build tools and frameworks that make developers happier and more productive. Aside from that, I take pleasure in mentoring and coaching other software engineers to grow their skills and pursue a fulfilling career.

**What motivated you to make your first contribution?**

I was already a user of the Apache Beam Java and Python SDKs and Google Cloud Dataflow in my previous job, and had started to play around with the Go SDK to learn Go. When I noticed that a feature I wanted was missing, it seemed like a great opportunity to implement it. I had been curious about developing open source software for some time, but did not have a good idea until then of what to contribute with.

**In which way have you contributed to Apache Beam?**

I have primarily worked on the Go SDK with implementation of new features, bug fixes, tests, documentation and code reviews. Some examples include a MongoDB I/O connector with dynamically scalable reads and writes, a file I/O connector supporting continuous file discovery, and an Amazon S3 file system implementation.

**How has your open source engagement impacted your personal or professional growth?**

Contributing to open source is one of the best decisions I have taken professionally. The Beam community has been incredibly welcoming and appreciative, and it has been rewarding to collaborate with talented people around the world to create software that is free for anyone to benefit from. Open source has opened up new opportunities to challenge myself, dive deeper into technologies I like, and learn from highly skilled professionals. To me, it has served as an outlet for creativity, problem solving and purposeful work.

**How have you noticed contributing to open source is different from contributing to closed source/proprietary software?**

My observation has been that there are higher requirements for software quality in open source, and it is more important to get things right the first time. My closed source software experience is from startups/scale-ups where speed is prioritized. When not working on public facing APIs or libraries, one can also more easily change things, whereas we need to be mindful about breaking changes in Beam. I care for software quality and value the high standards the Beam committers hold.

**What do you like to do with your spare time when you're not contributing to Beam?**

Coding is a passion of mine so I tend to spend a lot of my free time on hobby projects, reading books and articles, listening to talks and attending events. When I was younger I loved learning foreign languages and studied English, French, German and Spanish. Later I discovered an interest in computer science and switched focus to programming languages. I decided to change careers to software engineering and have tried to learn as much as possible ever since. I love that it never ends.

**What future features/improvements are you most excited about, or would you like to see on Beam?**

The multi-language pipeline support is an impressive feature of Beam, and I like that new SDKs such as TypeScript and Swift are emerging, which enables developers to write pipelines in their preferred language. Naturally, I am also excited to see where the Go SDK is headed and how we can make use of newer features of the Go language.

**What types of contributions or support do you think the Beam community needs more of?**

Many data and machine learning engineers feel more comfortable with Python than Java and wish the Python SDK were as feature rich as the Java SDK. This presents great opportunities for Python developers to start contributing to Beam. As an SDK author, one can take advantage of Beam's multiple SDKs. When I have developed in Go I have often studied the Java and Python implementations to get ideas for how to solve specific problems and make sure the Go SDK follows a similar pattern.

**What advice would you give to someone who wants to contribute but does not know where to begin?**

Start with asking yourself what prior knowledge you have and what you would like to learn, then look for opportunities that match that. The contribution guidelines will tell you where to find open issues and what the process looks like. There are tasks labeled as "good first issue" which can be a good starting point. I was quite nervous about making my first contribution and had my mentor pre-review my PR. There was no need to worry though, as people will be grateful for your effort to improve the project. The pride I felt when a committer approved my PR and welcomed me to Beam is something I still remember.

**What advice would you give to the Beam community? What could we improve?**

We can make it easier for new community members to get involved by providing more examples of tasks that we need help with, both in the form of code and non-code contributions. I will take it as an action point myself to label more issues accordingly and tailor the descriptions for newcomers. However, this is contingent on community members visiting the GitHub project. To address this, we could also proactively promote opportunities through social channels and the user mailing list.

*We thank Johanna for the interview and for her contributions! If you would like to learn more about contributing to Beam you can learn more about it here: https://beam.apache.org/contribute/.*
