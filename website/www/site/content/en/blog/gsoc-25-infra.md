---
title:  "Google Summer of Code 25 - Improving Apache Beam's Infrastructure"
date:   2025-09-15 00:00:00 -0600
categories:
  - blog
  - gsoc
aliases:
  - /blog/2025/09/15/gsoc-25-infra.html
authors:
  - ksobrenat32

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

I loved contributing to Apache Beam during Google Summer of Code 2025. I worked on improving the infrastructure of Apache Beam, which included enhancing the CI/CD pipelines, automating various tasks, and improving the overall developer experience.

## Motivation

Since I was in high school, I have been fascinated by computers, but when I discovered Open Source, I was amazed by the idea of people from all around the world collaborating to build software that anyone can use, just for the love of it. I started participating in open source communities, and I found it to be a great way to learn and grow as a developer.

When I heard about Google Summer of Code, I saw it as an opportunity to take my open source contributions to the next level. The idea of working on a real-world project while being mentored by experienced developers sounded like an amazing opportunity. I heard about Apache Beam from another contributor and ex-GSoC participant, and I was immediately drawn to the project, specifically on the infrastructure side of things, as I have a strong interest in DevOps and automation.

## The Challenge

When searching for a project, I was told that Apache Beam's infrastructure had several areas that could be improved. I was excited because the ideas were focused on improving the developer experience, and creating tools that could benefit not only Beam's developers but also the wider open source community.

There were four main challenges:

1. Automating the cleanup of unused cloud resources to reduce costs and improve resource management.
2. Implementing a system for managing permissions through Git, allowing for better tracking and auditing of changes.
3. Creating a tool for rotating service account keys to enhance security.
4. Developing a security monitoring system to detect and respond to potential threats.

## The Solution

I worked closely with my mentor to break down and define each challenge into manageable tasks, creating a plan for the summer. I started by taking a look at the current state of the infrastructure, after which I began working on each challenge one by one.

1. **Automating the cleanup of unused cloud resources:** We noticed that some resources in the GCP project, especially Pub/Sub topics created for testing, were often forgotten, leading to unnecessary costs. Since the infrastructure is primarily for testing and development, there's no need to keep unused resources. I developed a Python script that identifies and removes stale Pub/Sub topics that have existed for too long. This tool is now scheduled to run periodically via a GitHub Actions workflow to keep the project tidy and cost-effective.

2. **Implementing a system for managing permissions through Git:** This was more challenging, as it required a good understanding of both GCP IAM and the existing workflow. After some investigation, I learned that the current process was mostly manual and error-prone. The task involved creating a more automated and reliable system. This was achieved by using Terraform to define the desired state of IAM roles and permissions in code, which allows for better tracking and auditing of changes. This also included some custom roles, but that is still a work in progress.

3. **Creating a tool for rotating service account keys:** Key rotation is a security practice that we don't always follow, but it is essential to ensure that service account keys are not compromised. I noticed that GCP had some APIs that could help with this, but the rotation process itself was not automated. So I wrote a Python script that automates the rotation of GCP service account keys, enhancing the security of service account credentials.

4. **Developing a security monitoring system:** To keep track of incorrect usage and potential threats, I built a log analysis tool that monitors GCP audit logs for suspicious activity, collecting and parsing logs to identify potential security threats, delivering email alerts when something unusual is detected.

As an extra, and after noticing that some of these tools and policies could be ignored by developers, we also came up with the idea of an enforcement module to ensure the usage of these new tools and policies. This module would be integrated into the CI/CD pipeline, checking for compliance with the new infrastructure policies and notifying developers of any violations.

## The Impact

The tools developed during this project will have an impact on the Apache Beam community and the wider open source community. The automation of resource cleanup will help reduce costs and improve resource management, while the permission management system will provide better tracking and auditing of changes. The service account key rotation tool will enhance security, and the security monitoring system will help detect and respond to potential threats.

## Wrap Up

This project has been an incredible learning experience for me. I have gained a better understanding of how GCP works, as well as how to use Terraform and GitHub Actions. I have also learned a lot about security best practices and how to implement them in a real-world project.

I also learned a lot about working in an open source community, having direct communication with such experienced developers, and the importance of collaboration and communication in a distributed team. I am grateful for the opportunity to work on such an important project and to contribute to the Apache Beam community.

## Advice for Future Participants

If you are considering participating in Google Summer of Code, my advice would be to choose an area you are passionate about; this will make any coding challenge easier to overcome. Also, don't be afraid to ask questions and seek help from your mentors and the community. At the start, I made that mistake, and I learned that asking for help is a sign of strength, not weakness.

Finally, make sure to manage your time effectively and stay organized (keeping a progress journal is a great idea). GSoC is a great opportunity to learn and grow as a developer, but it can also be time-consuming, so it's important to stay focused and on track.
