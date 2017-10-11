---
layout: section
title: 'Design Principles in Beam'
section_menu: section-menu/contribute.html
permalink: /contribute/design-principles/
---

# Design Principles in the Apache Beam Project

Joshua Bloch’s [API Design Bumper Stickers](https://www.infoq.com/articles/API-Design-Joshua-Bloch) are a great list of what makes for good API design. In addition, we have specific design principles we follow in Beam.

* TOC
{:toc}

## Use cases

### Unify the model
Provide one model that works over both bounded (aka. batch) and unbounded (aka. streaming) datasets. Pay special attention to windows / triggers / state / timers, which often trip up folks used to a batch world.  Provide users with the right abstractions to adjust latency and completeness guarantees to cover both traditional batch and streaming use cases.

### Separate data shapes and runtime requirements
The model should focus on letting users describe their data and processing, without exposing any details of a specific runtime system. For example, bounded and unbounded describe the shape of data, but batch and streaming describe the behavior of specific runtime systems. Good test cases are to imagine a mythical micro-batching runner that sits somewhere between batch and streaming or a engine that dynamically switches between streaming and batch depending on the backlog.

### Make efficient things easy, rather than make easy things efficient
Don’t prevent efficiency for ease of use. Design APIs that provide the information necessary for efficiently executing at scale. Provide class hierarchies and wrappers to make the common cases simpler.

## Usability

### Validate Early
Validate constraints on graph shape, runner requirements, etc as early in the compile time - construction time - submission time - execution time spectrum as reasonably possible in order to provide a smoother user experience.

### Public APIs, like diamonds, are forever (at least until the next major version)
Backwards incompatible changes can only be made in the next major version. Because of the burden major versions place on users (code has to be modified, conflicting dependency nightmares, etc), we aim to do this infrequently. Clearly mark APIs that are considered experimental (may change at any point) and deprecated (will be removed in the next major version). Consider what APIs are more amenable to future changes (abstract classes vs. interfaces, etc.)

### Examples should be pedagogical
Canonical examples help people ingrain the principles. Design examples that teach complex concepts in modular chunks. If you can’t explain the concept easily, then the API isn’t right. Examples should withstand random copy-pasting.

## Extensibility

### Use PTransforms for modularity
Composite transformations (transformations formed by a subgraph of other transformations) are treated as first class objects. They can be named and applied directly in any pipeline to nicely encapsulate concepts. This removes the artificial separation between those built into PCollection and those provided by users. In addition, PTransforms can be used as a clear concept in graphical monitoring and provide a way to scope metadata like aggregators, logging, and resources. Use these when building pipelines.

### Keep Beam SDKs consistent
Beam SDKs should expose the complete set of concepts in the programming model. They should all use the same set of abstractions and be able to share conceptual documentation.

### When in ~~Rome~~ Python, do as the ~~Romans~~ Pythonians do
Each SDK must feel right to those who live and breath that language. Adapt the general Beam concepts into language-dependent styles when the benefits clearly outweigh the drawbacks.

### Encourage DSLs  
Many use cases or user communities can be served by provided ‘wrapper’ SDKs that provide a simpler or domain-specific set of abstractions that then build on a Beam SDK and take advantage of Beam Runners.

### Design for the model, not specific runners

The Beam APIs should serve all runners. Behind every runner-specific hook, there is a general principle in the model. Design APIs that generalize across multiple runners.
