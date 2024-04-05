Prompt:
What is a splittable `DoFn` in Apache Beam?

Response:
A splittable `DoFn` (SDF) is a generalization of [`DoFn`](https://beam.apache.org/documentation/programming-guide/#pardo) that lets you process elements in a non-monolithic way. A splittable `DoFn` makes it easier to create complex, modular I/O connectors in Apache Beam.
When you apply a splittable `DoFn` to an element, the runner can split the element’s processing into smaller tasks. You can checkpoint the processing of an element, and you can split the remaining work to yield additional parallelism.

At a high level, an SDF is responsible for processing element and restriction pairs. A restriction represents a subset of work that would have been necessary to have been done when processing the element.

Executing a [splittable `DoFn`](https://beam.apache.org/documentation/programming-guide/#splittable-dofns) uses the following steps:
1. Each element is paired with a restriction. For example, filename is paired with offset range representing the whole file.
2. Each element and restriction pair is split. For example, offset ranges are broken up into smaller pieces.
3. The runner redistributes the element and restriction pairs to several workers.
4. Element and restriction pairs are processed in parallel. For example, the file is read. Within this last step, the element and restriction pair can pause its own processing or be split into further element and restriction pairs.

For an example, see the [Splittable `DoFn` module](https://tour.beam.apache.org/tour/python/splittable-dofn/splittable) in the Tour of Beam.

For more information, see the [community blog post](https://beam.apache.org/blog/splittable-do-fn-is-available/).
