Prompt:
What are the metrics in Apache Beam?

Response:
In the Apache Beam model, metrics provide insights into the current state of your pipeline, including during pipeline execution.

Metrics are named and scoped to a specific step in the pipeline. They can be created dynamically during pipeline execution. If a runner doesn't support some part of reporting metrics, the fallback behavior is to drop the metric updates rather than fail the pipeline.

Apache Beam provides a number of built-in metric types:
* Counters
* Distributions
* Gauges

To declare a metric, use the `beam.metrics.Metrics` class. For example:

```python
self.words_counter = Metrics.counter(self.__class__, 'words')
self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
self.word_lengths_dist = Metrics.distribution(self.__class__, 'word_len_dist')
self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')
```

For implementation details, see the WordCount example with metrics in the Apache Beam GitHub repository.

You can export metrics to external sinks. Spark and Flink runners support REST HTTP and Graphite.
