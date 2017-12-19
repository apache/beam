package org.apache.beam.runners.core.metrics;

import java.io.Serializable;
import org.apache.beam.sdk.metrics.MetricQueryResults;

/**
 * A marshaller to convert MetricQueryResult as an output format.
 */
public interface MetricsSerializer<OutputT> extends Serializable{

  OutputT serializeMetrics(MetricQueryResults metricQueryResults) throws Exception;

}
