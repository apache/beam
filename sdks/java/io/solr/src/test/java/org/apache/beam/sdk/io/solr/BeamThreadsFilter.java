package org.apache.beam.sdk.io.solr;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Filter out Beam leak threads.
 */
public class BeamThreadsFilter implements ThreadFilter {

  @Override public boolean reject(Thread t) {
    return t.getName().startsWith("direct-metrics-counter");
  }
}
