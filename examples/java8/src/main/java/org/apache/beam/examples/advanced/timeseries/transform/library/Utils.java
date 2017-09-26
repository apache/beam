/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.examples.advanced.timeseries.transform.library;

import org.apache.beam.examples.advanced.timeseries.configuration.TSConfiguration;
import org.apache.beam.examples.advanced.timeseries.protos.TimeSeriesProtos.TSDataPoint;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utils for use with the timeseries BEAM examples.
 */
@SuppressWarnings("serial")
@Experimental
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Transform to create a Key Value pair from a TSDataPoint object.
   * @return PTransform
   */
  public static PTransform<PCollection<TSDataPoint>,
    PCollection<KV<String, TSDataPoint>>> createKVTSDataPoint() {
    return new CreateKVTSDataPoint();
  }

  /**
   * Transform to create a Key Value pair from a TSDataPoint object.
   */
  static class CreateKVTSDataPoint
      extends PTransform<PCollection<TSDataPoint>, PCollection<KV<String, TSDataPoint>>> {
    @Override
    public PCollection<KV<String, TSDataPoint>> expand(PCollection<TSDataPoint> input) {
      return input.apply(ParDo.of(new DoFn<TSDataPoint, KV<String, TSDataPoint>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(KV.of(c.element().getKey().getKey(), c.element()));
        }
      }));
    }
  }

  /**
   * Attach time stamp to Process Context.
   * @return PTransform
   */
  public static PTransform<PCollection<TSDataPoint>, PCollection<TSDataPoint>> extractTimeStamp() {
    return new ExtractTimeStamp();
  }

  /**
   * Attach time stamp to Process Context.
   */
  static class ExtractTimeStamp
      extends PTransform<PCollection<TSDataPoint>, PCollection<TSDataPoint>> {

    @Override
    public PCollection<TSDataPoint> expand(PCollection<TSDataPoint> input) {

      return input.apply(ParDo.of(new DoFn<TSDataPoint, TSDataPoint>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.outputWithTimestamp(c.element(),
              new Instant(c.element().getTimestamp().getSeconds() * 1000));
        }
      }));
    }
  }

  /**
   * Transform to test Start and End time are in tolerances.
   */
  public static class CheckConfiguration
      extends PTransform<PCollection<TSDataPoint>, PCollection<TSDataPoint>> {

    TSConfiguration configuration;

    public CheckConfiguration(TSConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollection<TSDataPoint> expand(PCollection<TSDataPoint> input) {

      return input.apply(ParDo.of(new DoFn<TSDataPoint, TSDataPoint>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          if (!configuration.isStreaming()
              && (c.timestamp().getMillis() > configuration.endTime().getMillis())) {
            String err =
                String.format("Time stamp %s past Configuration EndTime %s found in batch mode",
                    c.timestamp().getMillis(), configuration.endTime().getMillis());
            LOG.error(err);
            throw new IllegalStateException(err);
          }
          if (c.timestamp().getMillis() < configuration.startTime().getMillis()) {
            String err = String.format("Time stamp %s before Configuration StartTime %s found",
                c.timestamp().getMillis(), configuration.startTime().getMillis());
            LOG.error(err);
            throw new IllegalStateException(err);
          }

          c.output(c.element());
        }
      }));
    }
  }

  /**
   * Calculate the number of fixed windows between end and start time.
   * @param TSConfiguration
   * @return long
   */
  public static long calculateNumWindows(TSConfiguration configuration) {

    LOG.info(
        String.format("Computing Number of windows using startTime %s, endTime %s and Duration %s",
            configuration.startTime(), configuration.endTime(),
            configuration.downSampleDuration().getMillis()));

    double result =
        Math.ceil((configuration.endTime().getMillis() - configuration.startTime().getMillis())
            / configuration.downSampleDuration().getMillis());
    return (long) result;
  }
}
