/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.apex.translation.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple.WatermarkTuple;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Apex operator for Beam {@link PCollections}. */
public class ApexFlattenOperator<InputT> extends BaseOperator {

  private static final Logger LOG = LoggerFactory.getLogger(ApexFlattenOperator.class);
  private boolean traceTuples = false;

  private long inputWM1;
  private long inputWM2;
  private long outputWM;

  public int data1Tag;
  public int data2Tag;

  /** Data input port 1. */
  public final transient DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>> data1 =
      new DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>>() {
        /** Emits to port "out" */
        @Override
        public void process(ApexStreamTuple<WindowedValue<InputT>> tuple) {
          if (tuple instanceof WatermarkTuple) {
            WatermarkTuple<?> wmTuple = (WatermarkTuple<?>) tuple;
            if (wmTuple.getTimestamp() > inputWM1) {
              inputWM1 = wmTuple.getTimestamp();
              if (inputWM1 <= inputWM2) {
                // move output watermark and emit it
                outputWM = inputWM1;
                if (traceTuples) {
                  LOG.debug("\nemitting watermark {}\n", outputWM);
                }
                out.emit(tuple);
              }
            }
            return;
          }
          if (traceTuples) {
            LOG.debug("\nemitting {}\n", tuple);
          }

          if (data1Tag > 0 && tuple instanceof ApexStreamTuple.DataTuple) {
            ((ApexStreamTuple.DataTuple<?>) tuple).setUnionTag(data1Tag);
          }
          out.emit(tuple);
        }
      };

  /** Data input port 2. */
  public final transient DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>> data2 =
      new DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>>() {
        /** Emits to port "out" */
        @Override
        public void process(ApexStreamTuple<WindowedValue<InputT>> tuple) {
          if (tuple instanceof WatermarkTuple) {
            WatermarkTuple<?> wmTuple = (WatermarkTuple<?>) tuple;
            if (wmTuple.getTimestamp() > inputWM2) {
              inputWM2 = wmTuple.getTimestamp();
              if (inputWM2 <= inputWM1) {
                // move output watermark and emit it
                outputWM = inputWM2;
                if (traceTuples) {
                  LOG.debug("\nemitting watermark {}\n", outputWM);
                }
                out.emit(tuple);
              }
            }
            return;
          }
          if (traceTuples) {
            LOG.debug("\nemitting {}\n", tuple);
          }

          if (data2Tag > 0 && tuple instanceof ApexStreamTuple.DataTuple) {
            ((ApexStreamTuple.DataTuple<?>) tuple).setUnionTag(data2Tag);
          }
          out.emit(tuple);
        }
      };

  /** Output port. */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ApexStreamTuple<WindowedValue<InputT>>> out =
      new DefaultOutputPort<>();
}
