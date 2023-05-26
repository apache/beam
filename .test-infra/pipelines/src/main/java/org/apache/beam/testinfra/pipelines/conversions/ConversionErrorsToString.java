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
package org.apache.beam.testinfra.pipelines.conversions;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConversionErrorsToString<T extends GeneratedMessageV3>
    extends PTransform<PCollection<ConversionError<T>>, PCollection<ConversionError<String>>> {

  public static <T extends GeneratedMessageV3> ConversionErrorsToString<T> create() {
    return new ConversionErrorsToString<>();
  }

  private static final Logger LOG = LoggerFactory.getLogger(ConversionErrorsToString.class);

  @Override
  public PCollection<ConversionError<String>> expand(PCollection<ConversionError<T>> input) {
    return input.apply("ConversionError source to JSON", ParDo.of(new ToStringFn<T>()));
  }

  private static final class ToStringFn<T extends GeneratedMessageV3>
      extends DoFn<ConversionError<T>, ConversionError<String>> {
    @ProcessElement
    public void process(
        @Element ConversionError<T> element, OutputReceiver<ConversionError<String>> receiver) {
      try {
        String json = JsonFormat.printer().print(element.getSource());
        receiver.output(
            ConversionError.<String>builder()
                .setSource(json)
                .setObservedTime(element.getObservedTime())
                .setMessage(element.getMessage())
                .setStackTrace(element.getStackTrace())
                .build());
      } catch (InvalidProtocolBufferException e) {
        LOG.warn("error converting {} to JSON string: {}", element, e.getMessage());
      }
    }
  }
}
