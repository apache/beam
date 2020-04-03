/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.learning.katas.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log {

  private static final Logger LOG = LoggerFactory.getLogger(Log.class);

  private Log() {}

  public static <T> PTransform<PCollection<T>, PCollection<T>> ofElements() {
    return new LoggingTransform<>();
  }

  public static <T> PTransform<PCollection<T>, PCollection<T>> ofElements(String prefix) {
    return new LoggingTransform<>(prefix);
  }

  private static class LoggingTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private String prefix;

    private LoggingTransform() {
      prefix = "";
    }

    private LoggingTransform(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input.apply(
          ParDo.of(
              new DoFn<T, T>() {

                @ProcessElement
                public void processElement(
                    @Element T element, OutputReceiver<T> out, BoundedWindow window) {

                  String message = prefix + element.toString();

                  if (!(window instanceof GlobalWindow)) {
                    message = message + "  Window:" + window.toString();
                  }

                  LOG.info(message);

                  out.output(element);
                }
              }));
    }
  }
}
