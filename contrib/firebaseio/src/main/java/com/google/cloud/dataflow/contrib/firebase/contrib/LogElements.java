/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not  use this file except  in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.contrib.firebase.contrib;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.event.SubstituteLoggingEvent;
import org.slf4j.impl.SimpleLogger;
import org.slf4j.impl.SimpleLoggerFactory;

/**
 * Logs a {@link PCollection} to a logger for the specified {@link Class}.
 */
public class LogElements<T> extends PTransform<PCollection<T>, PDone> {

  private final Level level;
  private final String className;

  public LogElements(Class<?> clazz, Level level){
    this.className = clazz.getName();
    this.level = level;
  }

  @Override
  public PDone apply(PCollection<T> input){
    input.apply(ParDo.of(new LoggingDoFn<T>(this.className, this.level)));
    return PDone.in(input.getPipeline());
  }

  /**
   * Given a PCollection of {@link LoggingEvent}s, log them to a simple logger (uses System.err).
   */
  public static class LoggingDoFn<T> extends DoFn<T, Void>{

    private final String className;
    private final Level level;
    private transient SimpleLogger logger;

    public LoggingDoFn(String className, Level level){
      this.className = className;
      this.level = level;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      logger = (SimpleLogger) new SimpleLoggerFactory().getLogger(this.className);
    }

    @Override
    public void processElement(DoFn<T, Void>.ProcessContext c) throws Exception {
      SubstituteLoggingEvent event = new SubstituteLoggingEvent();
      event.setMessage(c.element().toString());
      event.setLevel(this.level);
      logger.log(event);
    }
  }
}
