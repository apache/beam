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
package org.apache.beam.runners.tez.translation.io;

import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * Abstract Output Manager that adds before and after methods to the {@link DoFnRunners.OutputManager}
 * interface so that outputs that require them can be added and used with the TezRunner.
 */
public abstract class TezOutputManager implements DoFnRunners.OutputManager {

  private WindowedValue currentElement;
  private KeyValueWriter writer;
  private LogicalOutput output;

  public TezOutputManager(LogicalOutput output){
    this.output = output;
  }

  public void before() {}

  public void after() {}

  public void setCurrentElement(WindowedValue currentElement) {
    this.currentElement = currentElement;
  }

  public WindowedValue getCurrentElement(){
    return currentElement;
  }

  public void setWriter(KeyValueWriter writer) {
    this.writer = writer;
  }

  public KeyValueWriter getWriter() {
    return writer;
  }

  public LogicalOutput getOutput() {
    return output;
  }
}
