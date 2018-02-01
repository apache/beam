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

package org.apache.beam.runners.gearpump.translators.io;

import java.io.IOException;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * wrapper over BoundedSource for Gearpump DataSource API.
 */
public class BoundedSourceWrapper<T> extends GearpumpSource<T> {

  private static final long serialVersionUID = 8199570485738786123L;
  private final BoundedSource<T> source;

  public BoundedSourceWrapper(BoundedSource<T> source, PipelineOptions options) {
    super(options);
    this.source = source;
  }


  @Override
  protected Source.Reader<T> createReader(PipelineOptions options) throws IOException {
    return source.createReader(options);
  }
}
