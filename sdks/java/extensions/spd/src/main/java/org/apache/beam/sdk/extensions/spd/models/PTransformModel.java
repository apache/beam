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
package org.apache.beam.sdk.extensions.spd.models;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class PTransformModel implements StructuredModel {
  private String path;
  private String name;
  private String input;
  private PTransform<PCollection<Row>, PCollection<Row>> transform;

  public PTransformModel(
      String path,
      String name,
      String input,
      PTransform<PCollection<Row>, PCollection<Row>> transform) {
    this.path = path;
    this.name = name;
    this.input = input;
    this.transform = transform;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public String getName() {
    return name;
  }

  public String getInput() {
    if (input.contains("(")) {
      return "{{ " + input + " }}";
    } else {
      return input;
    }
  }

  public PCollection<Row> applyTo(PCollection<Row> input) {
    // TODO: Implement schema binding. Right now we just passthru which is wrong
    return input.apply(transform);
  }
}
