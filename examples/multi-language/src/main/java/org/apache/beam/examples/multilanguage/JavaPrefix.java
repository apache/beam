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
package org.apache.beam.examples.multilanguage;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class JavaPrefix extends PTransform<PCollection<String>, PCollection<String>> {

  final String prefix;

  public JavaPrefix(String prefix) {
    this.prefix = prefix;
  }

  class AddPrefixDoFn extends DoFn<String, String> {

    @ProcessElement
    public void process(@Element String input, OutputReceiver<String> o) {
      o.output(prefix + input);
    }
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input.apply("AddPrefix", ParDo.of(new AddPrefixDoFn()));
  }
}
