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
package org.apache.beam.sdk.extensions.euphoria.fluent;

import static java.util.Objects.requireNonNull;

import org.apache.beam.sdk.extensions.euphoria.core.client.io.DataSource;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;

/** Fluent version of the {@link org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow}. */
public class Flow {
  private final org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow wrap;

  Flow(org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow wrap) {
    this.wrap = requireNonNull(wrap);
  }

  public static Flow create(String name) {
    return new Flow(org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow.create(name));
  }

  public static Flow create(String name, Settings settings) {
    return new Flow(
        org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow.create(name, settings));
  }

  public <T> Dataset<T> read(DataSource<T> src) {
    return new Dataset<>(wrap.createInput(src));
  }
}
