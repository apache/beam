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
package org.apache.beam.sdk.schemas.io;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;

/** Helper to generate a DLQ transform to write PCollection<Failure> to an external system. */
@Internal
public final class GenericDlq {
  private GenericDlq() {}

  private static final Map<String, GenericDlqProvider> PROVIDERS =
      Providers.loadProviders(GenericDlqProvider.class);

  @SuppressWarnings("dereference.of.nullable")
  public static PTransform<PCollection<Failure>, PDone> getDlqTransform(String fullConfig) {
    List<String> strings = Splitter.on(":").limit(2).splitToList(fullConfig);
    checkArgument(
        strings.size() == 2, "Invalid config, must start with `identifier:`. %s", fullConfig);
    String key = strings.get(0);
    String config = strings.get(1).trim();
    GenericDlqProvider provider = PROVIDERS.get(key);
    checkArgument(
        provider != null, "Invalid config, no DLQ provider exists with identifier `%s`.", key);
    return provider.newDlqTransform(config);
  }
}
