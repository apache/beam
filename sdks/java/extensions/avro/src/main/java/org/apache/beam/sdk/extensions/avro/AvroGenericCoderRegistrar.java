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
package org.apache.beam.sdk.extensions.avro;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.util.construction.CoderTranslator;
import org.apache.beam.sdk.util.construction.CoderTranslatorRegistrar;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Coder registrar for AvroGenericCoder. */
@AutoService(CoderTranslatorRegistrar.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class AvroGenericCoderRegistrar implements CoderTranslatorRegistrar {
  public static final String AVRO_CODER_URN = "beam:coder:avro:generic:v1";

  @Override
  public Map<Class<? extends Coder>, String> getCoderURNs() {
    return ImmutableMap.of(AvroGenericCoder.class, AVRO_CODER_URN);
  }

  @Override
  public Map<Class<? extends Coder>, CoderTranslator<? extends Coder>> getCoderTranslators() {
    return ImmutableMap.of(AvroGenericCoder.class, new AvroGenericCoderTranslator());
  }
}
