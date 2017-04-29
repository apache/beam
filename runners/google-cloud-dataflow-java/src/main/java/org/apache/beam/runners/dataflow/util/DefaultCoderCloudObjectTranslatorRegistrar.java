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

package org.apache.beam.runners.dataflow.util;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;

/**
 * The {@link CoderCloudObjectTranslatorRegistrar} containing the default collection of
 * {@link Coder} {@link CloudObjectTranslator Cloud Object Translators}.
 */
@AutoService(CoderCloudObjectTranslatorRegistrar.class)
public class DefaultCoderCloudObjectTranslatorRegistrar
    implements CoderCloudObjectTranslatorRegistrar {
  public Map<String, CloudObjectTranslator<? extends Coder>> classNamesToTranslators() {
    // TODO: Add translators
    return Collections.emptyMap();
  }

  @Override
  public Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      classesToTranslators() {
    // TODO: Add translato
    return Collections.emptyMap();
  }
}
