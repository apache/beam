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
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.coders.Coder;

/**
 * {@link Coder} authors have the ability to automatically have their {@link Coder} registered with
 * the Dataflow Runner by creating a {@link ServiceLoader} entry and a concrete implementation of
 * this interface.
 *
 * <p>It is optional but recommended to use one of the many build time tools such as {@link
 * AutoService} to generate the necessary META-INF files automatically.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public interface CoderCloudObjectTranslatorRegistrar {
  /**
   * Gets a map from {@link Coder} to a {@link CloudObjectTranslator} that can translate that {@link
   * Coder}.
   */
  Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>> classesToTranslators();

  /**
   * Gets a map from the name returned by {@link CloudObject#getClassName()} to a translator that
   * can convert into the equivalent {@link Coder}.
   */
  Map<String, CloudObjectTranslator<? extends Coder>> classNamesToTranslators();
}
