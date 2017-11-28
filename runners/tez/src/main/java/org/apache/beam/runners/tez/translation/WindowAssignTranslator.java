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
package org.apache.beam.runners.tez.translation;

import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.Assign;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Assign} translation to Tez equivalent.
 */
class WindowAssignTranslator<T> implements TransformTranslator<Window.Assign<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(WindowAssignTranslator.class);

  @Override
  public void translate(Assign<T> transform, TranslationContext context) {
    //TODO: Translate transform to Tez and add to TranslationContext
  }
}