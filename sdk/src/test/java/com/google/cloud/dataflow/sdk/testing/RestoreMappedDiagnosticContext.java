/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.testing;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import org.junit.rules.ExternalResource;
import org.slf4j.MDC;

import java.util.Map;

/**
 * Saves and restores the current MDC for tests.
 */
public class RestoreMappedDiagnosticContext extends ExternalResource {
  private Map<String, String> previousValue;

  public RestoreMappedDiagnosticContext() {
  }

  @Override
  protected void before() throws Throwable {
    previousValue = MoreObjects.firstNonNull(
        MDC.getCopyOfContextMap(),
        ImmutableMap.<String, String>of());
  }

  @Override
  protected void after() {
    MDC.setContextMap(previousValue);
  }
}
