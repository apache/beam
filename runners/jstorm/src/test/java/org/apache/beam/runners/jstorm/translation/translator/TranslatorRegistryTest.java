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
package org.apache.beam.runners.jstorm.translation.translator;

import static org.junit.Assert.assertNull;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link TranslatorRegistry}.
 */
@RunWith(JUnit4.class)
public class TranslatorRegistryTest {

  @Test
  public void testGetTranslatorUnknown() throws Exception {
    assertNull(TranslatorRegistry.getTranslator(new UnknownPTransform()));
  }

  private class UnknownPTransform extends PTransform<PInput, POutput> {
    @Override
    public POutput expand(PInput input) {
      return null;
    }
  }
}
