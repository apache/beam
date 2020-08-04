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
package org.apache.beam.sdk.extensions.euphoria.core.translate.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTranslator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

class ProviderTestUtils {

  interface Named {
    String getName();
  }

  /** Dummy {@link OperatorTranslator} used for testing purposes. */
  static class TestOpTranslator implements OperatorTranslator<Void, Void, TestOperator>, Named {
    private final String name;
    private final boolean canTranslate;

    TestOpTranslator(String name, boolean canTranslate) {
      this.name = name;
      this.canTranslate = canTranslate;
    }

    static TestOpTranslator ofName(String name) {
      return new TestOpTranslator(name, true);
    }

    static TestOpTranslator of(String name, boolean canTranslate) {
      return new TestOpTranslator(name, canTranslate);
    }

    @Override
    public PCollection<Void> translate(TestOperator operator, PCollectionList<Void> inputs) {
      throw new IllegalStateException("Not meant to actually translate something.");
    }

    @Override
    public boolean canTranslate(TestOperator operator) {
      return canTranslate;
    }

    @Override
    public String getName() {
      return name;
    }
  }

  /** Dummy {@link OperatorTranslator} used for testing purposes. */
  static class AnyOpTranslator implements OperatorTranslator<Void, Void, Operator>, Named {
    private final String name;
    private final boolean canTranslate;

    AnyOpTranslator(String name, boolean canTranslate) {
      this.name = name;
      this.canTranslate = canTranslate;
    }

    static AnyOpTranslator ofName(String name) {
      return new AnyOpTranslator(name, true);
    }

    static AnyOpTranslator of(String name, boolean canTranslate) {
      return new AnyOpTranslator(name, canTranslate);
    }

    @Override
    public PCollection<Void> translate(Operator operator, PCollectionList<Void> inputs) {
      throw new IllegalStateException("Not meant to actually translate something.");
    }

    @Override
    public boolean canTranslate(Operator operator) {
      return canTranslate;
    }

    @Override
    public String getName() {
      return name;
    }
  }

  /** Dummy {@link Operator} used for testing purposes. */
  static class TestOperator extends Operator<Void> {

    TestOperator(@Nullable String name) {
      super(name, TypeDescriptors.voids());
    }

    static TestOperator of() {
      return new TestOperator(null);
    }
  }

  /** Dummy {@link Operator} used for testing purposes. */
  static class SecondTestOperator extends Operator<Void> {

    SecondTestOperator(@Nullable String name) {
      super(name, TypeDescriptors.voids());
    }

    static SecondTestOperator of() {
      return new SecondTestOperator(null);
    }
  }

  static void assertTranslator(
      String translatorId,
      Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator,
      Class<? extends OperatorTranslator> translatorClass) {
    assertNotNull(maybeTranslator);
    assertTrue(maybeTranslator.isPresent());
    OperatorTranslator<Void, Void, TestOperator> translator = maybeTranslator.get();
    assertEquals(translator.getClass(), translatorClass);

    Named namedTranslator = (Named) translator;
    assertEquals(translatorId, namedTranslator.getName());
  }
}
