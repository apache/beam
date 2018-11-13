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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.ProviderTestUtils.AnyOpTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.ProviderTestUtils.SecondTestOperator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.ProviderTestUtils.TestOpTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.ProviderTestUtils.TestOperator;
import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link GenericTranslatorProvider}. */
public class GenericTranslatorProviderTest {

  @Test
  public void testBuild() {

    GenericTranslatorProvider builded = GenericTranslatorProvider.newBuilder().build();

    Assert.assertNotNull(builded);
  }

  @Test
  public void testClassToTranslatorRegistration() {

    String translatorName = "translator";
    GenericTranslatorProvider provider =
        GenericTranslatorProvider.newBuilder()
            .register(TestOperator.class, TestOpTranslator.ofName(translatorName))
            .build();
    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator =
        provider.findTranslator(TestOperator.of());

    ProviderTestUtils.assertTranslator(translatorName, maybeTranslator, TestOpTranslator.class);
  }

  @Test
  public void testClassWithPredicateToTranslatorRegistration() {

    String translatorName = "translator";
    GenericTranslatorProvider provider =
        GenericTranslatorProvider.newBuilder()
            .register(TestOperator.class, op -> true, TestOpTranslator.ofName(translatorName))
            .build();

    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator =
        provider.findTranslator(TestOperator.of());

    ProviderTestUtils.assertTranslator(translatorName, maybeTranslator, TestOpTranslator.class);
  }

  @Test
  public void testPredicateWithPredicateToTranslatorRegistration() {

    String translatorName = "translator";
    GenericTranslatorProvider provider =
        GenericTranslatorProvider.newBuilder()
            .register(op -> true, AnyOpTranslator.ofName(translatorName))
            .build();

    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator =
        provider.findTranslator(TestOperator.of());

    ProviderTestUtils.assertTranslator(translatorName, maybeTranslator, AnyOpTranslator.class);
  }

  @Test
  public void testClassWithPredicateToTranslatorFunction() {

    AtomicBoolean predicateEvalValue = new AtomicBoolean(false);

    String translatorName = "translator";
    GenericTranslatorProvider provider =
        GenericTranslatorProvider.newBuilder()
            .register(
                TestOperator.class,
                op -> predicateEvalValue.get(),
                TestOpTranslator.ofName(translatorName))
            .build();

    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator;
    maybeTranslator = provider.findTranslator(TestOperator.of());

    Assert.assertFalse(maybeTranslator.isPresent());

    predicateEvalValue.set(true);
    // now predicate will return true and we should get our translator
    maybeTranslator = provider.findTranslator(TestOperator.of());
    ProviderTestUtils.assertTranslator(translatorName, maybeTranslator, TestOpTranslator.class);

    // we should not obtain operator for different operator class
    Optional<OperatorTranslator<Void, Void, SecondTestOperator>> maybeSecondTranslator =
        provider.findTranslator(SecondTestOperator.of());
    Assert.assertFalse(maybeSecondTranslator.isPresent());
  }

  @Test
  public void testPredicateToTranslatorFunction() {

    AtomicBoolean predicateEvalValue = new AtomicBoolean(false);

    String translatorName = "translator";
    GenericTranslatorProvider provider =
        GenericTranslatorProvider.newBuilder()
            .register(op -> predicateEvalValue.get(), AnyOpTranslator.ofName(translatorName))
            .build();

    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator;
    maybeTranslator = provider.findTranslator(TestOperator.of());
    Assert.assertFalse(maybeTranslator.isPresent());

    Optional<OperatorTranslator<Void, Void, SecondTestOperator>> maybeSecondTranslator =
        provider.findTranslator(SecondTestOperator.of());
    Assert.assertFalse(maybeSecondTranslator.isPresent());

    predicateEvalValue.set(true);
    // now predicate will return true and we should get our translator
    maybeTranslator = provider.findTranslator(TestOperator.of());
    ProviderTestUtils.assertTranslator(translatorName, maybeTranslator, AnyOpTranslator.class);

    // we should get our translator for every operator's type
    maybeSecondTranslator = provider.findTranslator(SecondTestOperator.of());
    Assert.assertTrue(maybeSecondTranslator.isPresent());
  }
}
