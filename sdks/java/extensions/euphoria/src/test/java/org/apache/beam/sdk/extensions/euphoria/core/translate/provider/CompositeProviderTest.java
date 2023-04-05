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
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.TranslatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.ProviderTestUtils.TestOpTranslator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.ProviderTestUtils.TestOperator;
import org.junit.Test;

/** Units tests of {@link CompositeProvider}. */
public class CompositeProviderTest {

  private final TestOperator testOperator = new TestOperator("test-operator");

  @Test
  public void testOneComposite() {
    String translatorId = "first";
    CompositeProvider compositeProvider =
        CompositeProvider.of(TestProvider.ofTranslator(translatorId));

    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator =
        compositeProvider.findTranslator(testOperator);

    ProviderTestUtils.assertTranslator(translatorId, maybeTranslator, TestOpTranslator.class);
  }

  @Test
  public void testCompositesOrder() {

    String firstTranslatorId = "first";
    String secondTranslatorId = "second";

    CompositeProvider compositeProvider =
        CompositeProvider.of(
            TestProvider.ofTranslator(firstTranslatorId),
            TestProvider.ofTranslator(secondTranslatorId));

    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator =
        compositeProvider.findTranslator(testOperator);

    ProviderTestUtils.assertTranslator(firstTranslatorId, maybeTranslator, TestOpTranslator.class);
  }

  @Test
  public void testProviderCannotTranslate() {

    String firstTranslatorId = "first";
    String secondTranslatorId = "second";

    CompositeProvider compositeProvider =
        CompositeProvider.of(
            TestProvider.ofTtranslatorWhichCannotTranslate(firstTranslatorId),
            TestProvider.ofTranslator(secondTranslatorId));

    Optional<OperatorTranslator<Void, Void, TestOperator>> maybeTranslator =
        compositeProvider.findTranslator(testOperator);

    ProviderTestUtils.assertTranslator(secondTranslatorId, maybeTranslator, TestOpTranslator.class);
  }

  private static class TestProvider implements TranslatorProvider {
    private final OperatorTranslator<?, ?, ?> opTranslator;

    private TestProvider(TestOpTranslator opTranslator) {
      this.opTranslator = opTranslator;
    }

    static TestProvider ofTranslator(String translatorId) {
      return new TestProvider(new TestOpTranslator(translatorId, true));
    }

    static TestProvider ofTtranslatorWhichCannotTranslate(String translatorId) {
      return new TestProvider(new TestOpTranslator(translatorId, false));
    }

    @Override
    public <InputT, OutputT, OperatorT extends Operator<OutputT>>
        Optional<OperatorTranslator<InputT, OutputT, OperatorT>> findTranslator(
            OperatorT operator) {
      if (operator.getClass().equals(TestOperator.class)) {

        @SuppressWarnings("unchecked")
        OperatorTranslator<InputT, OutputT, OperatorT> opTranslator =
            (OperatorTranslator<InputT, OutputT, OperatorT>) this.opTranslator;

        return Optional.of(opTranslator);
      }
      return Optional.empty();
    }
  }
}
