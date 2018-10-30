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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Distinct;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.junit.Test;

/** NameBaseTranslationProvider testing scenarios. */
public class NameBaseTranslationProviderTest {

  private Join joinMock = mock(Join.class);

  private final TranslatorProvider translatorProvider =
      NameBasedTranslatorProvider.newBuilder()
          .setDefaultTranslationProvider(
              SimpleTranslatorProvider.newBuilder()
                  .registerTranslator(joinMock.getClass(), new JoinTranslator<>())
                  .build())
          .addNameBasedTranslation(
              joinMock.getClass(), new BroadcastHashJoinTranslator<>(), "broadcast")
          .build();

  @Test
  public void testSimpleFindTranslator() {
    when(joinMock.getName()).thenReturn(Optional.empty());
    assertEquals(getTranslatorClass(joinMock), JoinTranslator.class);
  }

  @Test
  public void testNameBasedFindTranslator() {
    when(joinMock.getName()).thenReturn(Optional.of("broadcast"));
    assertEquals(getTranslatorClass(joinMock), BroadcastHashJoinTranslator.class);
  }

  @Test
  public void testNonMatchingName() {
    when(joinMock.getName()).thenReturn(Optional.of("hello"));
    assertEquals(getTranslatorClass(joinMock), JoinTranslator.class);
  }

  @Test
  public void testNoAvailableTranslator() {
    Distinct distinctMock = mock(Distinct.class);
    assertEquals(getTranslator(distinctMock), Optional.empty());
  }

  <OperatorT extends Operator<Object>>
      Optional<OperatorTranslator<Object, Object, OperatorT>> getTranslator(OperatorT operatorT) {
    return translatorProvider.findTranslator(operatorT);
  }

  <OperatorT extends Operator<Object>> Class<?> getTranslatorClass(OperatorT operatorT) {
    return getTranslator(operatorT).get().getClass();
  }
}
