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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.io.kafka.KafkaIOTest.mkKafkaReadTransform;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadProperties;
import org.apache.beam.sdk.io.kafka.KafkaIOTest.ValueAsTimestampFn;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaIOReadImplementationCompatibilityTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testKafkaIOReadPropertiesEnumValuePresence() {
    final Set<String> properties = getGetterPropertyNamesInUpperUnderscore(KafkaIO.Read.class);
    final Set<String> enums = getEnumValueNamesInUpperUnderscore(KafkaIOReadProperties.class);

    final Sets.SetView<String> missingEnums = Sets.difference(properties, enums);
    final Sets.SetView<String> unnecessaryEnums = Sets.difference(enums, properties);

    assertThat("There are missing 'KafkaIOReadProperties' enum values!", missingEnums, is(empty()));
    assertThat(
        "There are unnecessary 'KafkaIOReadProperties' enum values present!",
        unnecessaryEnums,
        is(empty()));
  }

  private static Set<String> getGetterPropertyNamesInUpperUnderscore(Class<?> clazz) {
    final Set<String> result = Sets.newLinkedHashSet();
    for (Method method : clazz.getDeclaredMethods()) {
      if (!Modifier.isStatic(method.getModifiers())) {
        if (!Void.TYPE.equals(method.getReturnType())) {
          if (method.getParameterCount() == 0) {
            for (String prefix : new String[] {"get", "is"}) {
              final String methodName = method.getName();
              if (methodName.startsWith(prefix) && methodName.length() > prefix.length()) {
                final String propertyName = methodName.substring(prefix.length());
                result.add(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, propertyName));
              }
            }
          }
        }
      }
    }
    return result;
  }

  private static Set<String> getEnumValueNamesInUpperUnderscore(
      Class<? extends Enum<?>> enumClazz) {
    return Stream.of(enumClazz.getEnumConstants()).map(v -> v.name()).collect(Collectors.toSet());
  }

  @Test
  public void testPrimitiveKafkaIOReadPropertiesDefaultValueExistence() {
    for (KafkaIOReadProperties properties : KafkaIOReadProperties.values()) {
      if (KafkaIOReadProperties.findGetterMethod(properties).getReturnType().isPrimitive()) {
        assertThat(
            "KafkaIOReadProperties." + properties + " should have a default value!",
            properties.getDefaultValue(),
            is(notNullValue()));
      }
    }
  }

  private PipelineResult testReadTransformCreationWithImplementationBoundProperties(
      Function<KafkaIO.Read<Integer, Long>, KafkaIO.Read<Integer, Long>> kafkaReadDecorator) {
    p.apply(
        kafkaReadDecorator.apply(
            mkKafkaReadTransform(
                1000,
                null,
                new ValueAsTimestampFn(),
                false, /*redistribute*/
                false, /*allowDuplicates*/
                0)));
    return p.run();
  }

  private Function<KafkaIO.Read<Integer, Long>, KafkaIO.Read<Integer, Long>>
      legacyDecoratorFunction() {
    return read -> read.withMaxReadTime(Duration.millis(10));
  }

  private Function<KafkaIO.Read<Integer, Long>, KafkaIO.Read<Integer, Long>>
      sdfDecoratorFunction() {
    return read -> read.withStopReadTime(Instant.ofEpochMilli(10));
  }

  @Test
  public void testReadTransformCreationWithLegacyImplementationBoundProperty() {
    PipelineResult r =
        testReadTransformCreationWithImplementationBoundProperties(legacyDecoratorFunction());
    String[] expect =
        KafkaIOTest.mkKafkaTopics.stream()
            .map(topic -> String.format("kafka:`%s`.%s", KafkaIOTest.mkKafkaServers, topic))
            .toArray(String[]::new);
    assertThat(Lineage.query(r.metrics(), Lineage.Type.SOURCE), containsInAnyOrder(expect));
  }

  @Test
  public void testReadTransformCreationWithSdfImplementationBoundProperty() {
    PipelineResult r =
        testReadTransformCreationWithImplementationBoundProperties(sdfDecoratorFunction());
    String[] expect =
        KafkaIOTest.mkKafkaTopics.stream()
            .map(topic -> String.format("kafka:`%s`.%s", KafkaIOTest.mkKafkaServers, topic))
            .toArray(String[]::new);
    assertThat(Lineage.query(r.metrics(), Lineage.Type.SOURCE), containsInAnyOrder(expect));
  }

  @Test
  public void testReadTransformCreationWithBothImplementationBoundProperties() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("every configured property");
    thrown.expectMessage("Not supported implementations");
    // SDF does not support maxReadTime
    thrown.expectMessage("SDF");
    thrown.expectMessage("MAX_READ_TIME");
    // legacy does not support stopReadTime
    thrown.expectMessage("LEGACY");
    thrown.expectMessage("STOP_READ_TIME");

    testReadTransformCreationWithImplementationBoundProperties(
        legacyDecoratorFunction().andThen(sdfDecoratorFunction()));
  }
}
