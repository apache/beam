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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AllPrimitiveDataTypes;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Integration tests for {@link RedisClient}. */
@RunWith(JUnit4.class)
public class RedisClientIT {

  private static final String CONTAINER_IMAGE_NAME = "redis:5.0.3-alpine";
  private static final Integer PORT = 6379;

  @Rule
  public GenericContainer<?> redis =
      new GenericContainer<>(DockerImageName.parse(CONTAINER_IMAGE_NAME)).withExposedPorts(PORT);

  @Rule
  public RedisExternalResourcesRule externalClients =
      new RedisExternalResourcesRule(
          () -> {
            redis.start();
            return URI.create(
                String.format("redis://%s:%d", redis.getHost(), redis.getFirstMappedPort()));
          });

  @Test
  public void canSerialize() {
    SerializableUtils.serializeToByteArray(externalClients.getActualClient());
  }

  @Test
  public void wrongHostURIThrowsException() {
    URI uri = URI.create("redis://1.2.3.4:6379");
    RedisClient client = new RedisClient(uri);
    UserCodeExecutionException got = assertThrows(UserCodeExecutionException.class, client::setup);
    String expected =
        "Failed to connect to host: redis://1.2.3.4:6379, error: Failed to connect to any host resolved for DNS name.";
    assertEquals(expected, got.getMessage());
  }

  @Test
  public void givenCustomTypeAndCoder_setex_doesNotCorruptData()
      throws IOException, UserCodeExecutionException {

    String key = UUID.randomUUID().toString();
    StringUtf8Coder keyCoder = StringUtf8Coder.of();

    AllPrimitiveDataTypes value =
        allPrimitiveDataTypes(true, BigDecimal.ONE, 1.23456, 1.23456f, 1, 1L, "🦄🦄🦄🦄");
    SerializableCoder<@NonNull AllPrimitiveDataTypes> valueCoder =
        SerializableCoder.of(AllPrimitiveDataTypes.class);

    ByteArrayOutputStream keyBuffer = new ByteArrayOutputStream();
    keyCoder.encode(key, keyBuffer);
    ByteArrayOutputStream valueBuffer = new ByteArrayOutputStream();
    valueCoder.encode(value, valueBuffer);

    byte[] keyBytes = keyBuffer.toByteArray();

    externalClients
        .getActualClient()
        .setex(keyBytes, valueBuffer.toByteArray(), Duration.standardHours(1L));
    byte[] storedValueBytes = externalClients.getValidatingClient().get(keyBytes);
    AllPrimitiveDataTypes storedValue =
        valueCoder.decode(new ByteArrayInputStream(storedValueBytes));

    assertEquals(value, storedValue);
  }

  @Test
  public void setex_expiresDataWhenExpected()
      throws UserCodeExecutionException, InterruptedException {
    Duration expiry = Duration.standardSeconds(2L);
    String key = UUID.randomUUID().toString();
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    externalClients.getActualClient().setex(keyBytes, keyBytes, expiry);
    assertTrue(externalClients.getValidatingClient().exists(keyBytes));
    assertTrue(externalClients.getValidatingClient().ttl(keyBytes) > 0L);
    Thread.sleep(expiry.plus(Duration.millis(100L)).getMillis());
    assertFalse(externalClients.getValidatingClient().exists(keyBytes));
  }

  @Test
  public void givenCustomTypeAndCoder_rpush_doesNotCorruptData()
      throws IOException, UserCodeExecutionException {
    String key = UUID.randomUUID().toString();

    AllPrimitiveDataTypes value =
        allPrimitiveDataTypes(true, BigDecimal.ONE, 1.23456, 1.23456f, 1, 1L, "🦄🦄🦄🦄");
    SerializableCoder<@NonNull AllPrimitiveDataTypes> valueCoder =
        SerializableCoder.of(AllPrimitiveDataTypes.class);

    ByteArrayOutputStream valueBuffer = new ByteArrayOutputStream();
    valueCoder.encode(value, valueBuffer);

    assertEquals(0L, externalClients.getActualClient().llen(key));
    externalClients.getActualClient().rpush(key, valueBuffer.toByteArray());
    assertEquals(1L, externalClients.getActualClient().llen(key));

    byte[] storedBytes = externalClients.getActualClient().lpop(key);

    AllPrimitiveDataTypes storedValue = valueCoder.decode(new ByteArrayInputStream(storedBytes));

    assertEquals(value, storedValue);
    assertEquals(0L, externalClients.getActualClient().llen(key));
  }

  @Test
  public void rpushAndlPopYieldsFIFOOrder() throws UserCodeExecutionException {
    String key = UUID.randomUUID().toString();
    List<String> want = ImmutableList.of("1", "2", "3", "4", "5");

    for (String item : want) {
      externalClients.getActualClient().rpush(key, item.getBytes(StandardCharsets.UTF_8));
    }
    List<String> got = new ArrayList<>();
    while (!externalClients.getActualClient().isEmpty(key)) {
      byte[] bytes = externalClients.getActualClient().lpop(key);
      got.add(new String(bytes, StandardCharsets.UTF_8));
    }

    assertEquals(want, got);
  }

  @Test
  public void givenExpired_decr_yieldsNegativeOne_andNotExists()
      throws InterruptedException, UserCodeExecutionException {
    String key = UUID.randomUUID().toString();
    externalClients.getActualClient().setex(key, 100L, Duration.standardSeconds(1L));
    assertTrue(externalClients.getActualClient().exists(key));
    Thread.sleep(1500L);
    assertFalse(externalClients.getActualClient().exists(key));
    assertEquals(-1L, externalClients.getActualClient().decr(key));
    assertEquals(-2L, externalClients.getActualClient().decr(key));
    assertEquals(-3L, externalClients.getActualClient().decr(key));

    key = UUID.randomUUID().toString();
    externalClients.getActualClient().setex(key, -100L, Duration.standardSeconds(1L));
    assertTrue(externalClients.getActualClient().exists(key));
    Thread.sleep(1500L);
    assertFalse(externalClients.getActualClient().exists(key));
    assertEquals(-1L, externalClients.getActualClient().decr(key));
    assertEquals(-2L, externalClients.getActualClient().decr(key));
    assertEquals(-3L, externalClients.getActualClient().decr(key));
  }

  @Test
  public void setThenDecrThenIncr_yieldsExpectedValue() throws UserCodeExecutionException {
    String key = UUID.randomUUID().toString();
    externalClients.getActualClient().setex(key, 100L, Duration.standardHours(1L));
    assertEquals(100L, externalClients.getActualClient().getLong(key));
    for (long i = 0; i < 100L; i++) {
      externalClients.getActualClient().decr(key);
    }
    assertEquals(0L, externalClients.getActualClient().getLong(key));
    for (long i = 0; i < 100L; i++) {
      externalClients.getActualClient().incr(key);
    }
    assertEquals(100L, externalClients.getActualClient().getLong(key));
  }

  @Test
  public void givenKeyNotExists_getLong_yieldsZero() throws UserCodeExecutionException {
    assertEquals(0L, externalClients.getActualClient().getLong(UUID.randomUUID().toString()));
  }

  @Test
  public void givenKeyNotExists_getBytes_yieldsNull() throws UserCodeExecutionException {
    assertNull(
        externalClients
            .getActualClient()
            .getBytes(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void givenKeyExists_getBytes_yieldsValue() throws UserCodeExecutionException {
    byte[] keyBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    String expected = UUID.randomUUID().toString();
    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
    externalClients.getValidatingClient().set(keyBytes, expectedBytes);
    byte[] actualBytes = externalClients.getActualClient().getBytes(keyBytes);
    assertNotNull(actualBytes);
    String actual = new String(actualBytes, StandardCharsets.UTF_8);
    assertEquals(expected, actual);
  }
}
