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
package org.apache.beam.sdk.io.aws2.options;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/** Utilities for working with AWS Serializables. */
@Internal
public class AwsSerializableUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new AwsModule());
  }

  public static String serializeAwsCredentialsProvider(AwsCredentialsProvider credentialsProvider) {
    return serialize(credentialsProvider);
  }

  public static AwsCredentialsProvider deserializeAwsCredentialsProvider(
      String serializedCredentialsProvider) {
    return deserialize(serializedCredentialsProvider, AwsCredentialsProvider.class);
  }

  static String serialize(Object object) {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          object.getClass().getSimpleName() + " can not be serialized to Json", e);
    }
  }

  static <T> T deserialize(String serializedObject, Class<T> clazz) {
    try {
      return MAPPER.readValue(serializedObject, clazz);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          clazz.getSimpleName() + " can not be deserialized from Json", e);
    }
  }
}
