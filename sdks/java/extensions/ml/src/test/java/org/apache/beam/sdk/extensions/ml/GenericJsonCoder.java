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
package org.apache.beam.sdk.extensions.ml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 * Can be used as a coder for any object that extends GenericJson. This includes all objects in the
 * Google Genomics Java client library.
 */
public class GenericJsonCoder<T extends GenericJson> extends DelegatingAtomicCoder<T, String> {

  private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
  private final Class<T> type;

  private GenericJsonCoder(Class<T> type) {
    super(STRING_CODER);
    this.type = type;
  }

  public static <T extends GenericJson> GenericJsonCoder<T> of(Class<T> type) {
    return new GenericJsonCoder<>(type);
  }

  @JsonCreator
  @SuppressWarnings("unchecked")
  public static <T extends GenericJson> GenericJsonCoder<T> of(@JsonProperty("type") String type)
      throws ClassNotFoundException {
    return of((Class<T>) Class.forName(type));
  }

  @Override
  protected T from(String object) throws IOException {
    return JSON_FACTORY.fromString(object, type);
  }

  @Override
  protected String to(T object) throws IOException {
    return JSON_FACTORY.toString(object);
  }
}
