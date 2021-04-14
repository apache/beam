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
package org.apache.beam.examples.complete.TwitterStreamGenerator;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link Serializable} object to store twitter configurations for a connection * */
@DefaultCoder(SerializableCoder.class)
public class TwitterConfig implements Serializable {
  private final String key;
  private final String secret;
  private final String token;
  private final String tokenSecret;
  private final List<String> filters;
  private final String language;

  @Override
  public boolean equals(@Initialized @Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TwitterConfig that = (TwitterConfig) o;
    return Objects.equals(key, that.key)
        && Objects.equals(secret, that.secret)
        && Objects.equals(token, that.token)
        && Objects.equals(tokenSecret, that.tokenSecret)
        && Objects.equals(filters, that.filters)
        && Objects.equals(language, that.language);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, secret, token, tokenSecret, filters, language);
  }

  public TwitterConfig(
      String key,
      String secret,
      String token,
      String tokenSecret,
      List<String> filters,
      String language) {
    this.key = key;
    this.secret = secret;
    this.token = token;
    this.tokenSecret = tokenSecret;
    this.filters = filters;
    this.language = language;
  }

  public String getKey() {
    return key;
  }

  public String getSecret() {
    return secret;
  }

  public String getToken() {
    return token;
  }

  public String getTokenSecret() {
    return tokenSecret;
  }

  public List<String> getFilters() {
    return filters;
  }

  public String getLanguage() {
    return language;
  }
}
