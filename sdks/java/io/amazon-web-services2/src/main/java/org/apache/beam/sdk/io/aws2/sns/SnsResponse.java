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
package org.apache.beam.sdk.io.aws2.sns;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Optional;
import java.util.OptionalInt;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.sns.model.PublishResponse;

@AutoValue
abstract class SnsResponse<T> implements Serializable {

  public abstract T element();

  public abstract OptionalInt statusCode();

  public abstract Optional<String> statusText();

  static <T> SnsResponse<T> create(
      @NonNull T element, OptionalInt statusCode, Optional<String> statusText) {

    return new AutoValue_SnsResponse<>(element, statusCode, statusText);
  }

  public static <T> SnsResponse<T> of(@NonNull T element, @Nullable PublishResponse response) {

    final Optional<PublishResponse> publishResponse = Optional.ofNullable(response);
    OptionalInt statusCode =
        publishResponse
            .map(r -> OptionalInt.of(r.sdkHttpResponse().statusCode()))
            .orElse(OptionalInt.empty());

    Optional<String> statusText = publishResponse.flatMap(r -> r.sdkHttpResponse().statusText());

    return create(element, statusCode, statusText);
  }
}
