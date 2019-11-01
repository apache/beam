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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import software.amazon.awssdk.http.HttpStatusFamily;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sns.model.PublishResponse;

@AutoValue
public abstract class WrappedSnsResponse<T> implements Serializable {
  /** Gets the element that was used to publish. */
  public abstract T element();

  public abstract OptionalInt statusCode();

  public abstract Optional<String> statusText();

  /** Get the sns publish status for this result. */
  public abstract SnsPublishStatus status();

  public abstract Optional<Throwable> exception();

  static <T> WrappedSnsResponse<T> create(
      @Nonnull final T element,
      OptionalInt statusCode,
      Optional<String> statusText,
      SnsPublishStatus status,
      Optional<Throwable> error) {

    return new AutoValue_WrappedSnsResponse<>(element, statusCode, statusText, status, error);
  }

  /** Creates a WrappedSnsResponse for a valid response from SNS after a publish succeeds. */
  static <T> WrappedSnsResponse<T> of(
      @Nonnull final T element, @Nullable final PublishResponse response) {

    final Optional<PublishResponse> publishResponse = Optional.ofNullable(response);

    final OptionalInt statusCode =
        publishResponse
            .map(r -> OptionalInt.of(r.sdkHttpResponse().statusCode()))
            .orElse(OptionalInt.empty());

    final Optional<String> statusText =
        publishResponse.flatMap(r -> r.sdkHttpResponse().statusText());

    final SnsPublishStatus snsPublishStatus =
        publishResponse
            .flatMap(
                r ->
                    Optional.ofNullable(r.sdkHttpResponse())
                        .map(SdkHttpResponse::statusCode)
                        .map(
                            code ->
                                HttpStatusFamily.of(code) == HttpStatusFamily.SUCCESSFUL
                                    ? SnsPublishStatus.SUCCESS
                                    : SnsPublishStatus.ERROR))
            .orElse(SnsPublishStatus.UNKNOWN);

    return create(element, statusCode, statusText, snsPublishStatus, Optional.empty());
  }

  /** Creates a WrappedSnsResponse for a valid response from SNS after a publish fails. */
  static <T> WrappedSnsResponse<T> ofError(
      @Nonnull final T element, @Nonnull final Throwable error) {

    return create(
        element, OptionalInt.empty(), Optional.empty(), SnsPublishStatus.ERROR, Optional.of(error));
  }
}
