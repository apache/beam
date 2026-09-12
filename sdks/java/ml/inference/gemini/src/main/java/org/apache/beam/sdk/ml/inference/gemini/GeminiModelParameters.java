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
package org.apache.beam.sdk.ml.inference.gemini;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.ml.inference.remote.BaseInput;
import org.apache.beam.sdk.ml.inference.remote.BaseModelParameters;
import org.apache.beam.sdk.ml.inference.remote.BaseResponse;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class GeminiModelParameters<InputT extends BaseInput, OutputT extends BaseResponse>
    implements BaseModelParameters {

  public abstract @Nullable String getApiKey();

  public abstract @Nullable String getProject();

  public abstract @Nullable String getLocation();

  public abstract String getModelName();

  public abstract GeminiRequestFunction<InputT, OutputT> getRequestFn();

  public static <InputT extends BaseInput, OutputT extends BaseResponse>
      Builder<InputT, OutputT> builder() {
    return new AutoValue_GeminiModelParameters.Builder<InputT, OutputT>();
  }

  @AutoValue.Builder
  public abstract static class Builder<InputT extends BaseInput, OutputT extends BaseResponse> {
    public abstract Builder<InputT, OutputT> setApiKey(String apiKey);

    public abstract Builder<InputT, OutputT> setProject(String project);

    public abstract Builder<InputT, OutputT> setLocation(String location);

    public abstract Builder<InputT, OutputT> setModelName(String modelName);

    public abstract Builder<InputT, OutputT> setRequestFn(
        GeminiRequestFunction<InputT, OutputT> requestFn);

    public abstract GeminiModelParameters<InputT, OutputT> build();
  }
}
