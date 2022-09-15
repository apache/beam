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
package org.apache.beam.sdk.io.gcp.healthcare;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.Preconditions;

/**
 * FhirSearchParameterCoder is the coder for {@link FhirSearchParameter}, which takes a coder for
 * type T.
 */
public class FhirSearchParameterCoder<T> extends CustomCoder<FhirSearchParameter<T>> {

  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private final NullableCoder<Map<String, T>> originalCoder;

  FhirSearchParameterCoder(Coder<T> originalCoder) {
    this.originalCoder = NullableCoder.of(MapCoder.of(STRING_CODER, originalCoder));
  }

  public static <T> FhirSearchParameterCoder<T> of(Coder<T> originalCoder) {
    return new FhirSearchParameterCoder<T>(originalCoder);
  }

  @Override
  public void encode(FhirSearchParameter<T> value, OutputStream outStream) throws IOException {
    STRING_CODER.encode(value.getResourceType(), outStream);
    STRING_CODER.encode(value.getKey(), outStream);
    originalCoder.encode(value.getQueries(), outStream);
  }

  @Override
  public FhirSearchParameter<T> decode(InputStream inStream) throws IOException {
    String resourceType = Preconditions.checkArgumentNotNull(STRING_CODER.decode(inStream));
    String key = STRING_CODER.decode(inStream);
    Map<String, T> queries = originalCoder.decode(inStream);
    return FhirSearchParameter.of(resourceType, key, queries);
  }
}
