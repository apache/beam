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
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class FhirBundleResponseCoder extends CustomCoder<FhirBundleResponse> {
  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private final FhirBundleParameterCoder fhirBundleParameterCoder = FhirBundleParameterCoder.of();

  public static FhirBundleResponseCoder of() {
    return new FhirBundleResponseCoder();
  }

  @Override
  public void encode(FhirBundleResponse value, OutputStream outStream) throws IOException {
    fhirBundleParameterCoder.encode(value.getFhirBundleParameter(), outStream);
    STRING_CODER.encode(value.getResponse(), outStream);
  }

  @Override
  public FhirBundleResponse decode(InputStream inStream) throws IOException {
    FhirBundleParameter fhirBundleParameter = fhirBundleParameterCoder.decode(inStream);
    String response = STRING_CODER.decode(inStream);
    return FhirBundleResponse.of(fhirBundleParameter, response);
  }
}
