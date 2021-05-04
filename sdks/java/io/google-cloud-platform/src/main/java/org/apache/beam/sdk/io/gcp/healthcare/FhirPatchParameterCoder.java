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
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** FhirPatchParameterCoder is the coder for {@link FhirPatchParameter}. */
public class FhirPatchParameterCoder extends CustomCoder<FhirPatchParameter> {

  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final NullableCoder<Map<String, String>> MAP_CODER =
      NullableCoder.of(MapCoder.of(STRING_CODER, STRING_CODER));

  FhirPatchParameterCoder() {}

  public static FhirPatchParameterCoder of() {
    return new FhirPatchParameterCoder();
  }

  @Override
  public void encode(FhirPatchParameter value, OutputStream outStream) throws IOException {
    STRING_CODER.encode(value.getResourceName(), outStream);
    STRING_CODER.encode(value.getPatch(), outStream);
    MAP_CODER.encode(value.getQuery(), outStream);
  }

  @Override
  public FhirPatchParameter decode(InputStream inStream) throws IOException {
    String resourceName = STRING_CODER.decode(inStream);
    String patch = STRING_CODER.decode(inStream);
    Map<String, String> query = MAP_CODER.decode(inStream);
    return FhirPatchParameter.of(resourceName, patch, query);
  }
}
