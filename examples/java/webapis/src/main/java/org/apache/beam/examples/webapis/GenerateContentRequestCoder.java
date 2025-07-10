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
package org.apache.beam.examples.webapis;

import com.google.cloud.vertexai.api.GenerateContentRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

// [START webapis_java_custom_coder]

/** A {@link CustomCoder} of a {@link GenerateContentRequest}. */
class GenerateContentRequestCoder extends CustomCoder<GenerateContentRequest> {

  static GenerateContentRequestCoder of() {
    return new GenerateContentRequestCoder();
  }

  @Override
  public void encode(GenerateContentRequest value, OutputStream outStream)
      throws CoderException, IOException {
    value.writeTo(outStream);
  }

  @Override
  public GenerateContentRequest decode(InputStream inStream) throws CoderException, IOException {
    return GenerateContentRequest.parseFrom(inStream);
  }
}

// [END webapis_java_custom_coder]
