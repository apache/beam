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
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 * SearchParameterCoder is the coder for {@link SearchParameter}, which either takes a MapCoder or a
 * coder for type T.
 */
public class SearchParameterCoder<T> extends CustomCoder<SearchParameter<T>> {
  private final MapCoder<String, T> originalCoder;
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  // SearchParameterCoder(MapCoder<String, T> originalCoder) {
  //   this.originalCoder = originalCoder;
  // }

  SearchParameterCoder(Coder<T> originalCoder) {
    this.originalCoder = MapCoder.of(STRING_CODER, originalCoder);
  }

  // public static <T> SearchParameterCoder<T> of(MapCoder<String, T> originalCoder) {
  //   return new SearchParameterCoder<>(originalCoder);
  // }

  public static <T> SearchParameterCoder<T> of(Coder<T> originalCoder) {
    return new SearchParameterCoder<T>(originalCoder);
  }

  @Override
  public void encode(SearchParameter<T> value, OutputStream outStream)
      throws CoderException, IOException {
    STRING_CODER.encode(value.resourceType, outStream);
    originalCoder.encode(value.queries, outStream);
  }

  @Override
  public SearchParameter<T> decode(InputStream inStream) throws CoderException, IOException {
    String resourceType = STRING_CODER.decode(inStream);
    Map<String, T> queries = originalCoder.decode(inStream);
    return new SearchParameter<>(resourceType, queries);
  }
}
