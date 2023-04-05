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
package org.apache.beam.sdk.io.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;

/** A {@link Coder} for {@link ResourceId}. */
public class ResourceIdCoder extends AtomicCoder<ResourceId> {
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
  private static final Coder<Boolean> BOOL_CODER = BooleanCoder.of();

  /** Creates a {@link ResourceIdCoder}. */
  public static ResourceIdCoder of() {
    return new ResourceIdCoder();
  }

  @Override
  public void encode(ResourceId value, OutputStream os) throws IOException {
    STRING_CODER.encode(value.toString(), os);
    BOOL_CODER.encode(value.isDirectory(), os);
  }

  @Override
  public ResourceId decode(InputStream is) throws IOException {
    String spec = STRING_CODER.decode(is);
    boolean isDirectory = BOOL_CODER.decode(is);
    return FileSystems.matchNewResource(spec, isDirectory);
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }
}
