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
package org.apache.beam.sdk.io.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public final class TableIdentifierCoder extends AtomicCoder<TableIdentifier> {
  private static final StringUtf8Coder NAME_CODER = StringUtf8Coder.of();
  private static final ListCoder<String> NAMESPACE_CODER = ListCoder.of(StringUtf8Coder.of());
  private static final TableIdentifierCoder INSTANCE = new TableIdentifierCoder();

  public static TableIdentifierCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableIdentifier value, OutputStream outStream) throws IOException {
    NAMESPACE_CODER.encode(Arrays.asList(value.namespace().levels()), outStream);
    NAME_CODER.encode(value.name(), outStream);
  }

  @Override
  public TableIdentifier decode(InputStream inStream) throws IOException {
    List<String> levels = NAMESPACE_CODER.decode(inStream);
    Namespace namespace = Namespace.of(levels.toArray(new String[0]));
    String name = NAME_CODER.decode(inStream);
    return TableIdentifier.of(namespace, name);
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }
}
