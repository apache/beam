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
package org.apache.beam.sdk.extensions.spd.models;

import edu.umd.cs.findbugs.annotations.Nullable;

public class SqlModel extends ConfiguredModel implements StructuredModel {

  @Nullable private String[] path;
  @Nullable private String rawQuery;

  public SqlModel(String[] path, String name, String rawQuery) {
    super(name);
    this.path = path;
    this.rawQuery = rawQuery;
  }

  public String getRawQuery() {
    return rawQuery == null ? "" : rawQuery;
  }

  @Override
  public String getPath() {
    return path == null ? "" : String.join(".", path);
  }
}
