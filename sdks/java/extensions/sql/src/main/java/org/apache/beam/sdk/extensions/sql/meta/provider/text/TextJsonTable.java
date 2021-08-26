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
package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider.JsonToRow;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider.RowToJson;
import org.apache.beam.sdk.schemas.Schema;

/**
 * {@link TextJsonTable} is a {@link BeamSqlTable} that reads text files and converts them according
 * to the JSON format.
 *
 * <p>Support format is {@code "json"}.
 *
 * <p>Check {@link ObjectMapper} javadoc for more info on reading and writing JSON.
 */
@Internal
public class TextJsonTable extends TextTable {

  public TextJsonTable(
      Schema schema, String filePattern, JsonToRow readConverter, RowToJson writerConverter) {
    super(schema, filePattern, readConverter, writerConverter);
  }
}
