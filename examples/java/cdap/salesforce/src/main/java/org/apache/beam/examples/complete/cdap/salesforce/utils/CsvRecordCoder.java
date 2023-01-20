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
package org.apache.beam.examples.complete.cdap.salesforce.utils;

import com.google.gson.Gson;
import io.cdap.plugin.salesforce.plugin.sink.batch.CSVRecord;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** Custom coder for {@link CSVRecord}. */
public class CsvRecordCoder extends CustomCoder<CSVRecord> {

  private static final Gson GSON = new Gson();
  private static final CsvRecordCoder CODER = new CsvRecordCoder();
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

  public static CsvRecordCoder of() {
    return CODER;
  }

  @Override
  public void encode(CSVRecord value, OutputStream outStream) throws IOException {
    STRING_CODER.encode(GSON.toJson(value), outStream);
  }

  @Override
  public CSVRecord decode(InputStream inStream) throws IOException {
    return GSON.fromJson(STRING_CODER.decode(inStream), CSVRecord.class);
  }
}
