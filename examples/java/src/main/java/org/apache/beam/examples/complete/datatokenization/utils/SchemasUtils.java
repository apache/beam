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
package org.apache.beam.examples.complete.datatokenization.utils;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.fromTableSchema;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SchemasUtils} Class to read JSON based schema. Is there available to read from file or
 * from string. Currently supported local File System and GCS.
 */
@SuppressWarnings({
  "initialization.fields.uninitialized",
  "method.invocation",
  "dereference.of.nullable",
  "argument",
  "return"
})
public class SchemasUtils {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(SchemasUtils.class);

  private TableSchema bigQuerySchema;
  private Schema beamSchema;
  private String jsonBeamSchema;

  public SchemasUtils(String schema) {
    parseJson(schema);
  }

  public SchemasUtils(String path, Charset encoding) throws IOException {
    if (path.startsWith("gs://")) {
      parseJson(new String(readGcsFile(path), encoding));
    } else {
      byte[] encoded = Files.readAllBytes(Paths.get(path));
      parseJson(new String(encoded, encoding));
    }
    LOG.info("Extracted schema: " + bigQuerySchema.toPrettyString());
  }

  public TableSchema getBigQuerySchema() {
    return bigQuerySchema;
  }

  private void parseJson(String jsonSchema) throws UnsupportedOperationException {
    TableSchema schema = BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    validateSchemaTypes(schema);
    bigQuerySchema = schema;
    jsonBeamSchema = BigQueryHelpers.toJsonString(schema.getFields());
  }

  private void validateSchemaTypes(TableSchema bigQuerySchema) {
    try {
      beamSchema = fromTableSchema(bigQuerySchema);
    } catch (UnsupportedOperationException exception) {
      LOG.error("Check json schema, {}", exception.getMessage());
    } catch (NullPointerException npe) {
      LOG.error("Missing schema keywords, please check what all required fields presented");
    }
  }

  /**
   * Method to read a schema file from GCS and return the file contents as a string.
   *
   * @param gcsFilePath path to file in GCS in format "gs://your-bucket/path/to/file"
   * @return byte array with file contents
   * @throws IOException thrown if not able to read file
   */
  public static byte[] readGcsFile(String gcsFilePath) throws IOException {
    LOG.info("Reading contents from GCS file: {}", gcsFilePath);
    // Read the GCS file into byte array and will throw an I/O exception in case file not found.
    try (ReadableByteChannel readerChannel =
        FileSystems.open(FileSystems.matchSingleFileSpec(gcsFilePath).resourceId())) {
      try (InputStream stream = Channels.newInputStream(readerChannel)) {
        return ByteStreams.toByteArray(stream);
      }
    }
  }

  public Schema getBeamSchema() {
    return beamSchema;
  }

  public String getJsonBeamSchema() {
    return jsonBeamSchema;
  }

  /**
   * Reads a file from GCS and returns it as a string.
   *
   * @param filePath path to file in GCS
   * @return contents of the file as a string
   * @throws IOException thrown if not able to read file
   */
  public static String getGcsFileAsString(String filePath) {
    MatchResult result;
    try {
      result = FileSystems.match(filePath);
      checkArgument(
          result.status() == MatchResult.Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + filePath);

      List<ResourceId> rId =
          result.metadata().stream()
              .map(MatchResult.Metadata::resourceId)
              .collect(Collectors.toList());

      checkArgument(rId.size() == 1, "Expected exactly 1 file, but got " + rId.size() + " files.");

      Reader reader =
          Channels.newReader(FileSystems.open(rId.get(0)), StandardCharsets.UTF_8.name());

      return CharStreams.toString(reader);

    } catch (IOException ioe) {
      LOG.error("File system i/o error: " + ioe.getMessage());
      throw new RuntimeException(ioe);
    }
  }

  public static final String DEADLETTER_SCHEMA =
      "{\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"timestamp\",\n"
          + "      \"type\": \"TIMESTAMP\",\n"
          + "      \"mode\": \"REQUIRED\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"payloadString\",\n"
          + "      \"type\": \"STRING\",\n"
          + "      \"mode\": \"REQUIRED\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"payloadBytes\",\n"
          + "      \"type\": \"BYTES\",\n"
          + "      \"mode\": \"REQUIRED\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"attributes\",\n"
          + "      \"type\": \"RECORD\",\n"
          + "      \"mode\": \"REPEATED\",\n"
          + "      \"fields\": [\n"
          + "        {\n"
          + "          \"name\": \"key\",\n"
          + "          \"type\": \"STRING\",\n"
          + "          \"mode\": \"NULLABLE\"\n"
          + "        },\n"
          + "        {\n"
          + "          \"name\": \"value\",\n"
          + "          \"type\": \"STRING\",\n"
          + "          \"mode\": \"NULLABLE\"\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"errorMessage\",\n"
          + "      \"type\": \"STRING\",\n"
          + "      \"mode\": \"NULLABLE\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"stacktrace\",\n"
          + "      \"type\": \"STRING\",\n"
          + "      \"mode\": \"NULLABLE\"\n"
          + "    }\n"
          + "  ]\n"
          + "}";
}
