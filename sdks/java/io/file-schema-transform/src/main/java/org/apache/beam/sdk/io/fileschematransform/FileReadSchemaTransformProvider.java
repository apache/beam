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
package org.apache.beam.sdk.io.fileschematransform;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CharStreams;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class FileReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<FileReadSchemaTransformConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(FileReadSchemaTransformProvider.class);
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:file_read:v1";
  static final String INPUT_TAG = "input";
  static final String OUTPUT_TAG = "output";
  static final String FILEPATTERN_ROW_FIELD_NAME = "filepattern";

  @Override
  protected Class<FileReadSchemaTransformConfiguration> configurationClass() {
    return FileReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(FileReadSchemaTransformConfiguration configuration) {
    return new FileReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @VisibleForTesting
  static class FileReadSchemaTransform extends SchemaTransform {
    private FileReadSchemaTransformConfiguration configuration;
    private boolean useInputPCollection;

    FileReadSchemaTransform(FileReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
      useInputPCollection = Strings.isNullOrEmpty(configuration.getFilepattern());
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.getAll().isEmpty() ^ Strings.isNullOrEmpty(configuration.getFilepattern()),
          "Either an input PCollection of file patterns or the filepattern parameter must be set,"
              + "but not both.");

      // Input schema can be a schema String or a path to a file containing the schema
      // Resolve to get the String representation of the schema
      String schema = configuration.getSchema();
      if (!Strings.isNullOrEmpty(schema)) {
        schema = resolveSchemaStringOrFilePath(configuration.getSafeSchema());
        configuration = configuration.toBuilder().setSchema(schema).build();
      }

      PCollection<MatchResult.Metadata> files;
      if (useInputPCollection) {
        files =
            input
                .get(INPUT_TAG)
                .apply(
                    "Get filepatterns",
                    MapElements.into(TypeDescriptors.strings())
                        .via(
                            (Row row) ->
                                Objects.requireNonNull(row.getString(FILEPATTERN_ROW_FIELD_NAME)))
                        .exceptionsInto(TypeDescriptors.nulls())
                        .exceptionsVia(
                            exceptionElement -> {
                              String faultyFilepattern =
                                  Optional.ofNullable(
                                          exceptionElement
                                              .element()
                                              .getString(FILEPATTERN_ROW_FIELD_NAME))
                                      .orElse("[null filepattern]");
                              LOG.warn(
                                  "Could not acquire a faulty filepattern: {}. This will be skipped.",
                                  faultyFilepattern);
                              return null;
                            }))
                .output()
                .apply("Match files", (FileIO.MatchAll) buildMatchTransform());
      } else {
        Pipeline p = input.getPipeline();
        files = p.apply((FileIO.Match) buildMatchTransform());
      }

      // Pass readable files to the appropriate source and output rows.
      PCollection<Row> output =
          files
              .apply(FileIO.readMatches())
              .apply("Read files", getProvider().buildTransform(configuration));

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }

    @VisibleForTesting
    PTransform<?, PCollection<MatchResult.Metadata>> buildMatchTransform() {
      PTransform<?, PCollection<MatchResult.Metadata>> matchTransform =
          useInputPCollection
              ? FileIO.matchAll()
              : FileIO.match().filepattern(configuration.getSafeFilepattern());

      // Two parameters are provided to configure watching for new files.
      Long terminateAfterSeconds = configuration.getTerminateAfterSecondsSinceNewOutput();
      Long pollIntervalMillis = configuration.getPollIntervalMillis();

      // Streaming is enabled when a poll interval is provided
      if (pollIntervalMillis != null && pollIntervalMillis > 0L) {
        Duration pollDuration = Duration.millis(pollIntervalMillis);

        // If provided, will terminate after this many seconds since seeing a new file
        // By default, the file match transform never terminates
        TerminationCondition<String, ?> terminationCondition =
            (terminateAfterSeconds != null && terminateAfterSeconds > 0L)
                ? Growth.afterTimeSinceNewOutput(Duration.standardSeconds(terminateAfterSeconds))
                : Growth.never();

        matchTransform =
            useInputPCollection
                ? FileIO.matchAll().continuously(pollDuration, terminationCondition)
                : FileIO.match()
                    .filepattern(configuration.getSafeFilepattern())
                    .continuously(pollDuration, terminationCondition);
      }
      return matchTransform;
    }

    private String resolveSchemaStringOrFilePath(String schema) {
      try {
        MatchResult result;
        try {
          LOG.info("Attempting to locate input schema as a file path.");
          result = FileSystems.match(schema);
          // While some FileSystem implementations throw an IllegalArgumentException when matching
          // invalid file paths, others will return a status ERROR or NOT_FOUND. The following check
          // will throw an IllegalArgumentException and take us to the catch block.
          checkArgument(result.status() == MatchResult.Status.OK);
        } catch (IllegalArgumentException e) {
          LOG.info(
              "Input schema is not a valid file path. Will attempt to use it as a schema string.");
          return schema;
        }
        checkArgument(
            !result.metadata().isEmpty(),
            "Failed to match any files for the input schema file path.");
        List<ResourceId> resource =
            result.metadata().stream()
                .map(MatchResult.Metadata::resourceId)
                .collect(Collectors.toList());

        checkArgument(
            resource.size() == 1,
            "Expected exactly 1 schema file, but got " + resource.size() + " files.");

        ReadableByteChannel byteChannel = FileSystems.open(resource.get(0));
        Reader reader = Channels.newReader(byteChannel, UTF_8.name());
        return CharStreams.toString(reader);
      } catch (IOException e) {
        throw new RuntimeException("Error when parsing input schema file: ", e);
      }
    }

    private FileReadSchemaTransformFormatProvider getProvider() {
      String format = configuration.getFormat();
      Map<String, FileReadSchemaTransformFormatProvider> providers =
          Providers.loadProviders(FileReadSchemaTransformFormatProvider.class);
      checkArgument(
          providers.containsKey(format),
          String.format(
              "Received unsupported file format: %s. Supported formats are %s",
              format, providers.keySet()));

      Optional<FileReadSchemaTransformFormatProvider> provider =
          Optional.ofNullable(providers.get(format));
      checkState(provider.isPresent());
      return provider.get();
    }
  }
}
