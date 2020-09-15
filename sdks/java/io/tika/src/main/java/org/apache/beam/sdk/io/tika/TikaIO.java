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
package org.apache.beam.sdk.io.tika;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToTextContentHandler;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.xml.sax.ContentHandler;

/**
 * Transforms for parsing arbitrary files using <a href="https://tika.apache.org/">Apache Tika</a>.
 *
 * <p>Tika is able to extract text and metadata from files in many well known text, binary and
 * scientific formats.
 *
 * <p>The entry points are {@link #parse} and {@link #parseFiles}. They parse a set of files and
 * return a {@link PCollection} containing one {@link ParseResult} per each file. {@link #parse}
 * implements the common case of parsing all files matching a single filepattern, while {@link
 * #parseFiles} should be used for all use cases requiring more control, in combination with {@link
 * FileIO#match} and {@link FileIO#readMatches} (see their respective documentation).
 *
 * <p>{@link #parse} does not automatically uncompress compressed files: they are passed to Tika
 * as-is.
 *
 * <p>It's possible that some files will partially or completely fail to parse. In that case, the
 * respective {@link ParseResult} will be marked unsuccessful (see {@link ParseResult#isSuccess})
 * and will contain the error, available via {@link ParseResult#getError}.
 *
 * <p>Example: using {@link #parse} to parse all PDF files in a directory on GCS.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<ParseResult> results =
 *   p.apply(TikaIO.parse().filepattern("gs://my-bucket/files/*.pdf"));
 * }</pre>
 *
 * <p>Example: using {@link #parseFiles} in combination with {@link FileIO} to continuously parse
 * new PDF files arriving into the directory.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<ParseResult> results =
 *   p.apply(FileIO.match().filepattern("gs://my-bucket/files/*.pdf")
 *       .continuously(...))
 *    .apply(FileIO.readMatches())
 *    .apply(TikaIO.parseFiles());
 * }</pre>
 */
@Experimental(Kind.SOURCE_SINK)
public class TikaIO {
  /** Parses files matching a given filepattern. */
  public static Parse parse() {
    return new AutoValue_TikaIO_Parse.Builder().build();
  }

  /** Parses files in a {@link PCollection} of {@link ReadableFile}. */
  public static ParseFiles parseFiles() {
    return new AutoValue_TikaIO_ParseFiles.Builder().build();
  }

  /** Implementation of {@link #parse}. */
  @AutoValue
  public abstract static class Parse extends PTransform<PBegin, PCollection<ParseResult>> {

    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Parse build();
    }

    /** Matches the given filepattern. */
    public Parse filepattern(String filepattern) {
      return this.filepattern(ValueProvider.StaticValueProvider.of(filepattern));
    }

    /** Like {@link #filepattern(String)} but using a {@link ValueProvider}. */
    public Parse filepattern(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.addIfNotNull(
          DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"));
    }

    @Override
    public PCollection<ParseResult> expand(PBegin input) {
      return input
          .apply(FileIO.match().filepattern(getFilepattern()))
          .apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
          .apply(parseFiles());
    }
  }

  /** Implementation of {@link #parseFiles}. */
  @AutoValue
  public abstract static class ParseFiles
      extends PTransform<PCollection<ReadableFile>, PCollection<ParseResult>> {

    abstract @Nullable ValueProvider<String> getTikaConfigPath();

    abstract @Nullable String getContentTypeHint();

    abstract @Nullable Metadata getInputMetadata();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTikaConfigPath(ValueProvider<String> tikaConfigPath);

      abstract Builder setContentTypeHint(String contentTypeHint);

      abstract Builder setInputMetadata(Metadata metadata);

      abstract ParseFiles build();
    }

    /**
     * Uses the given <a
     * href="https://tika.apache.org/1.16/configuring.html#Using_a_Tika_Configuration_XML_file">Tika
     * Configuration XML file</a>.
     */
    public ParseFiles withTikaConfigPath(String tikaConfigPath) {
      checkArgument(tikaConfigPath != null, "tikaConfigPath can not be null.");
      return withTikaConfigPath(StaticValueProvider.of(tikaConfigPath));
    }

    /** Like {@code with(tikaConfigPath)}. */
    public ParseFiles withTikaConfigPath(ValueProvider<String> tikaConfigPath) {
      checkArgument(tikaConfigPath != null, "tikaConfigPath can not be null.");
      return toBuilder().setTikaConfigPath(tikaConfigPath).build();
    }

    /**
     * Sets a content type hint to make the file parser detection more efficient. Overrides the
     * content type hint in {@link #withInputMetadata}, if any.
     */
    public ParseFiles withContentTypeHint(String contentTypeHint) {
      checkNotNull(contentTypeHint, "contentTypeHint can not be null.");
      return toBuilder().setContentTypeHint(contentTypeHint).build();
    }

    /** Sets the input metadata for {@link Parser#parse}. */
    public ParseFiles withInputMetadata(Metadata metadata) {
      Metadata inputMetadata = this.getInputMetadata();
      if (inputMetadata != null) {
        for (String name : metadata.names()) {
          inputMetadata.set(name, metadata.get(name));
        }
      } else {
        inputMetadata = metadata;
      }
      return toBuilder().setInputMetadata(inputMetadata).build();
    }

    @Override
    public PCollection<ParseResult> expand(PCollection<ReadableFile> input) {
      return input.apply(ParDo.of(new ParseToStringFn(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      if (getTikaConfigPath() != null) {
        builder.add(
            DisplayData.item("tikaConfigPath", getTikaConfigPath()).withLabel("TikaConfig Path"));
      }
      Metadata metadata = getInputMetadata();
      if (metadata != null) {
        // TODO: use metadata.toString() only without a trim() once Apache Tika 1.17 gets released
        builder.add(
            DisplayData.item("inputMetadata", metadata.toString().trim())
                .withLabel("Input Metadata"));
      }
      builder.addIfNotNull(
          DisplayData.item("contentTypeHint", getContentTypeHint()).withLabel("Content type hint"));
    }

    private static class ParseToStringFn extends DoFn<ReadableFile, ParseResult> {
      private final ParseFiles spec;
      private transient TikaConfig tikaConfig;

      ParseToStringFn(ParseFiles spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        if (spec.getTikaConfigPath() != null) {
          ResourceId configResource =
              FileSystems.matchSingleFileSpec(spec.getTikaConfigPath().get()).resourceId();
          tikaConfig = new TikaConfig(Channels.newInputStream(FileSystems.open(configResource)));
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        ReadableFile file = c.element();
        InputStream stream = Channels.newInputStream(file.open());
        try (InputStream tikaStream = TikaInputStream.get(stream)) {
          Parser parser =
              tikaConfig == null ? new AutoDetectParser() : new AutoDetectParser(tikaConfig);

          ParseContext context = new ParseContext();
          context.set(Parser.class, parser);
          Metadata tikaMetadata =
              spec.getInputMetadata() != null ? spec.getInputMetadata() : new Metadata();
          if (spec.getContentTypeHint() != null) {
            tikaMetadata.set(Metadata.CONTENT_TYPE, spec.getContentTypeHint());
          }

          String location = file.getMetadata().resourceId().toString();
          ParseResult res;
          ContentHandler tikaHandler = new ToTextContentHandler();
          try {
            parser.parse(tikaStream, tikaHandler, tikaMetadata, context);
            res = ParseResult.success(location, tikaHandler.toString(), tikaMetadata);
          } catch (Exception e) {
            res = ParseResult.failure(location, tikaHandler.toString(), tikaMetadata, e);
          }

          c.output(res);
        }
      }
    }
  }
}
