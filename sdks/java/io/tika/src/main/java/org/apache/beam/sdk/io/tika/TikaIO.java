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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;

import java.io.InputStream;
import java.nio.channels.Channels;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
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
import org.xml.sax.ContentHandler;




/**
 * A collection of {@link PTransform} transforms for parsing arbitrary files using Apache Tika.
 * Files in many well known text, binary or scientific formats can be processed.
 *
 * <p>{@link TikaIO.Parse} and {@link TikaIO.ParseAll} parse the files and return
 * a {@link PCollection} containing one {@link ParseResult} per each file.
 *
 * <p>Combine {@link TikaIO.ParseAll} with {@link FileIO.Match}
 * and {@link FileIO.ReadMatches} to match, read and parse the files.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple parse of a local PDF file (only runs locally):
 * PCollection<ParseResult> results =
 *   p.apply(FileIO.match().filepattern("/local/path/to/file.pdf"))
 *    .apply(FileIO.readMatches())
 *    .apply(TikaIO.parseFiles());
 * }</pre>
 *
 * <p>Use {@link TikaIO.Parse} to match, read and parse the files in simple cases.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple parse of a local PDF file (only runs locally):
 * PCollection<ParseResult> results =
 *   p.apply(TikaIO.parseAll().filepattern("/local/path/to/file.pdf"));
 * }</pre>
 *
 * <b>Warning:</b> the API of this IO is likely to change in the next release.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class TikaIO {

  /**
   * A {@link PTransform} that matches and parses the files
   * and returns a bounded {@link PCollection} of {@link ParseResult}.
   */
  public static Parse parse() {
    return new AutoValue_TikaIO_Parse.Builder()
        .build();
  }

  /**
   * A {@link PTransform} that accepts a {@link PCollection} of {@link ReadableFile}
   * and returns a {@link PCollection} of {@link ParseResult}.
   */
  public static ParseAll parseAll() {
    return new AutoValue_TikaIO_ParseAll.Builder()
        .build();
  }

  /** Implementation of {@link #parse}. */
  @SuppressWarnings("serial")
  @AutoValue
  public abstract static class Parse extends PTransform<PBegin, PCollection<ParseResult>> {
    @Nullable
    abstract ValueProvider<String> getFilepattern();

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

      builder
        .addIfNotNull(
          DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"));
    }

    @Override
    public PCollection<ParseResult> expand(PBegin input) {
      return input
          .apply(FileIO.match().filepattern(getFilepattern()))
          .apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
          .apply(parseAll());
    }
  }

  /** Implementation of {@link #parseAll}. */
  @SuppressWarnings("serial")
  @AutoValue
  public abstract static class ParseAll extends
    PTransform<PCollection<ReadableFile>, PCollection<ParseResult>> {

    @Nullable abstract ValueProvider<String> getTikaConfigPath();
    @Nullable abstract Metadata getInputMetadata();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTikaConfigPath(ValueProvider<String> tikaConfigPath);
      abstract Builder setInputMetadata(Metadata metadata);

      abstract ParseAll build();
    }

    /**
     * Returns a new transform which will use the custom TikaConfig.
     */
    public ParseAll withTikaConfigPath(String tikaConfigPath) {
      checkNotNull(tikaConfigPath, "TikaConfigPath cannot be empty.");
      return withTikaConfigPath(StaticValueProvider.of(tikaConfigPath));
    }

    /** Same as {@code with(tikaConfigPath)}, but accepting a {@link ValueProvider}. */
    public ParseAll withTikaConfigPath(ValueProvider<String> tikaConfigPath) {
      checkNotNull(tikaConfigPath, "TikaConfigPath cannot be empty.");
      return toBuilder()
          .setTikaConfigPath(tikaConfigPath)
          .build();
    }

    /**
     * Returns a new transform which will use the provided content type hint
     * to make the file parser detection more efficient.
     */
    public ParseAll withContentTypeHint(String contentType) {
      checkNotNull(contentType, "ContentType cannot be empty.");
      Metadata metadata = new Metadata();
      metadata.add(Metadata.CONTENT_TYPE, contentType);
      return withInputMetadata(metadata);
    }

    /**
     * Returns a new transform which will use the provided input metadata
     * for parsing the files.
     */
    public ParseAll withInputMetadata(Metadata metadata) {
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
        String tikaConfigPathDisplay = getTikaConfigPath().isAccessible()
          ? getTikaConfigPath().get() : getTikaConfigPath().toString();
        builder.add(DisplayData.item("tikaConfigPath", tikaConfigPathDisplay)
            .withLabel("TikaConfig Path"));
      }
      Metadata metadata = getInputMetadata();
      if (metadata != null) {
        //TODO: use metadata.toString() only without a trim() once Apache Tika 1.17 gets released
        builder
            .add(DisplayData.item("inputMetadata", metadata.toString().trim())
            .withLabel("Input Metadata"));
      }
    }

    private static class ParseToStringFn extends DoFn<ReadableFile, ParseResult> {

      private static final long serialVersionUID = 6837207505313720989L;
      private final TikaIO.ParseAll spec;
      private TikaConfig tikaConfig;

      ParseToStringFn(TikaIO.ParseAll spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        if (spec.getTikaConfigPath() != null) {
          ResourceId configResource =
              FileSystems.matchSingleFileSpec(spec.getTikaConfigPath().get()).resourceId();
          tikaConfig = new TikaConfig(
                           Channels.newInputStream(FileSystems.open(configResource)));
        }
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        ReadableFile file = c.element();
        InputStream stream = Channels.newInputStream(file.open());
        try (InputStream tikaStream = TikaInputStream.get(stream)) {

          final Parser parser = tikaConfig == null
              ? new AutoDetectParser() : new AutoDetectParser(tikaConfig);

          final ParseContext context = new ParseContext();
          context.set(Parser.class, parser);
          Metadata tikaMetadata = spec.getInputMetadata() != null
            ? spec.getInputMetadata() : new org.apache.tika.metadata.Metadata();

          ContentHandler tikaHandler = new ToTextContentHandler();
          parser.parse(tikaStream, tikaHandler, tikaMetadata, context);

          c.output(new ParseResult(file.getMetadata().resourceId().toString(),
              tikaHandler.toString(),
              tikaMetadata));
        }
      }
    }
  }
}
