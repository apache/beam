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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToTextContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;



/**
 * {@link PTransform} for parsing arbitrary files using Apache Tika.
 * Files in many well known text, binary or scientific formats can be processed.
 *
 * <p>To read a {@link PCollection} from one or more files
 * use {@link TikaIO.Read#from(String)}
 * to specify the path of the file(s) to be read.
 *
 * <p>{@link TikaIO.Read} returns a bounded {@link PCollection} of {@link String Strings},
 * each corresponding to a sequence of characters reported by Apache Tika SAX Parser.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local PDF file (only runs locally):
 * PCollection<String> content = p.apply(TikaInput.from("/local/path/to/file.pdf"));
 * }</pre>
 *
 * <b>Warning:</b> the API of this IO is likely to change in the next release.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class TikaIO {

  /**
   * A {@link PTransform} that parses one or more files and returns a bounded {@link PCollection}
   * containing one element for each sequence of characters reported by Apache Tika SAX Parser.
   */
   public static ParseAll parseAll() {
     return new AutoValue_TikaIO_ParseAll.Builder()
        .build();
   }

   /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class ParseAll extends
    PTransform<PCollection<ReadableFile>, PCollection<ParseResult>> {
    private static final long serialVersionUID = 2198301984784351829L;

    @Nullable abstract ValueProvider<String> getFilepattern();
    @Nullable abstract ValueProvider<String> getTikaConfigPath();
    @Nullable abstract Metadata getInputMetadata();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setTikaConfigPath(ValueProvider<String> tikaConfigPath);
      abstract Builder setInputMetadata(Metadata metadata);

      abstract ParseAll build();
    }

    /**
     * A {@link PTransform} that parses one or more files with the given filename
     * or filename pattern and returns a bounded {@link PCollection} containing
     * one element for each sequence of characters reported by Apache Tika SAX Parser.
     *
     * <p>Filepattern can be a local path (if running locally), or a Google Cloud Storage
     * filename or filename pattern of the form {@code "gs://<bucket>/<filepath>"}
     * (if running locally or using remote execution service).
     *
     * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public ParseAll from(String filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public ParseAll from(ValueProvider<String> filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return toBuilder()
          .setFilepattern(filepattern)
          .build();
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

      //String filepatternDisplay = getFilepattern().isAccessible()
      //  ? getFilepattern().get() : getFilepattern().toString();
      //builder
      //    .addIfNotNull(DisplayData.item("filePattern", filepatternDisplay)
      //      .withLabel("File Pattern"));
      if (getTikaConfigPath() != null) {
        String tikaConfigPathDisplay = getTikaConfigPath().isAccessible()
          ? getTikaConfigPath().get() : getTikaConfigPath().toString();
        builder.add(DisplayData.item("tikaConfigPath", tikaConfigPathDisplay)
            .withLabel("TikaConfig Path"));
      }
      Metadata metadata = getInputMetadata();
      if (metadata != null) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (String name : metadata.names()) {
            if (sb.length() > 1) {
              sb.append(',');
            }
            sb.append(name).append('=').append(metadata.get(name));
        }
        sb.append(']');
        builder
            .add(DisplayData.item("inputMetadata", sb.toString())
            .withLabel("Input Metadata"));
      }
    }

    private static class ParseToStringFn extends DoFn<ReadableFile, ParseResult> {

      private static final long serialVersionUID = 6837207505313720989L;
      private TikaIO.ParseAll spec;
      ParseToStringFn(TikaIO.ParseAll spec) {
        this.spec = spec;
      }
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        ReadableFile file = c.element();
        InputStream stream = Channels.newInputStream(file.open());

        final InputStream is = TikaInputStream.get(stream);
        TikaConfig tikaConfig = null;
        if (spec.getTikaConfigPath() != null) {
          try {
            tikaConfig = new TikaConfig(spec.getTikaConfigPath().get());
          } catch (TikaException | SAXException e) {
            throw new IOException(e);
          }
        }
        final Parser parser = tikaConfig == null ? new AutoDetectParser()
            : new AutoDetectParser(tikaConfig);
        final ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        org.apache.tika.metadata.Metadata tikaMetadata = spec.getInputMetadata() != null
          ? spec.getInputMetadata() : new org.apache.tika.metadata.Metadata();

        ContentHandler tikaHandler = new ToTextContentHandler();
        try {
          parser.parse(is, tikaHandler, tikaMetadata, context);
        } catch (Exception ex) {
          throw new IOException(ex);
        } finally {
          is.close();
        }

        String content = tikaHandler.toString().trim();
        String filePath = file.getMetadata().resourceId().toString();
        c.output(new ParseResult(filePath, content, tikaMetadata));
      }
    }
  }
}
