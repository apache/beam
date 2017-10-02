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

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tika.metadata.Metadata;





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
   public static Read read() {
     return new AutoValue_TikaIO_Read.Builder()
        .build();
   }

   /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<ParseResult>> {
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

      abstract Read build();
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
    public Read from(String filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return toBuilder()
          .setFilepattern(filepattern)
          .build();
    }

    /**
     * Returns a new transform which will use the custom TikaConfig.
     */
    public Read withTikaConfigPath(String tikaConfigPath) {
      checkNotNull(tikaConfigPath, "TikaConfigPath cannot be empty.");
      return withTikaConfigPath(StaticValueProvider.of(tikaConfigPath));
    }

    /** Same as {@code with(tikaConfigPath)}, but accepting a {@link ValueProvider}. */
    public Read withTikaConfigPath(ValueProvider<String> tikaConfigPath) {
      checkNotNull(tikaConfigPath, "TikaConfigPath cannot be empty.");
      return toBuilder()
          .setTikaConfigPath(tikaConfigPath)
          .build();
    }

    /**
     * Returns a new transform which will use the provided content type hint
     * to make the file parser detection more efficient.
     */
    public Read withContentTypeHint(String contentType) {
      checkNotNull(contentType, "ContentType cannot be empty.");
      Metadata metadata = new Metadata();
      metadata.add(Metadata.CONTENT_TYPE, contentType);
      return withInputMetadata(metadata);
    }

    /**
     * Returns a new transform which will use the provided input metadata
     * for parsing the files.
     */
    public Read withInputMetadata(Metadata metadata) {
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

    /**
     * Returns a new transform which will use TikaOptions.
     */
    public Read withOptions(TikaOptions options) {
      checkNotNull(options, "TikaOptions cannot be empty.");
      Builder builder = toBuilder();
      builder.setFilepattern(StaticValueProvider.of(options.getInput()));
      if (options.getContentTypeHint() != null) {
        Metadata metadata = this.getInputMetadata();
        if (metadata == null) {
            metadata = new Metadata();
        }
        metadata.add(Metadata.CONTENT_TYPE, options.getContentTypeHint());
        builder.setInputMetadata(metadata);
      }
      if (options.getTikaConfigPath() != null) {
        builder.setTikaConfigPath(StaticValueProvider.of(options.getTikaConfigPath()));
      }
      return builder.build();
    }

    @Override
    public PCollection<ParseResult> expand(PBegin input) {
      checkNotNull(this.getFilepattern(), "Filepattern cannot be empty.");
      return input.apply(org.apache.beam.sdk.io.Read.from(new TikaSource(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String filepatternDisplay = getFilepattern().isAccessible()
        ? getFilepattern().get() : getFilepattern().toString();
      builder
          .addIfNotNull(DisplayData.item("filePattern", filepatternDisplay)
            .withLabel("File Pattern"));
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
  }
}
