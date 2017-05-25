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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read.Bounded;
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
 */
public class TikaIO {

  /**
   * A {@link PTransform} that parses one or more files and returns a bounded {@link PCollection}
   * containing one element for each sequence of characters reported by Apache Tika SAX Parser.
   */
   public static Read read() {
     return new AutoValue_TikaIO_Read.Builder()
        .setQueuePollTime(Read.DEFAULT_QUEUE_POLL_TIME)
        .setQueueMaxPollTime(Read.DEFAULT_QUEUE_MAX_POLL_TIME)
        .build();
   }

   /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    private static final long serialVersionUID = 2198301984784351829L;
    public static final long DEFAULT_QUEUE_POLL_TIME = 50L;
    public static final long DEFAULT_QUEUE_MAX_POLL_TIME = 3000L;

    @Nullable abstract ValueProvider<String> getFilepattern();
    @Nullable abstract ValueProvider<String> getTikaConfigPath();
    @Nullable abstract Metadata getInputMetadata();
    @Nullable abstract Boolean getReadOutputMetadata();
    @Nullable abstract Long getQueuePollTime();
    @Nullable abstract Long getQueueMaxPollTime();
    @Nullable abstract Integer getMinimumTextLength();
    @Nullable abstract Boolean getParseSynchronously();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setTikaConfigPath(ValueProvider<String> tikaConfigPath);
      abstract Builder setInputMetadata(Metadata metadata);
      abstract Builder setReadOutputMetadata(Boolean value);
      abstract Builder setQueuePollTime(Long value);
      abstract Builder setQueueMaxPollTime(Long value);
      abstract Builder setMinimumTextLength(Integer value);
      abstract Builder setParseSynchronously(Boolean value);

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
          .setQueuePollTime(Read.DEFAULT_QUEUE_POLL_TIME)
          .setQueueMaxPollTime(Read.DEFAULT_QUEUE_MAX_POLL_TIME)
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
     * Returns a new transform which will report the metadata.
     */
    public Read withReadOutputMetadata(Boolean value) {
      return toBuilder().setReadOutputMetadata(value).build();
    }

    /**
     * Returns a new transform which will use the specified queue poll time.
     */
    public Read withQueuePollTime(Long value) {
      return toBuilder().setQueuePollTime(value).build();
    }

    /**
     * Returns a new transform which will use the specified queue max poll time.
     */
    public Read withQueueMaxPollTime(Long value) {
      return toBuilder().setQueueMaxPollTime(value).build();
    }

    /**
     * Returns a new transform which will operate on the text blocks with the
     * given minimum text length.
     */
    public Read withMinimumTextlength(Integer value) {
      return toBuilder().setMinimumTextLength(value).build();
    }

    /**
     * Returns a new transform which will use the synchronous reader.
     */
    public Read withParseSynchronously(Boolean value) {
      return toBuilder().setParseSynchronously(value).build();
    }

    /**
     * Path to Tika configuration resource.
     */
    public Read withOptions(TikaOptions options) {
      checkNotNull(options, "TikaOptions cannot be empty.");
      Builder builder = toBuilder();
      builder.setFilepattern(StaticValueProvider.of(options.getInput()))
             .setQueuePollTime(options.getQueuePollTime())
             .setQueueMaxPollTime(options.getQueueMaxPollTime())
             .setMinimumTextLength(options.getMinimumTextLength())
             .setParseSynchronously(options.getParseSynchronously());
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
      if (Boolean.TRUE.equals(options.getReadOutputMetadata())) {
        builder.setReadOutputMetadata(options.getReadOutputMetadata());
      }
      return builder.build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      checkNotNull(this.getFilepattern(), "Filepattern cannot be empty.");
      final Bounded<String> read = org.apache.beam.sdk.io.Read.from(new TikaSource(this));
      PCollection<String> pcol = input.getPipeline().apply(read);
      pcol.setCoder(getDefaultOutputCoder());
      return pcol;
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
      if (Boolean.TRUE.equals(getParseSynchronously())) {
        builder
          .add(DisplayData.item("parseMode", "synchronous")
            .withLabel("Parse Mode"));
      } else {
        builder
          .add(DisplayData.item("parseMode", "asynchronous")
            .withLabel("Parse Mode"));
        builder
          .add(DisplayData.item("queuePollTime", getQueuePollTime().toString())
            .withLabel("Queue Poll Time"))
        .add(DisplayData.item("queueMaxPollTime", getQueueMaxPollTime().toString())
          .withLabel("Queue Max Poll Time"));
      }
      Integer minTextLen = getMinimumTextLength();
      if (minTextLen != null && minTextLen > 0) {
        builder
        .add(DisplayData.item("minTextLen", getMinimumTextLength().toString())
          .withLabel("Minimum Text Length"));
      }
      if (Boolean.TRUE.equals(getReadOutputMetadata())) {
        builder
          .add(DisplayData.item("readOutputMetadata", "true")
            .withLabel("Read Output Metadata"));
      }
    }

    @Override
    protected Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }
}
