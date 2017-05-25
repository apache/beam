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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation detail of {@link TikaIO.Read}.
 *
 * <p>A {@link Source} which can represent the content of the files parsed by Apache Tika.
 */
class TikaSource extends BoundedSource<String> {
  private static final long serialVersionUID = -509574062910491122L;
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedSource.class);

  @Nullable
  private MatchResult.Metadata singleFileMetadata;
  private final Mode mode;
  private final TikaIO.Read spec;

  /**
   * Source mode.
   */
  public enum Mode {
    FILEPATTERN, SINGLE_FILE
  }

  TikaSource(TikaIO.Read spec) {
    this.mode = Mode.FILEPATTERN;
    this.spec = spec;
  }

  TikaSource(Metadata fileMetadata, TikaIO.Read spec) {
    mode = Mode.SINGLE_FILE;
    this.singleFileMetadata = checkNotNull(fileMetadata, "fileMetadata");
    this.spec = spec;
  }

  @Override
  public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
    this.validate();
    checkState(spec.getFilepattern().isAccessible(),
        "Cannot create a Tika reader without access to the file"
        + " or pattern specification: {}.", spec.getFilepattern());
    if (spec.getTikaConfigPath() != null) {
      checkState(spec.getTikaConfigPath().isAccessible(),
        "Cannot create a Tika reader without access to its configuration",
        spec.getTikaConfigPath());
    }

    String fileOrPattern = spec.getFilepattern().get();
    if (mode == Mode.FILEPATTERN) {
      List<Metadata> fileMetadata = expandFilePattern(fileOrPattern);
      List<TikaReader> fileReaders = new ArrayList<>();
      for (Metadata metadata : fileMetadata) {
        fileReaders.add(new TikaReader(this, metadata.resourceId().toString()));
      }
      if (fileReaders.size() == 1) {
        return fileReaders.get(0);
      }
      return new FilePatternTikaReader(this, fileReaders);
    } else {
      return new TikaReader(this, singleFileMetadata.resourceId().toString());
    }

  }

  @Override
  public List<? extends TikaSource> split(long desiredBundleSizeBytes, PipelineOptions options)
    throws Exception {
    if (mode == Mode.SINGLE_FILE) {
      return ImmutableList.of(this);
    } else {
      List<Metadata> fileMetadata = expandFilePattern(spec.getFilepattern().get());

      List<TikaSource> splitResults = new LinkedList<>();
      for (Metadata metadata : fileMetadata) {
        splitResults.add(new TikaSource(metadata, spec));
      }
      return splitResults;
    }
  }

  public TikaIO.Read getTikaInputRead() {
    return spec;
  }

  @Override
  public Coder<String> getDefaultOutputCoder() {
    return StringUtf8Coder.of();
  }

  @Override
  public void validate() {
    switch (mode) {
    case FILEPATTERN:
      checkArgument(this.singleFileMetadata == null,
        "Unexpected initialized singleFileMetadata value");
      break;
    case SINGLE_FILE:
      checkNotNull(this.singleFileMetadata,
        "Unexpected null singleFileMetadata value");
      break;
    default:
      throw new IllegalStateException("Unknown mode: " + mode);
    }
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    long totalSize = 0;
    List<Metadata> fileMetadata = expandFilePattern(spec.getFilepattern().get());
    for (Metadata metadata : fileMetadata) {
      totalSize += metadata.sizeBytes();
    }
    return totalSize;
  }

  Mode getMode() {
    return this.mode;
  }

  Metadata getSingleFileMetadata() {
    return this.singleFileMetadata;
  }

  private static List<Metadata> expandFilePattern(String fileOrPattern) throws IOException {
    MatchResult matches = Iterables.getOnlyElement(
      FileSystems.match(Collections.singletonList(fileOrPattern)));
    LOG.info("Matched {} files for pattern {}", matches.metadata().size(), fileOrPattern);
    List<Metadata> metadata = ImmutableList.copyOf(matches.metadata());
    checkArgument(!metadata.isEmpty(),
      "Unable to find any files matching %s", fileOrPattern);

    return metadata;
  }

  /**
   *  FilePatternTikaReader.
   *  TODO: This is mostly a copy of FileBasedSource internal file-pattern reader
   *        so that code would need to be generalized as part of the future contribution
   */
  static class FilePatternTikaReader extends BoundedReader<String> {
    private final TikaSource source;
    final ListIterator<TikaReader> fileReadersIterator;
    TikaReader currentReader = null;

    public FilePatternTikaReader(TikaSource source, List<TikaReader> fileReaders) {
      this.source = source;
      this.fileReadersIterator = fileReaders.listIterator();
    }

    @Override
    public boolean start() throws IOException {
      return startNextNonemptyReader();
    }

    @Override
    public boolean advance() throws IOException {
      checkState(currentReader != null, "Call start() before advance()");
      if (currentReader.advance()) {
        return true;
      }
      return startNextNonemptyReader();
    }

    private boolean startNextNonemptyReader() throws IOException {
      while (fileReadersIterator.hasNext()) {
        currentReader = fileReadersIterator.next();
        if (currentReader.start()) {
          return true;
        }
        currentReader.close();
      }
      return false;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      return currentReader.getCurrent();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return currentReader.getCurrentTimestamp();
    }

    @Override
    public void close() throws IOException {
      if (currentReader != null) {
        currentReader.close();
      }
      while (fileReadersIterator.hasNext()) {
        fileReadersIterator.next().close();
      }
    }

    @Override
    public TikaSource getCurrentSource() {
      return source;
    }
  }
}
