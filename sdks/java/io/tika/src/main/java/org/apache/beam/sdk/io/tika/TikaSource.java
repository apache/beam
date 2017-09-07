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
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Implementation detail of {@link TikaIO.Read}.
 *
 * <p>A {@link Source} which can represent the content of the files parsed by Apache Tika.
 */
class TikaSource extends BoundedSource<String> {
  private static final long serialVersionUID = -509574062910491122L;
  private static final Logger LOG = LoggerFactory.getLogger(TikaSource.class);

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

  static class TikaReader extends BoundedReader<String> {
    private ExecutorService execService;
    private final ContentHandlerImpl tikaHandler = new ContentHandlerImpl();
    private String current;
    private TikaSource source;
    private String filePath;
    private TikaIO.Read spec;
    private org.apache.tika.metadata.Metadata tikaMetadata;
    private Iterator<String> metadataIterator;

    TikaReader(TikaSource source, String filePath) {
      this.source = source;
      this.filePath = filePath;
      this.spec = source.getTikaInputRead();
    }

    @Override
    public boolean start() throws IOException {
      final InputStream is = TikaInputStream.get(Paths.get(filePath));
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
      tikaMetadata = spec.getInputMetadata() != null ? spec.getInputMetadata()
          : new org.apache.tika.metadata.Metadata();

      if (spec.getMinimumTextLength() != null) {
        tikaHandler.setMinTextLength(spec.getMinimumTextLength());
      }

      if (!Boolean.TRUE.equals(spec.getParseSynchronously())) {
        // Try to parse the file on the executor thread to make the best effort
        // at letting the pipeline thread advancing over the file content
        // without immediately parsing all of it
        execService = Executors.newFixedThreadPool(1);
        execService.submit(new Runnable() {
          public void run() {
            try {
              parser.parse(is, tikaHandler, tikaMetadata, context);
              is.close();
            } catch (Exception ex) {
              tikaHandler.setParseException(ex);
            }
          }
        });
      } else {
        // Some parsers might not be able to report the content in chunks.
        // It does not make sense to create extra threads in such cases
        try {
          parser.parse(is, tikaHandler, tikaMetadata, context);
        } catch (Exception ex) {
          throw new IOException(ex);
        } finally {
          is.close();
        }
      }
      return advanceToNext();
    }

    @Override
    public boolean advance() throws IOException {
      checkState(current != null, "Call start() before advance()");
      return advanceToNext();
    }

    protected boolean advanceToNext() throws IOException {
      current = null;
      // The content is reported first
      if (metadataIterator == null) {
        // Check if some content is already available
        current = tikaHandler.getCurrent();

        if (current == null && !Boolean.TRUE.equals(spec.getParseSynchronously())) {
          long maxPollTime = 0;
          long configuredMaxPollTime = spec.getQueueMaxPollTime() == null
              ? TikaIO.Read.DEFAULT_QUEUE_MAX_POLL_TIME : spec.getQueueMaxPollTime();
          long configuredPollTime = spec.getQueuePollTime() == null
              ? TikaIO.Read.DEFAULT_QUEUE_POLL_TIME : spec.getQueuePollTime();

          // Poll the queue till the next piece of data is available
          while (current == null && maxPollTime < configuredMaxPollTime) {
            boolean docEnded = tikaHandler.waitForNext(configuredPollTime);
            current = tikaHandler.getCurrent();
            // End of Document ?
            if (docEnded) {
              break;
            }
            maxPollTime += spec.getQueuePollTime();
          }
        }
        // No more content ?
        if (current == null && Boolean.TRUE.equals(spec.getReadOutputMetadata())) {
          // Time to report the metadata
          metadataIterator = Arrays.asList(tikaMetadata.names()).iterator();
        }
      }

      if (metadataIterator != null && metadataIterator.hasNext()) {
          String key = metadataIterator.next();
          // The metadata name/value separator can be configured if needed
          current = key + "=" + tikaMetadata.get(key);
      }
      return current != null;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      if (execService != null) {
          execService.shutdown();
      }
    }

    ExecutorService getExecutorService() {
      return execService;
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
      return source;
    }
  }

  /**
   * Tika Parser Content Handler.
   */
  static class ContentHandlerImpl extends DefaultHandler {
    private Queue<String> queue = new ConcurrentLinkedQueue<>();
    private volatile boolean documentEnded;
    private volatile Exception parseException;
    private volatile String current;
    private int minTextLength;

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
      String value = new String(ch, start, length).trim();
      if (!value.isEmpty()) {
        if (minTextLength <= 0) {
          queue.add(value);
        } else {
          current = current == null ? value : current + " " + value;
          if (current.length() >= minTextLength) {
            queue.add(current);
            current = null;
          }
        }
      }
    }

    public void setParseException(Exception ex) {
      this.parseException = ex;
    }

    public synchronized boolean waitForNext(long pollTime) throws IOException {
      if (!documentEnded) {
        try {
          wait(pollTime);
        } catch (InterruptedException ex) {
          // continue;
        }
      }
      return documentEnded;
    }

    @Override
    public synchronized void endDocument() throws SAXException {
      this.documentEnded = true;
      notify();
    }

    public String getCurrent() throws IOException {
      checkParseException();
      String value = queue.poll();
      if (value == null && documentEnded) {
        return current;
      } else {
        return value;
      }
    }
    public void checkParseException() throws IOException {
      if (parseException != null) {
        throw new IOException(parseException);
      }
    }

    public void setMinTextLength(int minTextLength) {
      this.minTextLength = minTextLength;
    }
  }
}
