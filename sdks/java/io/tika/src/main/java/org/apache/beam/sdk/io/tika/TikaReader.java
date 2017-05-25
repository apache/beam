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

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A {@link BoundedReader}
 * which can decode the sequences of characters reported by Apache Tika.
 *
 * <p>It uses {@link ExecutorService} and {@link ConcurrentLinkedQueue} to make
 * the characters reported by Apache Tika SAX {@link Parser} accessible via
 * {@link BoundedReader} API.
 *
 * <p>See {@link TikaSource} for further details.
 */
class TikaReader extends BoundedReader<String> {
  private ExecutorService execService;
  private final ContentHandlerImpl tikaHandler = new ContentHandlerImpl();
  private String current;
  private TikaSource source;
  private String filePath;
  private TikaIO.Read spec;
  private Metadata tikaMetadata;
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
        : new Metadata();

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

  /**
   * Tika Parser Content Handler.
   */
  static class ContentHandlerImpl extends DefaultHandler {
    private Queue<String> queue = new ConcurrentLinkedQueue<>();
    private volatile boolean documentEnded;
    private volatile Exception parseException;

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
      String value = new String(ch, start, length).trim();
      if (!value.isEmpty()) {
        //TODO: review how concatenating multiple chunks into a minimum length text fragment
        // can optionally help optimize PTransform
        queue.add(value);
      }
    }

    public void setParseException(Exception ex) {
      this.parseException = ex;
    }

    public synchronized boolean waitForNext(long pollTime) throws IOException {
      try {
        wait(pollTime);
      } catch (InterruptedException ex) {
        // continue;
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
      return queue.poll();
    }
    public void checkParseException() throws IOException {
      if (parseException != null) {
        throw new IOException(parseException);
      }
    }
  }
}
