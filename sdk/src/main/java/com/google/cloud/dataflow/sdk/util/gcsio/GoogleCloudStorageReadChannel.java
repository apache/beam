/**
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.dataflow.sdk.util.gcsio;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.util.ApiErrorExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.regex.Pattern;

/**
 * Provides seekable read access to GCS.
 */
public class GoogleCloudStorageReadChannel implements SeekableByteChannel {
  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageReadChannel.class);

  // Used to separate elements of a Content-Range
  private static final Pattern SLASH = Pattern.compile("/");

  // GCS access instance.
  private Storage gcs;

  // Name of the bucket containing the object being read.
  private String bucketName;

  // Name of the object being read.
  private String objectName;

  // Read channel.
  private ReadableByteChannel readChannel;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen;

  // Current read position in the channel.
  private long currentPosition = -1;

  // When a caller calls position(long) to set stream position, we record the target position
  // and defer the actual seek operation until the caller tries to read from the channel.
  // This allows us to avoid an unnecessary seek to position 0 that would take place on creation
  // of this instance in cases where caller intends to start reading at some other offset.
  // If lazySeekPending is set to true, it indicates that a target position has been set
  // but the actual seek operation is still pending.
  private boolean lazySeekPending;

  // Size of the object being read.
  private long size = -1;

  // Maximum number of automatic retries when reading from the underlying channel without making
  // progress; each time at least one byte is successfully read, the counter of attempted retries
  // is reset.
  // TODO: Wire this setting out to GHFS; it should correspond to adding the wiring for
  // setting the equivalent value inside HttpRequest.java that determines the low-level retries
  // during "execute()" calls. The default in HttpRequest.java is also 10.
  private int maxRetries = 10;

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private final ApiErrorExtractor errorExtractor;

  // Sleeper used for waiting between retries.
  private Sleeper sleeper = Sleeper.DEFAULT;

  // The clock used by ExponentialBackOff to determine when the maximum total elapsed time has
  // passed doing a series of retries.
  private NanoClock clock = NanoClock.SYSTEM;

  // Lazily initialized BackOff for sleeping between retries; only ever initialized if a retry is
  // necessary.
  private BackOff backOff = null;

  // Settings used for instantiating the default BackOff used for determining wait time between
  // retries. TODO: Wire these out to be settable by the Hadoop configs.
  // The number of milliseconds to wait before the very first retry in a series of retries.
  public static final int DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS = 200;

  // The amount of jitter introduced when computing the next retry sleep interval so that when
  // many clients are retrying, they don't all retry at the same time.
  public static final double DEFAULT_BACKOFF_RANDOMIZATION_FACTOR = 0.5;

  // The base of the exponent used for exponential backoff; each subsequent sleep interval is
  // roughly this many times the previous interval.
  public static final double DEFAULT_BACKOFF_MULTIPLIER = 1.5;

  // The maximum amount of sleep between retries; at this point, there will be no further
  // exponential backoff. This prevents intervals from growing unreasonably large.
  public static final int DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS = 10 * 1000;

  // The maximum total time elapsed since the first retry over the course of a series of retries.
  // This makes it easier to bound the maximum time it takes to respond to a permanent failure
  // without having to calculate the summation of a series of exponentiated intervals while
  // accounting for the randomization of backoff intervals.
  public static final int DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS = 2 * 60 * 1000;

  // For files that have Content-Encoding: gzip set in the file metadata, the size of the response
  // from GCS is the size of the compressed file. However, the HTTP client wraps the content
  // in a GZIPInputStream, so the number of bytes that can be read from the stream may be greater
  // than the size of the response. In this case, we allow the position in the stream to be greater
  // than size when the position is validated.
  private FileEncoding fileEncoding = FileEncoding.UNINITIALIZED;

  // ClientRequestHelper to be used instead of calling final methods in client requests.
  private static ClientRequestHelper clientRequestHelper = new ClientRequestHelper();

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param gcs storage object instance
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException on IO error
   */
  public GoogleCloudStorageReadChannel(
      Storage gcs, String bucketName, String objectName, ApiErrorExtractor errorExtractor)
      throws IOException {
    this.gcs = gcs;
    this.bucketName = bucketName;
    this.objectName = objectName;
    this.errorExtractor = errorExtractor;
    channelIsOpen = true;
    position(0);
  }

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   * Used for unit testing only. Do not use elsewhere.
   *
   * @throws IOException on IO error
   */
  GoogleCloudStorageReadChannel()
      throws IOException {
    this.errorExtractor = null;
    channelIsOpen = true;
    position(0);
  }

  /**
   * Sets the ClientRequestHelper to be used instead of calling final methods in client requests.
   */
  static void setClientRequestHelper(ClientRequestHelper helper) {
    clientRequestHelper = helper;
  }

  /**
   * Sets the Sleeper used for sleeping between retries.
   */
  void setSleeper(Sleeper sleeper) {
    Preconditions.checkArgument(sleeper != null, "sleeper must not be null!");
    this.sleeper = sleeper;
  }

  /**
   * Sets the clock to be used for determining when max total time has elapsed doing retries.
   */
  void setNanoClock(NanoClock clock) {
    Preconditions.checkArgument(clock != null, "clock must not be null!");
    this.clock = clock;
  }

  /**
   * Sets the backoff for determining sleep duration between retries.
   *
   * @param backOff May be null to force the next usage to auto-initialize with default settings.
   */
  void setBackOff(BackOff backOff) {
    this.backOff = backOff;
  }

  /**
   * Gets the backoff used for determining sleep duration between retries. May be null if it was
   * never lazily initialized.
   */
  BackOff getBackOff() {
    return backOff;
  }

  /**
   * Helper for initializing the BackOff used for retries.
   */
  private BackOff createBackOff() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS)
        .setRandomizationFactor(DEFAULT_BACKOFF_RANDOMIZATION_FACTOR)
        .setMultiplier(DEFAULT_BACKOFF_MULTIPLIER)
        .setMaxIntervalMillis(DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS)
        .setMaxElapsedTimeMillis(DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS)
        .setNanoClock(clock)
        .build();
  }

  /**
   * Sets the number of times to automatically retry by re-opening the underlying readChannel
   * whenever an exception occurs while reading from it. The count of attempted retries is reset
   * whenever at least one byte is successfully read, so this number of retries refers to retries
   * made without achieving any forward progress.
   */
  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  /**
   * Reads from this channel and stores read data in the given buffer.
   *
   * <p> On unexpected failure, will attempt to close the channel and clean up state.
   *
   * @param buffer buffer to read data into
   * @return number of bytes read or -1 on end-of-stream
   * @throws java.io.IOException on IO error
   */
  @Override
  public int read(ByteBuffer buffer)
      throws IOException {
    throwIfNotOpen();

    // Don't try to read if the buffer has no space.
    if (buffer.remaining() == 0) {
      return 0;
    }

    // Perform a lazy seek if not done already.
    performLazySeek();

    int totalBytesRead = 0;
    int retriesAttempted = 0;

    // We read from a streaming source. We may not get all the bytes we asked for
    // in the first read. Therefore, loop till we either read the required number of
    // bytes or we reach end-of-stream.
    do {
      int remainingBeforeRead = buffer.remaining();
      try {
        int numBytesRead = readChannel.read(buffer);
        checkIOPrecondition(numBytesRead != 0, "Read 0 bytes without blocking");
        if (numBytesRead < 0) {
          // Check that we didn't get a premature End of Stream signal by checking the number of
          // bytes read against the stream size. Unfortunately we don't have information about the
          // actual size of the data stream when stream compression is used, so we can only ignore
          // this case here.
          checkIOPrecondition(fileEncoding == FileEncoding.GZIPPED || currentPosition == size,
              String.format(
                  "Received end of stream result before all the file data has been received; "
                  + "totalBytesRead: %s, currentPosition: %s, size: %s",
                  totalBytesRead, currentPosition, size));
          break;
        }
        totalBytesRead += numBytesRead;
        currentPosition += numBytesRead;

        // The count of retriesAttempted is per low-level readChannel.read call; each time we make
        // progress we reset the retry counter.
        retriesAttempted = 0;
      } catch (IOException ioe) {
        // TODO: Refactor any reusable logic for retries into a separate RetryHelper class.
        if (retriesAttempted == maxRetries) {
          LOG.warn("Already attempted max of {} retries while reading '{}'; throwing exception.",
              maxRetries, StorageResourceId.createReadableString(bucketName, objectName));
          throw ioe;
        } else {
          if (retriesAttempted == 0) {
            // If this is the first of a series of retries, we also want to reset the backOff
            // to have fresh initial values.
            if (backOff == null) {
              backOff = createBackOff();
            } else {
              backOff.reset();
            }
          }

          ++retriesAttempted;
          LOG.warn("Got exception while reading '{}'; retry # {}. Sleeping...",
              StorageResourceId.createReadableString(bucketName, objectName),
              retriesAttempted, ioe);

          try {
            boolean backOffSuccessful = BackOffUtils.next(sleeper, backOff);
            if (!backOffSuccessful) {
              LOG.warn("BackOff returned false; maximum total elapsed time exhausted. Giving up "
                      + "after {} retries for '{}'", retriesAttempted,
                      StorageResourceId.createReadableString(bucketName, objectName));
              throw ioe;
            }
          } catch (InterruptedException ie) {
            LOG.warn("Interrupted while sleeping before retry."
                + "Giving up after {} retries for '{}'", retriesAttempted,
                StorageResourceId.createReadableString(bucketName, objectName));
            ioe.addSuppressed(ie);
            throw ioe;
          }
          LOG.info("Done sleeping before retry for '{}'; retry # {}.",
              StorageResourceId.createReadableString(bucketName, objectName),
              retriesAttempted);

          if (buffer.remaining() != remainingBeforeRead) {
            int partialRead = remainingBeforeRead - buffer.remaining();
            LOG.info("Despite exception, had partial read of {} bytes; resetting retry count.",
                partialRead);
            retriesAttempted = 0;
            totalBytesRead += partialRead;
            currentPosition += partialRead;
          }

          // Force the stream to be reopened by seeking to the current position.
          long newPosition = currentPosition;
          currentPosition = -1;
          position(newPosition);

          // Before performing lazy seek, explicitly close the underlying channel if necessary.
          if (lazySeekPending && readChannel != null) {
            closeReadChannel();
          }
          performLazySeek();
        }
      } catch (RuntimeException r) {
        closeReadChannel();
        throw r;
      }
    } while (buffer.remaining() > 0);

    // If this method was called when the stream was already at EOF
    // (indicated by totalBytesRead == 0) then return EOF else,
    // return the number of bytes read.
    boolean isEndOfStream = (totalBytesRead == 0);
    if (isEndOfStream) {
      // Check that we didn't get a premature End of Stream signal by checking the number of bytes
      // read against the stream size. Unfortunately we don't have information about the actual size
      // of the data stream when stream compression is used, so we can only ignore this case here.
      checkIOPrecondition(fileEncoding == FileEncoding.GZIPPED || currentPosition == size,
          String.format("Failed to read any data before all the file data has been received; "
              + "currentPosition: %s, size: %s", currentPosition, size));
      return -1;
    } else {
      return totalBytesRead;
    }
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  /**
   * Tells whether this channel is open.
   *
   * @return a value indicating whether this channel is open
   */
  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

 /**
   * Closes the underlying {@link ReadableByteChannel}.
   *
   * <p>Catches and ignores all exceptions as there is not a lot the user can do to fix errors here
   * and a new connection will be needed. Especially SSLExceptions since the there's a high
   * probability that SSL connections would be broken in a way that causes
   * {@link Channel#close()} itself to throw an exception, even though underlying
   * sockets have already been cleaned up; close() on an SSLSocketImpl requires a shutdown
   * handshake in order to shutdown cleanly, and if the connection has been broken already, then
   * this is not possible, and the SSLSocketImpl was already responsible for performing local
   * cleanup at the time the exception was raised.
   */
  private void closeReadChannel() {
    if (readChannel != null) {
      try {
        readChannel.close();
      } catch (Exception e) {
        LOG.debug("Got an exception on readChannel.close(); ignoring it.", e);
      } finally {
        readChannel = null;
      }
    }
  }

  /**
   * Closes this channel.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close() throws IOException {
    if (!channelIsOpen) {
      LOG.warn(
          "Channel for {} is not open.",
          StorageResourceId.createReadableString(bucketName, objectName));
      return;
    }
    channelIsOpen = false;
    closeReadChannel();
  }

  /**
   * Returns this channel's current position.
   *
   * @return this channel's current position
   */
  @Override
  public long position()
      throws IOException {
    throwIfNotOpen();
    return currentPosition;
  }

  /**
   * Sets this channel's position.
   *
   * @param newPosition the new position, counting the number of bytes from the beginning.
   * @return this channel instance
   * @throws java.io.FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @Override
  public SeekableByteChannel position(long newPosition)
      throws IOException {
    throwIfNotOpen();

    // If the position has not changed, avoid the expensive operation.
    if (newPosition == currentPosition) {
      return this;
    }

    validatePosition(newPosition);
    currentPosition = newPosition;
    lazySeekPending = true;
    return this;
  }

  /**
   * Returns size of the object to which this channel is connected.
   *
   * @return size of the object to which this channel is connected
   * @throws IOException on IO error
   */
  @Override
  public long size()
      throws IOException {
    throwIfNotOpen();
    // Perform a lazy seek if not done already so that size of this channel is set correctly.
    performLazySeek();
    return size;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  /**
   * Sets size of this channel to the given value.
   */
  protected void setSize(long size) {
    this.size = size;
  }

  /**
   * Validates that the given position is valid for this channel.
   */
  protected void validatePosition(long newPosition) {
    // Validate: 0 <= newPosition
    if (newPosition < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid seek offset: position value (%d) must be >= 0", newPosition));
    }

    // Validate: newPosition < size
    // Note that we access this.size directly rather than calling size() to avoid initiating
    // lazy seek that leads to recursive error. We validate newPosition < size only when size of
    // this channel has been computed by a prior call. This means that position could be
    // potentially set to an invalid value (>= size) by position(long). However, that error
    // gets caught during lazy seek.
    // If a file is gzip encoded, the size of the response may be less than the number of bytes
    // that can be read. In this case, the new position may be a valid offset, and we proceed.
    // If not, then the size of the response is the number of bytes that can be read and we throw
    // an exception for an invalid seek.
    if ((size >= 0) && (newPosition >= size) && (fileEncoding != FileEncoding.GZIPPED)) {
      throw new IllegalArgumentException(String.format(
          "Invalid seek offset: position value (%d) must be between 0 and %d", newPosition, size));
    }
  }

  /**
   * Seeks to the given position in the underlying stream.
   *
   * <p>Note: Seek is an expensive operation because a new stream is opened each time.
   *
   * @throws java.io.FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  private void performLazySeek()
      throws IOException {

    // Return quickly if there is no pending seek operation.
    if (!lazySeekPending) {
      return;
    }

    // Close the underlying channel if it is open.
    closeReadChannel();

    // Open the stream and create the channel.
    InputStream objectContentStream = openStreamAndSetMetadata(currentPosition);
    readChannel = Channels.newChannel(objectContentStream);

    lazySeekPending = false;
  }

  /**
   * Retrieve the object's metadata from GCS.
   *
   * @throws IOException on IO error.
   */
  protected StorageObject getMetadata() throws IOException {
    Storage.Objects.Get getObject = gcs.objects().get(bucketName, objectName);
    try {
      StorageObject response = getObject.execute();
      return response;
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
      }
      String msg =
          "Error reading " + StorageResourceId.createReadableString(bucketName, objectName);
      throw new IOException(msg, e);
    }
  }

  /**
   * Returns the FileEncoding of a file given its metadata. Currently supports GZIPPED and OTHER.
   *
   * @param metadata the object's metadata.
   * @return FileEncoding.GZIPPED if the response from GCS will have gzip encoding or
   *         FileEncoding.OTHER otherwise.
   */
  protected FileEncoding getEncoding(StorageObject metadata) {
    String contentEncoding = metadata.getContentEncoding();
    return contentEncoding != null && contentEncoding.contains("gzip") ? FileEncoding.GZIPPED
        : FileEncoding.OTHER;
  }

  /**
   * Set the size of the content.
   *
   * <p>First, we examine the Content-Length header.  If it does not exists, we then look for and
   * parse the Content-Range header. If there is no way to determine the content length, an
   * exception is thrown. If the Content-Length header is present, then the offset is added to this
   * value (i.e., offset is the number of bytes that were not requested).
   *
   * @param response response to parse.
   * @param offset the number of bytes that were not requested.
   * @throws IOException on IO error.
   */
  protected void setSize(HttpResponse response, long offset) throws IOException {
    String contentRange = response.getHeaders().getContentRange();
    if (response.getHeaders().getContentLength() != null) {
      size = response.getHeaders().getContentLength() + offset;
    } else if (contentRange != null) {
      String sizeStr = SLASH.split(contentRange)[1];
      try {
        size = Long.parseLong(sizeStr);
      } catch (NumberFormatException e) {
        throw new IOException(
            "Could not determine size from response from Content-Range: " + contentRange, e);
      }
    } else {
      throw new IOException("Could not determine size of response");
    }
  }

  /**
   * Opens the underlying stream, sets its position to the given value and initializes the object's
   * metadata (size and encoding).
   *
   * <p>If the file encoding in GCS is gzip (and therefore the HTTP client will attempt to
   * decompress it), the entire file is always requested and we seek to the position requested. If
   * the file encoding is not gzip, only the remaining bytes to be read are requested from GCS.
   *
   * @param newPosition position to seek into the new stream.
   * @throws IOException on IO error
   */
  protected InputStream openStreamAndSetMetadata(long newPosition)
      throws IOException {
    if (fileEncoding == FileEncoding.UNINITIALIZED) {
      StorageObject metadata = getMetadata();
      fileEncoding = getEncoding(metadata);
    }
    validatePosition(newPosition);
    Storage.Objects.Get getObject = gcs.objects().get(bucketName, objectName);
    // Set the range on the existing request headers that may have been initialized with things
    // like user-agent already. If the file is gzip encoded, request the entire file.
    clientRequestHelper.getRequestHeaders(getObject).setRange(
        String.format("bytes=%d-", fileEncoding == FileEncoding.GZIPPED ? 0 : newPosition));
    HttpResponse response;
    try {
      response = getObject.executeMedia();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions
            .getFileNotFoundException(bucketName, objectName);
      } else if (errorExtractor.rangeNotSatisfiable(e)
                 && newPosition == 0
                 && size == -1) {
        // We don't know the size yet (size == -1) and we're seeking to byte 0, but got 'range
        // not satisfiable'; the object must be empty.
        LOG.info("Got 'range not satisfiable' for reading {} at position 0; assuming empty.",
            StorageResourceId.createReadableString(bucketName, objectName));
        size = 0;
        return new ByteArrayInputStream(new byte[0]);
      } else {
        String msg = String.format("Error reading %s at position %d",
            StorageResourceId.createReadableString(bucketName, objectName), newPosition);
        throw new IOException(msg, e);
      }
    } catch (RuntimeException r) {
      closeReadChannel();
      throw r;
    }

    InputStream content = response.getContent();
    // If the file is gzip encoded, we requested the entire file and need to seek in the content
    // to the desired position.  If it is not, we only requested the bytes we haven't read.
    setSize(response, fileEncoding == FileEncoding.GZIPPED ? 0 : newPosition);
    if (fileEncoding == FileEncoding.GZIPPED) {
      content.skip(newPosition);
    }

    return content;
  }

  /**
   * Throws if this channel is not currently open.
   */
  private void throwIfNotOpen()
      throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Throws an IOException if precondition is false.
   *
   * <p>This method should be used in place of Preconditions.checkState in cases where the
   * precondition is derived from the status of the IO operation. That makes it possible to retry
   * the operation by catching IOException.
   */
  private void checkIOPrecondition(boolean precondition, String errorMessage) throws IOException {
    if (!precondition) {
      throw new IOException(errorMessage);
    }
  }

  private static enum FileEncoding {
    UNINITIALIZED, GZIPPED, OTHER;
  }
}
