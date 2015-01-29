/**
 * Copyright 2013 Google Inc. All Rights Reserved.
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
import javax.net.ssl.SSLException;

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
  // setting the equivalent value inside HttpRequest.java which determines the low-level retries
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
        Preconditions.checkState(numBytesRead != 0, "Read 0 bytes without blocking!");
        if (numBytesRead < 0) {
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

          // Before performing lazy seek, explicitly close the underlying channel if necessary,
          // catching and ignoring SSLException since the retry indicates an error occurred, so
          // there's a high probability that SSL connections would be broken in a way that
          // causes close() itself to throw an exception, even though underlying sockets have
          // already been cleaned up; close() on an SSLSocketImpl requires a shutdown handshake
          // in order to shutdown cleanly, and if the connection has been broken already, then
          // this is not possible, and the SSLSocketImpl was already responsible for performing
          // local cleanup at the time the exception was raised.
          if (lazySeekPending && readChannel != null) {
            try {
              readChannel.close();
              readChannel = null;
            } catch (SSLException ssle) {
              LOG.warn("Got SSLException on readChannel.close() before retry; ignoring it.", ssle);
              readChannel = null;
            }
            // For "other" exceptions, we'll let it propagate out without setting readChannel to
            // null, in case the caller is able to handle it and then properly try to close()
            // again.
          }
          performLazySeek();
        }
      }
    } while (buffer.remaining() > 0);

    // If this method was called when the stream was already at EOF
    // (indicated by totalBytesRead == 0) then return EOF else,
    // return the number of bytes read.
    return (totalBytesRead == 0) ? -1 : totalBytesRead;
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
   * Closes this channel.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close()
      throws IOException {
    throwIfNotOpen();
    channelIsOpen = false;
    if (readChannel != null) {
      readChannel.close();
    }
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
    if ((size >= 0) && (newPosition >= size)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d",
              newPosition, size));
    }
  }

  /**
   * Seeks to the given position in the underlying stream.
   *
   * Note: Seek is an expensive operation because a new stream is opened each time.
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
    if (readChannel != null) {
      readChannel.close();
    }

    InputStream objectContentStream = openStreamAndSetSize(currentPosition);
    readChannel = Channels.newChannel(objectContentStream);
    lazySeekPending = false;
  }

  /**
   * Opens the underlying stream, sets its position to the given value and sets size based on
   * stream content size.
   *
   * @param newPosition position to seek into the new stream.
   * @throws IOException on IO error
   */
  protected InputStream openStreamAndSetSize(long newPosition)
      throws IOException {
    validatePosition(newPosition);
    Storage.Objects.Get getObject = gcs.objects().get(bucketName, objectName);
    // Set the range on the existing request headers which may have been initialized with things
    // like user-agent already.
    clientRequestHelper.getRequestHeaders(getObject)
        .setRange(String.format("bytes=%d-", newPosition));
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
    }

    String contentRange = response.getHeaders().getContentRange();
    if (response.getHeaders().getContentLength() != null) {
      size = response.getHeaders().getContentLength() + newPosition;
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
    return response.getContent();
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
}
