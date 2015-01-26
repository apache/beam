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

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.Preconditions;
import com.google.api.services.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * Implements WritableByteChannel to provide write access to GCS.
 */
public class GoogleCloudStorageWriteChannel
    implements WritableByteChannel {

  // The minimum logging interval for upload progress.
  private static final long MIN_LOGGING_INTERVAL_MS = 60000L;

  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageWriteChannel.class);

  // Buffering used in the upload path:
  // There are a series of buffers used along the upload path. It is important to understand their
  // function before tweaking their values.
  //
  // Note: Most values are already tweaked based on performance measurements. If you want to change
  // buffer sizes, you should change only 1 buffer size at a time to make sure you understand
  // the correlation between various buffers and their characteristics.
  //
  // Upload path:
  // Uploading a file involves the following steps:
  // -- caller creates a write stream. It involves creating a pipe between data writer (controlled
  // by the caller) and data uploader.
  // The writer and the uploader are on separate threads. That is, pipe operation is asynchronous
  // between its
  // two ends.
  // -- caller puts data in a ByteBuffer and calls write(ByteBuffer). The write() method starts
  // writing into sink end of the pipe. It blocks if pipe buffer is full till the other end
  // reads data to make space.
  // -- MediaHttpUploader code keeps on reading from the source end of the pipe till it has
  // uploadBufferSize amount of data.
  //
  // The following buffers are involved along the above path:
  // -- ByteBuffer passed by caller. We have no control over its size.
  //
  // -- Pipe buffer.
  // size = UPLOAD_PIPE_BUFFER_SIZE_DEFAULT (1 MB)
  // Increasing size does not have noticeable difference on performance.
  //
  // -- Buffer used by Java client
  // code.
  // size = UPLOAD_CHUNK_SIZE_DEFAULT (64 MB)

  // A pipe that connects write channel used by caller to the input stream used by GCS uploader.
  // The uploader reads from input stream which blocks till a caller writes some data to the
  // write channel (pipeSinkChannel below). The pipe is formed by connecting pipeSink to pipeSource.
  private PipedOutputStream pipeSink;
  private PipedInputStream pipeSource;

  // Size of buffer used by upload pipe.
  private int pipeBufferSize = UPLOAD_PIPE_BUFFER_SIZE_DEFAULT;

  // A channel wrapper over pipeSink.
  private WritableByteChannel pipeSinkChannel;

  // Upload operation that takes place on a separate thread.
  private UploadOperation uploadOperation;

  // Default GCS upload granularity.
  private static final int GCS_UPLOAD_GRANULARITY = 8 * 1024 * 1024;

  // Upper limit on object size.
  // We use less than 250GB limit to avoid potential boundary errors
  // in scotty/blobstore stack.
  private static final long UPLOAD_MAX_SIZE = 249 * 1024 * 1024 * 1024L;

  // Chunk size to use. Limit the amount of memory used in low memory
  // environments such as small AppEngine instances.
  private static final int UPLOAD_CHUNK_SIZE_DEFAULT =
      Runtime.getRuntime().totalMemory() < 512 * 1024 * 1024
      ? GCS_UPLOAD_GRANULARITY : 8 * GCS_UPLOAD_GRANULARITY;

  // If true, we get very high write throughput but writing files larger than UPLOAD_MAX_SIZE
  // will not succeed. Set it to false to allow larger files at lower throughput.
  private static boolean limitFileSizeTo250Gb = true;

  // Chunk size to use.
  static int uploadBufferSize = UPLOAD_CHUNK_SIZE_DEFAULT;

  // Default size of upload buffer.
  public static final int UPLOAD_PIPE_BUFFER_SIZE_DEFAULT = 1 * 1024 * 1024;

  // ClientRequestHelper to be used instead of calling final methods in client requests.
  private static ClientRequestHelper clientRequestHelper = new ClientRequestHelper();

  /**
   * Allows running upload operation on a background thread.
   */
  static class UploadOperation
      implements Runnable {

    // Object to be uploaded. This object declared final for safe object publishing.
    private final Storage.Objects.Insert insertObject;

    // Exception encountered during upload.
    Throwable exception;

    // Allows other threads to wait for this operation to be complete. This object declared final
    // for safe object publishing.
    final CountDownLatch uploadDone = new CountDownLatch(1);

    // Read end of the pipe. This object declared final for safe object publishing.
    private final InputStream pipeSource;

    /**
     * Constructs an instance of UploadOperation.
     *
     * @param insertObject object to be uploaded
     */
    public UploadOperation(Storage.Objects.Insert insertObject, InputStream pipeSource) {
      this.insertObject = insertObject;
      this.pipeSource = pipeSource;
    }

    /**
     * Gets exception/error encountered during upload or null.
     */
    public Throwable exception() {
      return exception;
    }

    /**
     * Runs the upload operation.
     */
    @Override
    public void run() {
      try {
        insertObject.execute();
      } catch (Throwable t) {
        exception = t;
        LOG.error("Upload failure", t);
      } finally {
        uploadDone.countDown();
        try {
          // Close this end of the pipe so that the writer at the other end
          // will not hang indefinitely.
          pipeSource.close();
        } catch (IOException ioe) {
          LOG.error("Error trying to close pipe.source()", ioe);
          // Log and ignore IOException while trying to close the channel,
          // as there is not much we can do about it.
        }
      }
    }

    public void waitForCompletion() {
      do {
        try {
          uploadDone.await();
        } catch (InterruptedException e) {
          // Ignore it and continue to wait.
        }
      } while(uploadDone.getCount() > 0);
    }
  }

  /**
   * Constructs an instance of GoogleCloudStorageWriteChannel.
   *
   * @param threadPool thread pool to use for running the upload operation
   * @param gcs storage object instance
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @throws IOException on IO error
   */
  public GoogleCloudStorageWriteChannel(
      ExecutorService threadPool, Storage gcs, String bucketName,
      String objectName, String contentType)
      throws IOException {
    init(threadPool, gcs, bucketName, objectName, contentType);
  }

  /**
   * Sets the ClientRequestHelper to be used instead of calling final methods in client requests.
   */
  static void setClientRequestHelper(ClientRequestHelper helper) {
    clientRequestHelper = helper;
  }

  /**
   * Writes contents of the given buffer to this channel.
   *
   * Note: The data that one writes gets written to a pipe which may not block
   * if the pipe has sufficient buffer space. A success code returned from this method
   * does not mean that the specific data was successfully written to the underlying
   * storage. It simply means that there is no error at present. The data upload
   * may encounter an error on a separate thread. Such error is not ignored;
   * it shows up as an exception during a subsequent call to write() or close().
   * The only way to be sure of successful upload is when the close() method
   * returns successfully.
   *
   * @param buffer buffer to write
   * @throws IOException on IO error
   */
  @Override
  public int write(ByteBuffer buffer)
      throws IOException {
    throwIfNotOpen();

    // No point in writing further if upload failed on another thread.
    throwIfUploadFailed();

    return pipeSinkChannel.write(buffer);
  }

  /**
   * Tells whether this channel is open.
   *
   * @return a value indicating whether this channel is open
   */
  @Override
  public boolean isOpen() {
    return (pipeSinkChannel != null) && pipeSinkChannel.isOpen();
  }

  /**
   * Closes this channel.
   *
   * Note:
   * The method returns only after all data has been successfully written to GCS
   * or if there is a non-retry-able error.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close()
      throws IOException {
    throwIfNotOpen();
    try {
      pipeSinkChannel.close();
      uploadOperation.waitForCompletion();
      throwIfUploadFailed();
    } finally {
      pipeSinkChannel = null;
      pipeSink = null;
      pipeSource = null;
      uploadOperation = null;
    }
  }

  /**
   * Sets size of upload buffer used.
   */
  public static void setUploadBufferSize(int bufferSize) {
    Preconditions.checkArgument(bufferSize > 0,
        "Upload buffer size must be great than 0.");
    if (bufferSize % GCS_UPLOAD_GRANULARITY != 0) {
      LOG.warn("Upload buffer size should be a multiple of {} for best performance, got {}",
          GCS_UPLOAD_GRANULARITY, bufferSize);
    }
    GoogleCloudStorageWriteChannel.uploadBufferSize = bufferSize;
  }

  /**
   * Enables or disables hard limit of 250GB on size of uploaded files.
   *
   * If enabled, we get very high write throughput but writing files larger than UPLOAD_MAX_SIZE
   * will not succeed. Set it to false to allow larger files at lower throughput.
   */
  public static void enableFileSizeLimit250Gb(boolean enableLimit) {
    GoogleCloudStorageWriteChannel.limitFileSizeTo250Gb = enableLimit;
  }

  /**
   * Initializes an instance of GoogleCloudStorageWriteChannel.
   *
   * @param threadPool thread pool to use for running the upload operation
   * @param gcs storage object instance
   * @param bucketName name of the bucket in which to create object
   * @param objectName name of the object to create
   * @throws IOException on IO error
   */
  private void init(
      ExecutorService threadPool, Storage gcs, String bucketName,
      String objectName, String contentType)
      throws IOException {
    // Create a pipe such that its one end is connected to the input stream used by
    // the uploader and the other end is the write channel used by the caller.
    pipeSource = new PipedInputStream(pipeBufferSize);
    pipeSink = new PipedOutputStream(pipeSource);
    pipeSinkChannel = Channels.newChannel(pipeSink);

    // Connect pipe-source to the stream used by uploader.
    InputStreamContent objectContentStream =
        new InputStreamContent(contentType, pipeSource);
    // Indicate that we do not know length of file in advance.
    objectContentStream.setLength(-1);
    objectContentStream.setCloseInputStream(false);
    Storage.Objects.Insert insertObject =
        gcs.objects().insert(bucketName, null, objectContentStream);
    insertObject.setName(objectName);
    insertObject.setDisableGZipContent(true);
    insertObject.getMediaHttpUploader().setProgressListener(
        new LoggingMediaHttpUploaderProgressListener(objectName, MIN_LOGGING_INTERVAL_MS));

    // Insert necessary http headers to enable 250GB limit+high throughput if so configured.
    if (limitFileSizeTo250Gb) {
      HttpHeaders headers = clientRequestHelper.getRequestHeaders(insertObject);
      headers.set("X-Goog-Upload-Desired-Chunk-Granularity", GCS_UPLOAD_GRANULARITY);
      headers.set("X-Goog-Upload-Max-Raw-Size", UPLOAD_MAX_SIZE);
    }
    // Change chunk size from default value (10MB) to one that yields higher performance.
    clientRequestHelper.setChunkSize(insertObject, uploadBufferSize);

    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    uploadOperation = new UploadOperation(insertObject, pipeSource);
    threadPool.execute(uploadOperation);
  }

  /**
   * Throws if this channel is not currently open.
   *
   * @throws IOException on IO error
   */
  private void throwIfNotOpen()
      throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Throws if upload operation failed. Propagates any errors.
   *
   * @throws IOException on IO error
   */
  private void throwIfUploadFailed()
      throws IOException {
    if ((uploadOperation != null) && (uploadOperation.exception() != null)) {
      if (uploadOperation.exception() instanceof Error) {
        throw (Error) uploadOperation.exception();
      }
      throw new IOException(String.format("Failed to write to GCS path %s.", getPrintableGCSPath()),
          uploadOperation.exception());
    }
  }

  /**
   * Gets the printable GCS path of the current channel.
   */
  private String getPrintableGCSPath() {
    // The bucket and object name are fields stored in the uploadOperation.
    return String.format("gs://%s/%s", uploadOperation.insertObject.getBucket(),
        uploadOperation.insertObject.getName());
  }
}
