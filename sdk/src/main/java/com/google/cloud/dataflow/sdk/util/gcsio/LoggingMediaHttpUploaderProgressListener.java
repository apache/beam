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

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploader.UploadState;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Logs the status of uploads. At the beginning, during, and
 * at the end of the upload, emits relevant statistics such as how many bytes
 * uploaded and the rate at which the upload is progressing.
 * <p>
 * A new instance of this progress listener should be used for each MediaHttpUploader.
 */
class LoggingMediaHttpUploaderProgressListener implements MediaHttpUploaderProgressListener {
  private static final Logger LOG =
      LoggerFactory.getLogger(MediaHttpUploaderProgressListener.class);
  private static final double BYTES_IN_MB = 1048576.0;
  private final long minLoggingInterval;
  private final String name;
  private long startTime;
  private long prevTime;
  private long prevUploadedBytes;

  /**
   * Creates a upload progress listener which emits relevant statistics about the
   * progress of the upload.
   * @param name The name of the resource being uploaded.
   * @param minLoggingInterval The minimum amount of time (millis) between logging upload progress.
   */
  LoggingMediaHttpUploaderProgressListener(String name, long minLoggingInterval) {
    this.name = name;
    this.minLoggingInterval = minLoggingInterval;
  }

  @Override
  public void progressChanged(MediaHttpUploader uploader) throws IOException {
    progressChanged(LOG,
        uploader.getUploadState(),
        uploader.getNumBytesUploaded(),
        System.currentTimeMillis());
  }

  void progressChanged(Logger log, UploadState uploadState, long bytesUploaded, long currentTime) {
    switch (uploadState) {
      case INITIATION_STARTED:
        startTime = currentTime;
        prevTime = currentTime;
        log.debug("Uploading: {}", name);
        break;
      case MEDIA_IN_PROGRESS:
        // Limit messages to be emitted for in progress uploads.
        if (currentTime > prevTime + minLoggingInterval) {
          double averageRate = (bytesUploaded / BYTES_IN_MB)
                               / ((currentTime - startTime) / 1000.0);
          double currentRate = ((bytesUploaded - prevUploadedBytes) / BYTES_IN_MB)
                               / ((currentTime - prevTime) / 1000.0);
          log.debug(String.format(
              "Uploading: %s Average Rate: %.3f MiB/s, Current Rate: %.3f MiB/s, Total: %.3f MiB",
              name, averageRate, currentRate, bytesUploaded / BYTES_IN_MB));
          prevTime = currentTime;
          prevUploadedBytes = bytesUploaded;
        }
        break;
      case MEDIA_COMPLETE:
        log.debug("Finished Uploading: {}", name);
        break;
      default:
    }
  }
}
