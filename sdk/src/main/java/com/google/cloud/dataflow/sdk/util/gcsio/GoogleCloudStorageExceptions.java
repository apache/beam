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

import com.google.api.client.util.Preconditions;
import com.google.common.base.Strings;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Miscellaneous helper methods for standardizing the types of exceptions thrown by the various
 * GCS-based FileSystems.
 */
public class GoogleCloudStorageExceptions {
  /**
   * Creates FileNotFoundException with suitable message for a GCS bucket or object.
   */
  public static FileNotFoundException getFileNotFoundException(
      String bucketName, String objectName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(bucketName),
        "bucketName must not be null or empty");
    if (objectName == null) {
      objectName = "";
    }
    return new FileNotFoundException(
        String.format("Item not found: %s/%s", bucketName, objectName));
  }

  /**
   * Creates a composite IOException out of multiple IOExceptions. If there is only a single
   * {@code innerException}, it will be returned as-is without wrapping into an outer exception.
   * it.
   */
  public static IOException createCompositeException(
      List<IOException> innerExceptions) {
    Preconditions.checkArgument(innerExceptions != null,
        "innerExceptions must not be null");
    Preconditions.checkArgument(innerExceptions.size() > 0,
        "innerExceptions must contain at least one element");

    if (innerExceptions.size() == 1) {
      return innerExceptions.get(0);
    }

    IOException combined = new IOException("Multiple IOExceptions.");
    for (IOException inner : innerExceptions) {
      combined.addSuppressed(inner);
    }
    return combined;
  }

  /**
   * Wraps the given IOException into another IOException, adding the given error message and a
   * reference to the supplied bucket and object. It allows one to know which bucket and object
   * were being accessed when the exception occurred for an operation.
   */
  public static IOException wrapException(IOException e, String message,
      String bucketName, String objectName) {
    String name = "bucket: " + bucketName;
    if (!Strings.isNullOrEmpty(objectName)) {
      name += ", object: " + objectName;
    }
    String fullMessage = String.format("%s: %s", message, name);
    return new IOException(fullMessage, e);
  }
}
