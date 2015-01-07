/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.util.Preconditions;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.util.gcsio.GoogleCloudStorageReadChannel;
import com.google.cloud.dataflow.sdk.util.gcsio.GoogleCloudStorageWriteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides operations on GCS.
 */
public class GcsUtil {
  /**
   * This is a {@link DefaultValueFactory} able to create a {@link GcsUtil} using
   * any transport flags specified on the {@link PipelineOptions}.
   */
  public static class GcsUtilFactory implements DefaultValueFactory<GcsUtil> {
    /**
     * Returns an instance of {@link GcsUtil} based on the
     * {@link PipelineOptions}.
     * <p>
     * If no instance has previously been created, one is created and the value
     * stored in {@code options}.
     */
    @Override
    public GcsUtil create(PipelineOptions options) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      LOG.debug("Creating new GcsUtil");
      return new GcsUtil(Transport.newStorageClient(gcsOptions).build(),
          gcsOptions.getExecutorService());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(GcsUtil.class);

  /** Maximum number of items to retrieve per Objects.List request. */
  private static final long MAX_LIST_ITEMS_PER_CALL = 1024;

  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^*?]*)[*?].*");

  private static final String RECURSIVE_WILDCARD = "[*]{2}";

  /**
   * A {@link Pattern} for globs with a recursive wildcard.
   */
  private static final Pattern RECURSIVE_GCS_PATTERN =
      Pattern.compile(".*" + RECURSIVE_WILDCARD + ".*");

  /////////////////////////////////////////////////////////////////////////////

  /** Client for the GCS API. */
  private final Storage storage;

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private final ApiErrorExtractor errorExtractor = new ApiErrorExtractor();

  // Exposed for testing.
  final ExecutorService executorService;

  /**
   * Returns true if the given GCS pattern is supported otherwise fails with an
   * exception.
   */
  public boolean isGcsPatternSupported(String gcsPattern) {
    if (RECURSIVE_GCS_PATTERN.matcher(gcsPattern).matches()) {
      throw new IllegalArgumentException("Unsupported wildcard usage in \"" + gcsPattern + "\": "
          + " recursive wildcards are not supported.");
    }

    return true;
  }

  private GcsUtil(Storage storageClient, ExecutorService executorService) {
    storage = storageClient;
    this.executorService = executorService;
  }

  /**
   * Expands a pattern into matched paths. The pattern path may contain
   * globs, which are expanded in the result.
   */
  public List<GcsPath> expand(GcsPath gcsPattern) throws IOException {
    Preconditions.checkArgument(isGcsPatternSupported(gcsPattern.getObject()));
    Matcher m = GLOB_PREFIX.matcher(gcsPattern.getObject());
    if (!m.matches()) {
      return Arrays.asList(gcsPattern);
    }

    // Part before the first wildcard character.
    String prefix = m.group("PREFIX");
    Pattern p = Pattern.compile(globToRegexp(gcsPattern.getObject()));
    LOG.info("matching files in bucket {}, prefix {} against pattern {}",
        gcsPattern.getBucket(), prefix, p.toString());

    // List all objects that start with the prefix (including objects in sub-directories).
    Storage.Objects.List listObject = storage.objects().list(gcsPattern.getBucket());
    listObject.setMaxResults(MAX_LIST_ITEMS_PER_CALL);
    listObject.setPrefix(prefix);

    String pageToken = null;
    List<GcsPath> results = new LinkedList<>();
    do {
      if (pageToken != null) {
        listObject.setPageToken(pageToken);
      }

      Objects objects = listObject.execute();
      Preconditions.checkNotNull(objects);

      if (objects.getItems() == null) {
        break;
      }

      // Filter objects based on the regex.
      for (StorageObject o : objects.getItems()) {
        String name = o.getName();
        // Skip directories, which end with a slash.
        if (p.matcher(name).matches() && !name.endsWith("/")) {
          LOG.debug("Matched object: {}", name);
          results.add(GcsPath.fromObject(o));
        }
      }

      pageToken = objects.getNextPageToken();
    } while (pageToken != null);

    return results;
  }

  /**
   * Returns the file size from GCS, or -1 if the file does not exist.
   */
  public long fileSize(GcsPath path) throws IOException {
    try {
      Storage.Objects.Get getObject =
          storage.objects().get(path.getBucket(), path.getObject());

      StorageObject object = getObject.execute();
      return object.getSize().longValue();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        return -1;
      }

      // Re-throw any other error.
      throw e;
    }
  }

  /**
   * Opens an object in GCS.
   *
   * <p> Returns a SeekableByteChannel which provides access to data in the bucket.
   *
   * @param path the GCS filename to read from
   * @return a SeekableByteChannel which can read the object data
   * @throws IOException
   */
  public SeekableByteChannel open(GcsPath path)
      throws IOException {
    return new GoogleCloudStorageReadChannel(storage, path.getBucket(),
            path.getObject(), errorExtractor);
  }

  /**
   * Creates an object in GCS.
   *
   * <p> Returns a WritableByteChannel which can be used to write data to the
   * object.
   *
   * @param path the GCS file to write to
   * @param type the type of object, eg "text/plain".
   * @return a Callable object which encloses the operation.
   * @throws IOException
   */
  public WritableByteChannel create(GcsPath path,
      String type) throws IOException {
    return new GoogleCloudStorageWriteChannel(
        executorService,
        storage,
        path.getBucket(),
        path.getObject(),
        type);
  }

  /**
   * Expands glob expressions to regular expressions.
   *
   * @param globExp the glob expression to expand
   * @return a string with the regular expression this glob expands to
   */
  static String globToRegexp(String globExp) {
    StringBuilder dst = new StringBuilder();
    char[] src = globExp.toCharArray();
    int i = 0;
    while (i < src.length) {
      char c = src[i++];
      switch (c) {
        case '*':
          dst.append("[^/]*");
          break;
        case '?':
          dst.append("[^/]");
          break;
        case '.':
        case '+':
        case '{':
        case '}':
        case '(':
        case ')':
        case '|':
        case '^':
        case '$':
          // These need to be escaped in regular expressions
          dst.append('\\').append(c);
          break;
        case '\\':
          i = doubleSlashes(dst, src, i);
          break;
        default:
          dst.append(c);
          break;
      }
    }
    return dst.toString();
  }

  private static int doubleSlashes(StringBuilder dst, char[] src, int i) {
    // Emit the next character without special interpretation
    dst.append('\\');
    if ((i - 1) != src.length) {
      dst.append(src[i]);
      i++;
    } else {
      // A backslash at the very end is treated like an escaped backslash
      dst.append('\\');
    }
    return i;
  }
}
