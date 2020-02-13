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
package org.apache.beam.runners.fnexecution.artifact;

import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link ArtifactRetrievalService} that loads artifacts as {@link ClassLoader} resources.
 *
 * <p>The retrieval token should be a path to a JSON-formatted ProxyManifest accessible via {@link
 * ClassLoader#getResource(String)} whose resource locations also point to paths loadable via {@link
 * ClassLoader#getResource(String)}.
 */
public class ClassLoaderArtifactRetrievalService extends AbstractArtifactRetrievalService {

  private final ClassLoader classLoader;

  public ClassLoaderArtifactRetrievalService() {
    this(ClassLoaderArtifactRetrievalService.class.getClassLoader());
  }

  public ClassLoaderArtifactRetrievalService(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public InputStream openManifest(String retrievalToken) throws IOException {
    return openUri(retrievalToken, retrievalToken);
  }

  @Override
  public InputStream openUri(String retrievalToken, String uri) throws IOException {
    if (uri.charAt(0) == '/') {
      uri = uri.substring(1);
    }
    InputStream result = classLoader.getResourceAsStream(uri);
    if (result == null) {
      throw new IOException("Unable to load " + uri + " with " + classLoader);
    }
    return result;
  }
}
