/*
 * Copyright (C) 2015 Google Inc.
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
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implements a response intercepter that logs the upload id if the upload
 * id header exists and it is the first request (does not have upload_id parameter in the request).
 * Only logs if debug level is enabled.
 */
public class UploadIdResponseInterceptor implements HttpResponseInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(UploadIdResponseInterceptor.class);
  private static final String UPLOAD_ID_PARAM = "upload_id";
  private static final String UPLOAD_TYPE_PARAM = "uploadType";
  private static final String UPLOAD_HEADER = "X-GUploader-UploadID";

  @Override
  public void interceptResponse(HttpResponse response) throws IOException {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    String uploadId = response.getHeaders().getFirstHeaderStringValue(UPLOAD_HEADER);
    if (uploadId == null) {
      return;
    }

    GenericUrl url = response.getRequest().getUrl();
    // The check for no upload id limits the output to one log line per upload.
    // The check for upload type makes sure this is an upload and not a read.
    if (url.get(UPLOAD_ID_PARAM) == null && url.get(UPLOAD_TYPE_PARAM) != null) {
      LOG.debug(
          "Upload ID for url {} on worker {} is {}",
          url,
          System.getProperty("worker_id"),
          uploadId);
    }
  }
}
