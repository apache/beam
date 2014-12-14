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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.google.api.client.googleapis.media.MediaHttpUploader.UploadState;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

/** Unit tests for {@link LoggingMediaHttpUploaderProgressListener}. */
@RunWith(JUnit4.class)
public class LoggingMediaHttpUploaderProgressListenerTest {
  @Mock
  private Logger mockLogger;
  private LoggingMediaHttpUploaderProgressListener listener;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    listener = new LoggingMediaHttpUploaderProgressListener("NAME", 60000L);
  }

  @Test
  public void testLoggingInitiation() {
    listener.progressChanged(mockLogger, UploadState.INITIATION_STARTED, 0L, 0L);
    verify(mockLogger).debug("Uploading: {}", "NAME");
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  public void testLoggingProgressAfterSixtySeconds() {
    listener.progressChanged(mockLogger, UploadState.MEDIA_IN_PROGRESS, 10485760L, 60001L);
    listener.progressChanged(mockLogger, UploadState.MEDIA_IN_PROGRESS, 104857600L, 120002L);
    verify(mockLogger).debug(
        "Uploading: NAME Average Rate: 0.167 MiB/s, Current Rate: 0.167 MiB/s, Total: 10.000 MiB");
    verify(mockLogger).debug(
        "Uploading: NAME Average Rate: 0.833 MiB/s, Current Rate: 1.500 MiB/s, Total: 100.000 MiB");
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  public void testSkippingLoggingAnInProgressUpdate() {
    listener.progressChanged(mockLogger, UploadState.MEDIA_IN_PROGRESS, 104857600L, 60000L);
    verifyZeroInteractions(mockLogger);
  }

  @Test
  public void testLoggingCompletion() {
    listener.progressChanged(mockLogger, UploadState.MEDIA_COMPLETE, 104857600L, 60000L);
    verify(mockLogger).debug("Finished Uploading: {}", "NAME");
    verifyNoMoreInteractions(mockLogger);
  }

  @Test
  public void testOtherUpdatesIgnored() {
    listener.progressChanged(mockLogger, UploadState.NOT_STARTED, 0L, 60001L);
    listener.progressChanged(mockLogger, UploadState.INITIATION_COMPLETE, 0L, 60001L);
    verifyZeroInteractions(mockLogger);
  }
}
