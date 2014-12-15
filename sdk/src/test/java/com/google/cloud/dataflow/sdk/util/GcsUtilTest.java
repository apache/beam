/*
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
 */

package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.Throwables;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Test case for {@link GcsUtil}. */
@RunWith(JUnit4.class)
public class GcsUtilTest {
  @Test
  public void testGlobTranslation() {
    assertEquals("foo", GcsUtil.globToRegexp("foo"));
    assertEquals("fo[^/]*o", GcsUtil.globToRegexp("fo*o"));
    assertEquals("f[^/]*o\\.[^/]", GcsUtil.globToRegexp("f*o.?"));
    assertEquals("foo-[0-9][^/]*", GcsUtil.globToRegexp("foo-[0-9]*"));
  }

  @Test
  public void testCreationWithDefaultOptions() {
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    pipelineOptions.setGcpCredential(Mockito.mock(Credential.class));
    assertNotNull(pipelineOptions.getGcpCredential());
  }

  @Test
  public void testCreationWithExecutorServiceProvided() {
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    pipelineOptions.setGcpCredential(Mockito.mock(Credential.class));
    pipelineOptions.setExecutorService(Executors.newCachedThreadPool());
    assertSame(pipelineOptions.getExecutorService(), pipelineOptions.getGcsUtil().executorService);
  }

  @Test
  public void testCreationWithGcsUtilProvided() {
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    GcsUtil gcsUtil = Mockito.mock(GcsUtil.class);
    pipelineOptions.setGcsUtil(gcsUtil);
    assertSame(gcsUtil, pipelineOptions.getGcsUtil());
  }

  @Test
  public void testMultipleThreadsCanCompleteOutOfOrderWithDefaultThreadPool() throws Exception {
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    ExecutorService executorService = pipelineOptions.getExecutorService();

    int numThreads = 1000;
    final CountDownLatch[] countDownLatches = new CountDownLatch[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int currentLatch = i;
      countDownLatches[i] = new CountDownLatch(1);
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          // Wait for latch N and then release latch N - 1
          try {
            countDownLatches[currentLatch].await();
            if (currentLatch > 0) {
              countDownLatches[currentLatch - 1].countDown();
            }
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }
        }
      });
    }

    // Release the last latch starting the chain reaction.
    countDownLatches[countDownLatches.length - 1].countDown();
    executorService.shutdown();
    assertTrue("Expected tasks to complete",
        executorService.awaitTermination(10, TimeUnit.SECONDS));
  }
}
