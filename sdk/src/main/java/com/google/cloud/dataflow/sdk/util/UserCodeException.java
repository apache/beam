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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * An exception that was thrown in user-code. Sets the stack trace
 * from the first time execution enters user code down through the
 * rest of the user's stack frames until the exception is
 * reached.
 */
public class UserCodeException extends RuntimeException {
  private static final long serialVersionUID = 0;
  private static final Logger LOG = LoggerFactory.getLogger(UserCodeException.class);

  public UserCodeException(Throwable t) {
    super(t);

    StackTraceElement[] currentFrames =
        Thread.currentThread().getStackTrace();

    // We're interested in getting the third stack frame here, since
    // the exception stack trace includes the getStackTrace frame from
    // Thread and the frame from where the UserCodeException is
    // actually thrown. If there aren't more than two frames,
    // something is odd about where the exception was thrown, so leave
    // the stack trace alone and allow it to propagate.
    //
    // For example, if an exception in user code has a stack trace like this:
    //
    // java.lang.NullPointerException
    // at com.google.cloud.dataflow.sdk.examples.
    //     SimpleWordCount$ExtractWordsFn.dieHere(SimpleWordCount.java:23)
    // at com.google.cloud.dataflow.sdk.examples.
    //     SimpleWordCount$ExtractWordsFn.
    //         processElement(SimpleWordCount.java:27)
    // at com.google.cloud.dataflow.sdk.
    //     DoFnRunner.processElement(DoFnRunner.java:95)       <-- caught here
    // at com.google.cloud.dataflow.sdk.
    //     worker.NormalParDoFn.processElement(NormalParDoFn.java:119)
    // at com.google.cloud.dataflow.sdk.
    //     worker.executor.ParDoOperation.process(ParDoOperation.java:65)
    // at com.google.cloud.dataflow.sdk.
    //     worker.executor.ReadOperation.start(ReadOperation.java:65)
    // at com.google.cloud.dataflow.sdk.
    //     worker.executor.MapTaskExecutor.execute(MapTaskExecutor.java:79)
    // at com.google.cloud.dataflow.sdk.
    //     worker.DataflowWorkerHarness.main(DataflowWorkerHarness.java:95)
    //
    // It would be truncated to:
    //
    // java.lang.NullPointerException
    // at com.google.cloud.dataflow.sdk.examples.
    //     SimpleWordCount$ExtractWordsFn.dieHere(SimpleWordCount.java:23)
    // at com.google.cloud.dataflow.sdk.examples.
    //     SimpleWordCount$ExtractWordsFn.
    //         processElement(SimpleWordCount.java:27)
    //
    // However, we need to get the third stack frame from the
    // getStackTrace, since after catching the error in DoFnRunner,
    // the trace is two frames deeper by the time we get it:
    //
    // [0] java.lang.Thread.getStackTrace(Thread.java:1568)
    // [1] com.google.cloud.dataflow.sdk.
    //         UserCodeException.<init>(UserCodeException.java:16)
    // [2] com.google.cloud.dataflow.sdk.
    //         DoFnRunner.processElement(DoFnRunner.java:95)  <-- common frame
    //
    // We then proceed to truncate the original exception at the
    // common frame, setting the UserCodeException's cause to the
    // truncated stack trace.

    // Check to make sure the stack is > 2 deep.
    if (currentFrames.length <= 2) {
      LOG.error("Expecting stack trace to be > 2 frames long.");
      return;
    }

    // Perform some checks to make sure javac doesn't change from below us.
    if (!Objects.equals(currentFrames[1].getClassName(), getClass().getName())) {
      LOG.error("Expected second frame coming from Thread.currentThread.getStackTrace() "
          + "to be {}, was: {}", getClass().getName(), currentFrames[1].getClassName());
      return;
    }
    if (Objects.equals(currentFrames[2].getClassName(), currentFrames[1].getClassName())) {
      LOG.error("Javac's Thread.CurrentThread.getStackTrace() changed unexpectedly.");
      return;
    }

    // Now that all checks have passed, select the common frame.
    StackTraceElement callingFrame = currentFrames[2];
    // Truncate the user-level stack trace below where the
    // UserCodeException was thrown.
    truncateStackTrace(callingFrame, t);
  }

  /**
   * Truncates this Throwable's stack frame at the given frame,
   * removing all frames below.
   */
  private void truncateStackTrace(
      StackTraceElement currentFrame, Throwable t) {
    int index = 0;
    StackTraceElement[] stackTrace = t.getStackTrace();
    for (StackTraceElement element : stackTrace) {
      if (Objects.equals(element.getClassName(), currentFrame.getClassName()) &&
          Objects.equals(element.getMethodName(), currentFrame.getMethodName())) {
        t.setStackTrace(Arrays.copyOfRange(stackTrace, 0, index));
        break;
      }
      index++;
    }
  }
}
