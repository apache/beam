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
package org.apache.beam.runners.dataflow.worker.util;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Exposes methods to interop with JFR. This is only supported on java 9 and up, java 8 does not
 * include JFR and will throw an exception when instantiated.
 *
 * <p>Note, since Apache Beam needs to compile against Java 8, we can't directly bind to JFR
 * classes. at compile time. Instead, we need to use reflection to create/invoke JFR methods.
 */
class JfrInterop {
  // ensure only a single JFR profile is running at once
  private static final ExecutorService JFR_EXECUTOR =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("JFRprofile-thread").build());

  private final Constructor<?> recordingCtor;
  private final Method recordingStart;
  private final Method recordingStop;
  private final Method recordingGetStream;
  private final Method recordingClose;
  private final Object jfrConfig;

  @SuppressWarnings("nullness")
  JfrInterop() {
    try {
      Class<?> configClass = Class.forName("jdk.jfr.Configuration");
      Class<?> jfrClass = Class.forName("jdk.jfr.Recording");
      recordingCtor = jfrClass.getConstructor(configClass);
      recordingStart = jfrClass.getMethod("start");
      recordingStop = jfrClass.getMethod("stop");
      recordingGetStream = jfrClass.getMethod("getStream", Instant.class, Instant.class);
      recordingClose = jfrClass.getMethod("close");

      jfrConfig = configClass.getMethod("getConfiguration", String.class).invoke(null, "profile");
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @SuppressWarnings("nullness")
  private InputStream runJfrProfile(Duration duration) {
    try {
      Object recording = recordingCtor.newInstance(jfrConfig);

      recordingStart.invoke(recording);
      Thread.sleep(duration.toMillis());
      recordingStop.invoke(recording);
      InputStream dataStream = (InputStream) recordingGetStream.invoke(recording, null, null);
      recordingClose.invoke(recording);

      return dataStream;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<InputStream> runProfile(Duration duration) {
    return CompletableFuture.supplyAsync(() -> runJfrProfile(duration), JFR_EXECUTOR);
  }
}
