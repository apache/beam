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
package org.apache.beam.sdk.util;

import java.lang.reflect.InvocationTargetException;

/** Stores whether we are running within AppEngine or not. */
public class AppEngineEnvironment {
  /**
   * True if running inside of AppEngine, false otherwise.
   */
  @Deprecated
  public static final boolean IS_APP_ENGINE = isAppEngine();

  /**
   * Attempts to detect whether we are inside of AppEngine.
   *
   * <p>Purposely copied and left private from private <a href="https://code.google.com/p/
   * guava-libraries/source/browse/guava/src/com/google/common/util/concurrent/
   * MoreExecutors.java#785">code.google.common.util.concurrent.MoreExecutors#isAppEngine</a>.
   *
   * @return true if we are inside of AppEngine, false otherwise.
   */
  static boolean isAppEngine() {
    if (System.getProperty("com.google.appengine.runtime.environment") == null) {
      return false;
    }
    try {
      // If the current environment is null, we're not inside AppEngine.
      return Class.forName("com.google.apphosting.api.ApiProxy")
          .getMethod("getCurrentEnvironment")
          .invoke(null) != null;
    } catch (ClassNotFoundException e) {
      // If ApiProxy doesn't exist, we're not on AppEngine at all.
      return false;
    } catch (InvocationTargetException e) {
      // If ApiProxy throws an exception, we're not in a proper AppEngine environment.
      return false;
    } catch (IllegalAccessException e) {
      // If the method isn't accessible, we're not on a supported version of AppEngine;
      return false;
    } catch (NoSuchMethodException e) {
      // If the method doesn't exist, we're not on a supported version of AppEngine;
      return false;
    }
  }
}
