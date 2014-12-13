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

/**
 * Utilities related to Base64 encoding.
 */
public class Base64Utils {
  /**
   * Returns an upper bound of the length of non-chunked Base64 encoded version
   * of the string of the given length.
   */
  public static int getBase64Length(int length) {
    return 4 * ((length + 2) / 3);
  }
}
