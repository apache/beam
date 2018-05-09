/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.annotation.audience.Audience;

/**
 * Various constants (mostly) related to configuration keys independent of executors.
 */
@Audience(Audience.Type.EXECUTOR)
public class Constants {

  /**
   * Number of items to collect in memory before spilling to disk (e.g. when doing merge sort).
   */
  public static final String SPILL_BUFFER_ITEMS = "euphoria.spill.buffer.items";
  public static final int SPILL_BUFFER_ITEMS_DEFAULT = 10_000;
  public static final String SPILL_BUFFER_ITEMS_DESC =
        "Number of items to be buffered before spilling to disk."
      + "This setting affects performace in the sense that the larger it is "
      + "the more memory will be needed for data processing (mostly sorting), "
      + "but the less it is, the more IO intensive the processing will become.";

  /**
   * Path to local temporary storage.
   */
  public static final String LOCAL_TMP_DIR = "euphoria.spill.tmp.dir";
  public static final String LOCAL_TMP_DIR_DEFAULT = "./";
  public static final String LOCAL_TMP_DIR_DESC =
        "Path to local directory where to spill data. Defaults to current"
      + "working dir.";


  private Constants() {
    // nop
  }

}
