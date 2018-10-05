/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.time;

import java.io.Serializable;
import java.time.Duration;
import java.util.Date;

/**
 * A provider of the "current" timestamp aiming to decouple
 * the "current" time consumers and their corresponding producers.
 */
public interface TimeProvider extends Serializable {

  /**
   * Retrieves the current moment of time (timestamp of "now")
   *
   * @return the "current" timestamp; never {@code null}
   */
  Date now();

  /**
   * Retrieves he current moment of time (timestamp of "now")
   * with given offset added
   *
   * @param offset offset in milliseconds
   * @return the "current" timestamp with offset; never {@code null}
   */
  Date nowOffset(Duration offset);

  /**
   * Retrieves the current moment of time with a precision of a day.
   * This is basically the timestamp of "today's" midnight.
   *
   * @return the "current" date; never {@code null}
   */
  Date today();
}
