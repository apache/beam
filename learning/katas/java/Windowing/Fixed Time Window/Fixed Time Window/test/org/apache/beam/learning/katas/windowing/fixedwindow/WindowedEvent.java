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

package org.apache.beam.learning.katas.windowing.fixedwindow;

import java.io.Serializable;
import java.util.Objects;

public class WindowedEvent implements Serializable {

  private String event;
  private Long count;
  private String window;

  public WindowedEvent(String event, Long count, String window) {
    this.event = event;
    this.count = count;
    this.window = window;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WindowedEvent that = (WindowedEvent) o;
    return event.equals(that.event) &&
        count.equals(that.count) &&
        window.equals(that.window);
  }

  @Override
  public int hashCode() {
    return Objects.hash(event, count, window);
  }

  @Override
  public String toString() {
    return "WindowedEvent{" +
        "event='" + event + '\'' +
        ", count=" + count +
        ", window='" + window + '\'' +
        '}';
  }

}
