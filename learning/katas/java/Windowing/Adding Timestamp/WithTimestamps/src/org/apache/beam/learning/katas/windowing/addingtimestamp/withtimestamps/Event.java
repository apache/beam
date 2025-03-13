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

package org.apache.beam.learning.katas.windowing.addingtimestamp.withtimestamps;

import java.io.Serializable;
import java.util.Objects;
import org.joda.time.DateTime;

public class Event implements Serializable {

  private String id;
  private String event;
  private DateTime date;

  public Event(String id, String event, DateTime date) {
    this.id = id;
    this.event = event;
    this.date = date;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }

  public DateTime getDate() {
    return date;
  }

  public void setDate(DateTime date) {
    this.date = date;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Event event1 = (Event) o;

    return id.equals(event1.id) &&
        event.equals(event1.event) &&
        date.equals(event1.date);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, event, date);
  }

  @Override
  public String toString() {
    return "Event{" +
        "id='" + id + '\'' +
        ", event='" + event + '\'' +
        ", date=" + date +
        '}';
  }

}
