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
package org.apache.beam.learning.katas.windowing.addingtimestamp.pardo

import org.joda.time.DateTime
import java.io.Serializable
import java.util.*

class Event(var id: String, var event: String, var date: DateTime) : Serializable {

    override fun equals(o: Any?): Boolean {
        if (this === o) {
            return true
        }

        if (o == null || javaClass != o.javaClass) {
            return false
        }

        val event1 = o as Event

        return id == event1.id && event == event1.event && date == event1.date
    }

    override fun hashCode(): Int {
        return Objects.hash(id, event, date)
    }

    override fun toString(): String {
        return "Event{id='$id', event='$event', date=$date}"
    }

}