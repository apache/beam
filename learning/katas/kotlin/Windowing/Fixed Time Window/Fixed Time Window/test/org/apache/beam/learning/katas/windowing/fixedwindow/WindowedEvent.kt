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
package org.apache.beam.learning.katas.windowing.fixedwindow

import java.io.Serializable
import java.util.*

class WindowedEvent(private val event: String?, private val count: Long, private val window: String) : Serializable {
    override fun equals(o: Any?): Boolean {
        if (this === o) {
            return true
        }

        if (o == null || javaClass != o.javaClass) {
            return false
        }

        val that = o as WindowedEvent

        return event == that.event && count == that.count && window == that.window
    }

    override fun hashCode(): Int {
        return Objects.hash(event, count, window)
    }

    override fun toString(): String {
        return "WindowedEvent{event='$event', count=$count, window='$window'}"
    }

}