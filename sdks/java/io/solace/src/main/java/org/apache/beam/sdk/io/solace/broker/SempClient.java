/*
 * Copyright 2024 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.dce.io.solace.broker;

import com.solacesystems.jcsmp.Queue;
import java.io.IOException;
import java.io.Serializable;

public interface SempClient extends Serializable {

    boolean isQueueNonExclusive(String queueName) throws IOException;

    Queue createQueueForTopic(String queueName, String topicName) throws IOException;

    long getBacklogBytes(String queueName) throws IOException;
}
