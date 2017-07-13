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
package org.apache.beam.runners.jstorm.translation.runtime;

import backtype.storm.topology.IComponent;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.jstorm.translation.util.CommonInstance;

/*
 * Enable user to add output stream definitions by API, rather than hard-code.
 */
public abstract class AbstractComponent implements IComponent {
    private Map<String, Fields> streamToFields = new HashMap<>();
    private Map<String, Boolean> keyStreams = new HashMap<>();
    private int parallelismNum = 0;

    public void addOutputField(String streamId) {
        addOutputField(streamId, new Fields(CommonInstance.VALUE));
    }

    public void addOutputField(String streamId, Fields fields) {
        streamToFields.put(streamId, fields);
        keyStreams.put(streamId, false);
    }

    public void addKVOutputField(String streamId) {
        streamToFields.put(streamId, new Fields(CommonInstance.KEY, CommonInstance.VALUE));
        keyStreams.put(streamId, true);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Fields> entry : streamToFields.entrySet()) {
            declarer.declareStream(entry.getKey(), entry.getValue());
        }
    }

    public boolean keyedEmit(String streamId) {
        Boolean isKeyedStream = keyStreams.get(streamId);
        return isKeyedStream == null ? false : isKeyedStream;
    }

    public int getParallelismNum() {
        return parallelismNum;
    }

    public void setParallelismNum(int num) {
        parallelismNum = num;
    }
}