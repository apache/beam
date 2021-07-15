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
package org.apache.beam.validate.runner.model;

import java.util.List;
import java.util.Map;

public class Configuration {
    private List<Map<String, String>> batch;
    private List<Map<String, String>> stream;
    private String server;
    private String jsonapi;

    public List<Map<String, String>> getBatch() {
        return batch;
    }

    public void setBatch(List<Map<String, String>> batch) {
        this.batch = batch;
    }

    public List<Map<String, String>> getStream() {
        return stream;
    }

    public void setStream(List<Map<String, String>> stream) {
        this.stream = stream;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getJsonapi() {
        return jsonapi;
    }

    public void setJsonapi(String jsonapi) {
        this.jsonapi = jsonapi;
    }
}
