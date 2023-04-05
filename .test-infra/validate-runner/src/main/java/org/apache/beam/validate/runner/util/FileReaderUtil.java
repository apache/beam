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
package org.apache.beam.validate.runner.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.beam.validate.runner.model.Configuration;

import java.io.InputStream;

/**
 * Reads the input configurations from the configuration.yaml file
 * and passes as a {@link Configuration} object for processing.
 */
public class FileReaderUtil {

    private static final String FILE_PATH = "/configuration.yaml";

    public static Configuration readConfiguration() {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            InputStream inputStream = FileReaderUtil.class.getResourceAsStream(FILE_PATH);
            return mapper.readValue(inputStream, Configuration.class);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
