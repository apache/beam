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
package org.apache.beam.examples.complete.cdap.hubspot.utils;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.gson.Gson;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSource;
import java.util.HashMap;
import org.apache.beam.sdk.io.cdap.Plugin;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for getting a {@link SerializableFunction} that defines how to get record offset for
 * different CDAP {@link Plugin} classes.
 */
public class GetOffsetUtils {

  private static final Logger LOG = LoggerFactory.getLogger(GetOffsetUtils.class);
  private static final Gson GSON = new Gson();

  private static final String HUBSPOT_ID_FIELD = "vid";

  /**
   * Function for getting offset for given streaming Cdap {@link
   * io.cdap.cdap.api.annotation.Plugin}.
   */
  public static SerializableFunction<String, Long> getOffsetFnForCdapPlugin(Class<?> pluginClass) {
    if (HubspotStreamingSource.class.equals(pluginClass)) {
      return getOffsetFnForHubspot();
    }
    throw new UnsupportedOperationException(
        String.format("Given plugin class '%s' is not supported!", pluginClass.getName()));
  }

  /**
   * Function for getting offset for Hubspot record that has {@link #HUBSPOT_ID_FIELD} number field.
   */
  private static SerializableFunction<String, Long> getOffsetFnForHubspot() {
    return input -> {
      if (input != null) {
        try {
          HashMap<String, Object> json =
              GSON.fromJson(input, new TypeToken<HashMap<String, Object>>() {}.getType());
          checkArgumentNotNull(json, "Can not get JSON from Hubspot input string");
          Object id = json.get(HUBSPOT_ID_FIELD);
          checkArgumentNotNull(id, "Can not get ID from Hubspot input string");
          return ((Double) id).longValue();
        } catch (Exception e) {
          LOG.error("Can not get offset from json", e);
        }
      }
      return 0L;
    };
  }
}
