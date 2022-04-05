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
package org.apache.beam.sdk.io.cdap.hubspot.common;

import com.google.gson.JsonElement;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/** Representing page in Hubspot API. */
public class HubspotPage {

  private List<JsonElement> hubspotObjects;
  private SourceHubspotConfig hubspotConfig;
  private String offset;
  private Boolean hasNext;

  /**
   * Constructor for HubspotPage object.
   *
   * @param hubspotObjects the hubspot objects
   * @param hubspotConfig the hubspot config
   * @param offset the offset is string type
   * @param hasNext the hasnext is is boolean type
   */
  public HubspotPage(
      List<JsonElement> hubspotObjects,
      SourceHubspotConfig hubspotConfig,
      String offset,
      Boolean hasNext) {
    this.hubspotObjects = hubspotObjects;
    this.hubspotConfig = hubspotConfig;
    this.offset = offset;
    this.hasNext = hasNext;
  }

  public Iterator<JsonElement> getIterator() {
    return hubspotObjects.iterator();
  }

  public String getOffset() {
    return offset;
  }

  /**
   * Returns the instance of HubspotPage.
   *
   * @return the instance of HubspotPage
   * @throws IOException on issues with data reading
   */
  @Nullable
  public HubspotPage nextPage() throws IOException {
    return (hasNext != null && hasNext)
        ? new HubspotHelper().getHubspotPage(hubspotConfig, offset)
        : null;
  }
}
