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
package org.apache.beam.sdk.io.gcp.healthcare;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WebPathParserTest {

  @Test
  public void test_parsedAllElements() throws IOException {
    String webpathStr =
        "projects/foo/location/earth/datasets/bar/dicomStores/fee/dicomWeb/studies/abc/series/xyz/instances/123";

    WebPathParser parser = new WebPathParser();
    WebPathParser.DicomWebPath dicomWebPath = parser.parseDicomWebpath(webpathStr);

    Assert.assertNotNull(dicomWebPath);
    Assert.assertEquals("foo", dicomWebPath.project);
    Assert.assertEquals("earth", dicomWebPath.location);
    Assert.assertEquals("bar", dicomWebPath.dataset);
    Assert.assertEquals("fee", dicomWebPath.storeId);
    Assert.assertEquals("abc", dicomWebPath.studyId);
    Assert.assertEquals("xyz", dicomWebPath.seriesId);
    Assert.assertEquals("123", dicomWebPath.instanceId);
    Assert.assertEquals(
        "projects/foo/location/earth/datasets/bar/dicomStores/fee", dicomWebPath.dicomStorePath);
  }
}
