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
package org.apache.beam.runners.samza.util;

import static org.apache.beam.runners.samza.util.SamzaPipelineTranslatorUtils.escape;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Test for {@link org.apache.beam.runners.samza.util.SamzaPipelineTranslatorUtils}. */
public class SamzaPipelineTranslatorUtilsTest {

  @Test
  public void testEscape() {
    final String name1 = "Combine.perKey(Count)";
    assertEquals("Combine-perKey-Count", escape(name1));

    final String name2 = "Combine.perKey(Count)/GroupByKey";
    assertEquals("Combine-perKey-Count-GroupByKey", escape(name2));

    final String name3 = "Combine.perKey(Count)/Combine.perKey(Count)";
    assertEquals("Combine-perKey-Count-Combine-perKey-Count", escape(name3));
  }
}
