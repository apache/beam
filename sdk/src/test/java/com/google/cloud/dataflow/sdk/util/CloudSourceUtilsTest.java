/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Map;

/**
 * Tests for {@code CloudSourceUtils}.
 */
@RunWith(JUnit4.class)
public class CloudSourceUtilsTest {
  @Test
  public void testFlattenBaseSpecs() throws Exception {
    // G = grandparent, P = parent, C = child.
    CloudObject grandparent = CloudObject.forClassName("text");
    addString(grandparent, "G", "g_g");
    addString(grandparent, "GP", "gp_g");
    addString(grandparent, "GC", "gc_g");
    addString(grandparent, "GPC", "gpc_g");

    CloudObject parent = CloudObject.forClassName("text");
    addString(parent, "P", "p_p");
    addString(parent, "PC", "pc_p");
    addString(parent, "GP", "gp_p");
    addString(parent, "GPC", "gpc_p");

    CloudObject child = CloudObject.forClassName("text");
    addString(child, "C", "c_c");
    addString(child, "PC", "pc_c");
    addString(child, "GC", "gc_c");
    addString(child, "GPC", "gpc_c");

    Source source = new Source();
    source.setBaseSpecs(new ArrayList<Map<String, Object>>());
    source.getBaseSpecs().add(grandparent);
    source.getBaseSpecs().add(parent);
    source.setSpec(child);
    source.setCodec(makeCloudEncoding(StringUtf8Coder.class.getName()));

    Source flat = CloudSourceUtils.flattenBaseSpecs(source);
    assertNull(flat.getBaseSpecs());
    assertEquals(
        StringUtf8Coder.class.getName(),
        getString(flat.getCodec(), PropertyNames.OBJECT_TYPE_NAME));

    CloudObject flatSpec = CloudObject.fromSpec(flat.getSpec());
    assertEquals("g_g", getString(flatSpec, "G"));
    assertEquals("p_p", getString(flatSpec, "P"));
    assertEquals("c_c", getString(flatSpec, "C"));
    assertEquals("gp_p", getString(flatSpec, "GP"));
    assertEquals("gc_c", getString(flatSpec, "GC"));
    assertEquals("pc_c", getString(flatSpec, "PC"));
    assertEquals("gpc_c", getString(flatSpec, "GPC"));
  }
}
