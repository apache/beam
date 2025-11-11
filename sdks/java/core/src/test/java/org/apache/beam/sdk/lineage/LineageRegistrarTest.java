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
package org.apache.beam.sdk.lineage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ServiceLoader;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LineageRegistrar} ServiceLoader discovery. */
@RunWith(JUnit4.class)
public class LineageRegistrarTest {

  @Test
  public void testServiceLoaderDiscovery() {
    // Load all LineageRegistrar implementations via ServiceLoader
    for (LineageRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(LineageRegistrar.class).iterator())) {

      // Check if we found the TestLineageRegistrar
      if (registrar instanceof TestLineageRegistrar) {

        // Test with SOURCE direction
        Lineage sourceLineage =
            registrar.fromOptions(PipelineOptionsFactory.create(), Lineage.LineageDirection.SOURCE);
        assertThat(sourceLineage, notNullValue());
        assertThat(sourceLineage, instanceOf(TestLineage.class));
        assertEquals(Lineage.LineageDirection.SOURCE, ((TestLineage) sourceLineage).getDirection());

        // Test with SINK direction
        Lineage sinkLineage =
            registrar.fromOptions(PipelineOptionsFactory.create(), Lineage.LineageDirection.SINK);
        assertThat(sinkLineage, notNullValue());
        assertThat(sinkLineage, instanceOf(TestLineage.class));
        assertEquals(Lineage.LineageDirection.SINK, ((TestLineage) sinkLineage).getDirection());

        return;
      }
    }

    fail("Expected to find " + TestLineageRegistrar.class);
  }
}
