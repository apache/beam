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
package org.apache.beam.runners.core.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.MetricName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link MonitoringInfoMetricName}. */
public class MonitoringInfoMetricNameTest implements Serializable {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testElementCountConstruction() {
    Map<String, String> labels = new HashMap<>();
    String urn = SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
    MonitoringInfoMetricName name =
        (MonitoringInfoMetricName) MonitoringInfoMetricName.named(urn, labels);
    assertEquals(null, name.getName());
    assertEquals(null, name.getNamespace());
    assertEquals(labels, name.getLabels());
    assertEquals(urn, name.getUrn());

    assertEquals(name, name); // test self equals;

    // Reconstruct and test equality and hash code equivalence
    urn = SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
    labels = new HashMap<>();
    MetricName name2 = MonitoringInfoMetricName.named(urn, labels);

    assertEquals(name, name2);
    assertEquals(name.hashCode(), name2.hashCode());
  }

  @Test
  public void testUserCounterUrnConstruction() {
    String urn = SimpleMonitoringInfoBuilder.userMetricUrn("namespace", "name");
    Map<String, String> labels = new HashMap<>();
    MetricName name = MonitoringInfoMetricName.named(urn, labels);
    assertEquals("name", name.getName());
    assertEquals("namespace", name.getNamespace());

    assertEquals(name, name); // test self equals;

    // Reconstruct and test equality and hash code equivalence
    urn = SimpleMonitoringInfoBuilder.userMetricUrn("namespace", "name");
    labels = new HashMap<>();
    MetricName name2 = MonitoringInfoMetricName.named(urn, labels);

    assertEquals(name, name2);
    assertEquals(name.hashCode(), name2.hashCode());
  }

  @Test
  public void testNotEqualsDiffLabels() {
    Map<String, String> labels = new HashMap<>();
    String urn = SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
    MetricName name = MonitoringInfoMetricName.named(urn, labels);

    // Reconstruct and test equality and hash code equivalence
    urn = SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
    labels = new HashMap<>();
    labels.put("label", "value1");
    MetricName name2 = MonitoringInfoMetricName.named(urn, labels);

    assertNotEquals(name, name2);
    assertNotEquals(name.hashCode(), name2.hashCode());
  }

  @Test
  public void testNotEqualsDiffUrn() {
    Map<String, String> labels = new HashMap<>();
    String urn = SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
    MetricName name = MonitoringInfoMetricName.named(urn, labels);

    // Reconstruct and test equality and hash code equivalence
    urn = "differentUrn";
    labels = new HashMap<>();
    MetricName name2 = MonitoringInfoMetricName.named(urn, labels);

    assertNotEquals(name, name2);
    assertNotEquals(name.hashCode(), name2.hashCode());
  }

  @Test
  public void testNullLabelsThrows() {
    thrown.expect(IllegalArgumentException.class);
    Map<String, String> labels = null;
    MonitoringInfoMetricName.named(SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN, labels);
  }

  @Test
  public void testNullUrnThrows() {
    Map<String, String> labels = new HashMap<>();
    thrown.expect(NullPointerException.class);
    MonitoringInfoMetricName.named(null, labels);
  }

  @Test
  public void testEmptyUrnThrows() {
    Map<String, String> labels = new HashMap<>();
    thrown.expect(IllegalArgumentException.class);
    MonitoringInfoMetricName.named("", labels);
  }
}
