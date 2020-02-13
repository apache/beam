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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link MonitoringInfoMetricName}. */
public class MonitoringInfoMetricNameTest implements Serializable {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testElementCountConstruction() {
    HashMap<String, String> labels = new HashMap<String, String>();
    String urn = MonitoringInfoConstants.Urns.ELEMENT_COUNT;
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named(urn, labels);
    assertEquals(labels, name.getLabels());
    assertEquals(urn, name.getUrn());

    assertEquals(name, name); // test self equals;

    // Reconstruct and test equality and hash code equivalence
    urn = MonitoringInfoConstants.Urns.ELEMENT_COUNT;
    labels = new HashMap<String, String>();
    MonitoringInfoMetricName name2 = MonitoringInfoMetricName.named(urn, labels);

    assertEquals(name, name2);
    assertEquals(name.hashCode(), name2.hashCode());
  }

  @Test
  public void testGetNameReturnsNameIfLabelIsPresent() {
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put(MonitoringInfoConstants.Labels.NAME, "anyName");
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named("anyUrn", labels);
    assertEquals("anyName", name.getName());
  }

  @Test
  public void testGetNamespaceReturnsNamespaceIfLabelIsPresent() {
    HashMap<String, String> labels = new HashMap<>();
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "anyTransform");
    labels.put(MonitoringInfoConstants.Labels.NAMESPACE, "anyNamespace");
    labels.put(MonitoringInfoConstants.Labels.PCOLLECTION, "anyPCollection");
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named("anyUrn", labels);
    assertEquals("anyNamespace", name.getNamespace());
  }

  @Test
  public void testGetNamespaceReturnsTransformIfNamespaceLabelIsNotPresent() {
    HashMap<String, String> labels = new HashMap<>();
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "anyTransform");
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named("anyUrn", labels);
    assertEquals("anyTransform", name.getNamespace());
  }

  @Test
  public void testGetNamespaceReturnsPCollectionIfNamespaceLabelIsNotPresent() {
    HashMap<String, String> labels = new HashMap<>();
    labels.put(MonitoringInfoConstants.Labels.PCOLLECTION, "anyPCollection");
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named("anyUrn", labels);
    assertEquals("anyPCollection", name.getNamespace());
  }

  @Test
  public void testNotEqualsDiffLabels() {
    HashMap<String, String> labels = new HashMap<String, String>();
    String urn = MonitoringInfoConstants.Urns.ELEMENT_COUNT;
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named(urn, labels);

    // Reconstruct and test equality and hash code equivalence
    urn = MonitoringInfoConstants.Urns.ELEMENT_COUNT;
    labels = new HashMap<String, String>();
    labels.put("label", "value1");
    MonitoringInfoMetricName name2 = MonitoringInfoMetricName.named(urn, labels);

    assertNotEquals(name, name2);
    assertNotEquals(name.hashCode(), name2.hashCode());
  }

  @Test
  public void testNotEqualsDiffUrn() {
    HashMap<String, String> labels = new HashMap<String, String>();
    String urn = MonitoringInfoConstants.Urns.ELEMENT_COUNT;
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named(urn, labels);

    // Reconstruct and test equality and hash code equivalence
    urn = "differentUrn";
    labels = new HashMap<String, String>();
    MonitoringInfoMetricName name2 = MonitoringInfoMetricName.named(urn, labels);

    assertNotEquals(name, name2);
    assertNotEquals(name.hashCode(), name2.hashCode());
  }

  @Test
  public void testNullLabelsThrows() {
    thrown.expect(IllegalArgumentException.class);
    HashMap<String, String> labels = null;
    MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.ELEMENT_COUNT, labels);
  }

  @Test
  public void testNullUrnThrows() {
    HashMap<String, String> labels = new HashMap<String, String>();
    thrown.expect(IllegalArgumentException.class);
    MonitoringInfoMetricName.named(null, labels);
  }

  @Test
  public void testEmptyUrnThrows() {
    HashMap<String, String> labels = new HashMap<String, String>();
    thrown.expect(IllegalArgumentException.class);
    MonitoringInfoMetricName.named("", labels);
  }
}
