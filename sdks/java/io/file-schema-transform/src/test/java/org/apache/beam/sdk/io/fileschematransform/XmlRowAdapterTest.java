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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContainingFromRowFn;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TimeContaining;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/** Tests for {@link XmlRowAdapter}. */
@RunWith(JUnit4.class)
public class XmlRowAdapterTest {

  @Test
  public void allPrimitiveDataTypes()
      throws XPathExpressionException, JAXBException, IOException, SAXException,
          ParserConfigurationException {

    for (Row row : DATA.allPrimitiveDataTypesRows) {
      NodeList entries = xmlDocumentEntries(row);
      assertEquals(ALL_PRIMITIVE_DATA_TYPES_SCHEMA.getFieldNames().size(), entries.getLength());
      Map<String, Node> actualMap = keyValues("allPrimitiveDataTypes", entries);
      assertEquals(
          new HashSet<>(ALL_PRIMITIVE_DATA_TYPES_SCHEMA.getFieldNames()), actualMap.keySet());
      for (Entry<String, Node> actualKV : actualMap.entrySet()) {
        String key = actualKV.getKey();
        Node node = actualKV.getValue();
        String actual = node.getTextContent();
        Optional<Object> safeExpected = Optional.ofNullable(row.getValue(key));
        assertTrue(safeExpected.isPresent());
        String expected = safeExpected.get().toString();
        assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void nullableAllPrimitiveDataTypes()
      throws XPathExpressionException, JAXBException, IOException, SAXException,
          ParserConfigurationException {
    for (Row row : DATA.nullableAllPrimitiveDataTypesRows) {
      NodeList entries = xmlDocumentEntries(row);
      assertEquals(
          NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA.getFieldNames().size(), entries.getLength());
      Map<String, Node> actualMap = keyValues("nullableAllPrimitiveDataTypes", entries);
      assertEquals(
          new HashSet<>(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA.getFieldNames()),
          actualMap.keySet());
      for (Entry<String, Node> actualKV : actualMap.entrySet()) {
        String key = actualKV.getKey();
        Node node = actualKV.getValue();
        String actual = node.getTextContent();
        String expected = "";
        Optional<Object> safeExpected = Optional.ofNullable(row.getValue(key));
        if (safeExpected.isPresent()) {
          expected = safeExpected.get().toString();
        }
        assertEquals(expected, actual);
      }
    }
  }

  @Test
  public void timeContaining()
      throws XPathExpressionException, JAXBException, IOException, ParserConfigurationException,
          SAXException {
    String instant = "instant";
    DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
    String instantList = "instantList";
    for (Row row : DATA.timeContainingRows) {
      Optional<TimeContaining> safeExpectedTimeContaining =
          Optional.ofNullable(timeContainingFromRowFn().apply(row));
      assertTrue(safeExpectedTimeContaining.isPresent());
      TimeContaining expectedTimeContaining = safeExpectedTimeContaining.get();
      NodeList entries = xmlDocumentEntries(row);
      assertEquals(TIME_CONTAINING_SCHEMA.getFieldNames().size(), entries.getLength());
      Map<String, Node> actualMap = keyValues("timeContaining", entries);
      assertEquals(new HashSet<>(TIME_CONTAINING_SCHEMA.getFieldNames()), actualMap.keySet());
      Node actualInstantNode = actualMap.get(instant);
      String actual = actualInstantNode.getTextContent();
      String expected = formatter.print(expectedTimeContaining.getInstant().toDateTime());
      assertEquals(expected, actual);

      List<String> actualInstantList =
          toStringList(actualMap.get(instantList).getChildNodes()).stream()
              .sorted()
              .collect(Collectors.toList());
      List<String> expectedInstantList =
          expectedTimeContaining.getInstantList().stream()
              .map(Instant::toDateTime)
              .map(formatter::print)
              .sorted()
              .collect(Collectors.toList());
      assertEquals(expectedInstantList, actualInstantList);
    }
  }

  private static List<String> toStringList(NodeList nodes) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      result.add(node.getTextContent());
    }
    return result;
  }

  private static Map<String, Node> keyValues(String testName, NodeList entries) {
    Map<String, Node> result = new HashMap<>();
    for (int i = 0; i < entries.getLength(); i++) {
      NodeList kv = entries.item(i).getChildNodes();
      assertEquals(testName, 2, kv.getLength());
      String key = kv.item(0).getTextContent();
      Node value = kv.item(1);
      result.put(key, value);
    }
    return result;
  }

  private NodeList xmlDocumentEntries(Row row)
      throws JAXBException, IOException, SAXException, XPathExpressionException,
          ParserConfigurationException {
    JAXBContext context = JAXBContext.newInstance(XmlRowAdapter.class);
    Marshaller marshaller = context.createMarshaller();
    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder = builderFactory.newDocumentBuilder();
    XPath entryPath = XPathFactory.newInstance().newXPath();
    XPathExpression entryPathExpression = entryPath.compile("/row/data/entry");
    XmlRowAdapter adapter = new XmlRowAdapter();
    adapter.wrapRow(row);
    StringWriter writer = new StringWriter();
    marshaller.marshal(adapter, writer);
    String content = writer.toString();
    Document xmlDocument = documentBuilder.parse(new InputSource(new StringReader(content)));
    return (NodeList) entryPathExpression.evaluate(xmlDocument, XPathConstants.NODESET);
  }
}
