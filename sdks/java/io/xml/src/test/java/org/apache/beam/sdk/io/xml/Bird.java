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
package org.apache.beam.sdk.io.xml;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Test JAXB annotated class.
 */
@SuppressWarnings("unused") @XmlRootElement(name = "bird") @XmlType(propOrder = { "name",
  "adjective" }) public final class Bird implements Serializable {
  private String name;
  private String adjective;

  @XmlElement(name = "species")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAdjective() {
    return adjective;
  }

  public void setAdjective(String adjective) {
    this.adjective = adjective;
  }

  public Bird() {}

  public Bird(String adjective, String name) {
    this.adjective = adjective;
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Bird bird = (Bird) o;

    if (!name.equals(bird.name)) {
      return false;
    }
    return adjective.equals(bird.adjective);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + adjective.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return String.format("Bird: %s, %s", name, adjective);
  }
}
