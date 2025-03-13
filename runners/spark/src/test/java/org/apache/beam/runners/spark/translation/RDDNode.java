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
package org.apache.beam.runners.spark.translation;

class RDDNode {
  private final int id;
  private final String name;
  private final String operator;
  private final String location;

  public RDDNode(int id, String name, String operator, String location) {
    this.id = id;
    this.name = name;
    this.operator = operator;
    this.location = location;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getOperator() {
    return operator;
  }

  public String getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return "RDDNode{"
        + "id="
        + id
        + ", name='"
        + name
        + '\''
        + ", operator='"
        + operator
        + '\''
        + ", location='"
        + location
        + '\''
        + '}';
  }
}
