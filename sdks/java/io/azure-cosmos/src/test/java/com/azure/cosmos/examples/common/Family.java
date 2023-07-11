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
package com.azure.cosmos.examples.common;

import java.io.Serializable;

public class Family implements Serializable {
  public Family() {}

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getDistrict() {
    return district;
  }

  public void setDistrict(String district) {
    this.district = district;
  }

  public Parent[] getParents() {
    return parents;
  }

  public void setParents(Parent[] parents) {
    this.parents = parents;
  }

  public Child[] getChildren() {
    return children;
  }

  public void setChildren(Child[] children) {
    this.children = children;
  }

  public Address getAddress() {
    return address;
  }

  public void setAddress(Address address) {
    this.address = address;
  }

  public boolean isRegistered() {
    return isRegistered;
  }

  public void setRegistered(boolean isRegistered) {
    this.isRegistered = isRegistered;
  }

  private String id = "";
  private String lastName = "";
  private String district = "";
  private Parent[] parents = {};
  private Child[] children = {};
  private Address address = new Address();
  private boolean isRegistered = false;
}
