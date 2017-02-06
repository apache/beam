/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
/**
 * This class is Employee POJO class with properties- employee name and address. 
 * Used in {@linkplain HadoopInputFormatIO} for different unit tests.
 */
@DefaultCoder(AvroCoder.class)
public class Employee {
  private String empAddress;
  private String empName;

  /** 
   * Empty constructor required for Avro decoding. 
   */
  public Employee() {}

  public Employee(String empName, String empAddress) {
    this.empAddress = empAddress;
    this.empName = empName;
  }

  public String getEmpName() {
    return empName;
  }

  public void setEmpName(String empName) {
    this.empName = empName;
  }

  public String getEmpAddress() {
    return empAddress;
  }

  public void setEmpAddress(String empAddress) {
    this.empAddress = empAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Employee employeePojo = (Employee) o;

    if (empName != null 
        ? !empName.equals(employeePojo.empName) 
        : employeePojo.empName != null) {
      return false;
    }
    if (empAddress != null 
        ? !empAddress.equals(employeePojo.empAddress) 
        : employeePojo.empAddress != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "Employee{" + "Name='" + empName + '\'' + ", Address=" + empAddress + '}';
  }
}
