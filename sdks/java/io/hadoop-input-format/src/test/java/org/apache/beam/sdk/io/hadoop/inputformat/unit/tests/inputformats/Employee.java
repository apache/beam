package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Employee {
  private String empAddress;
  private String empName;

  // Empty constructor required for Avro decoding.
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
