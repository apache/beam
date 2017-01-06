package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public class Employee implements Serializable {
  private static final long serialVersionUID = 1L;
  private String empAddress;
  private String empName;

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
}
