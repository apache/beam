package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class TestEmployeeDataSet {
  /**
   * Test Utils used in {@link NewObjectsEmployeeInputFormat} and {@link ReuseObjectsEmployeeInputFormat}
   * for computing splits.
   */
  public static final long NUMBER_OF_RECORDS_IN_EACH_SPLIT = 5L;
  public static final long NUMBER_OF_SPLITS = 3L;

  private static final List<KV<String, String>> data = new ArrayList<KV<String, String>>();

  /**
   * Returns List of employee details. Employee details are available in the form of {@link KV} in
   * which, key indicates employee id and value indicates employee details such as name and address
   * separated by '_'. This is data input to {@link NewObjectsEmployeeInputFormat} and
   * {@link ReuseObjectsEmployeeInputFormat}.
   */
  public static List<KV<String, String>> populateEmployeeData() {
    if (!data.isEmpty()) {
      return data;
    }
    data.add(KV.of("0", "Alex_US"));
    data.add(KV.of("1", "John_UK"));
    data.add(KV.of("2", "Tom_UK"));
    data.add(KV.of("3", "Nick_UAE"));
    data.add(KV.of("4", "Smith_IND"));
    data.add(KV.of("5", "Taylor_US"));
    data.add(KV.of("6", "Gray_UK"));
    data.add(KV.of("7", "James_UAE"));
    data.add(KV.of("8", "Jordan_IND"));
    data.add(KV.of("9", "Leena_UK"));
    data.add(KV.of("10", "Zara_UAE"));
    data.add(KV.of("11", "Talia_IND"));
    data.add(KV.of("12", "Rose_UK"));
    data.add(KV.of("13", "Kelvin_UAE"));
    data.add(KV.of("14", "Goerge_IND"));
    return data;
  }

  /**
   * This is a helper function used in unit tests for validating data against data read using
   * {@link NewObjectsEmployeeInputFormat} and {@link ReuseObjectsEmployeeInputFormat}.
   */
  public static List<KV<Text, Employee>> getEmployeeData() {
    return Lists.transform((data.isEmpty() ? populateEmployeeData() : data),
        new Function<KV<String, String>, KV<Text, Employee>>() {
          @Override
          public KV<Text, Employee> apply(KV<String, String> input) {
            String[] empData = input.getValue().split("_");
            return KV.of(new Text(input.getKey()), new Employee(empData[0], empData[1]));
          }
        });
  }

}
