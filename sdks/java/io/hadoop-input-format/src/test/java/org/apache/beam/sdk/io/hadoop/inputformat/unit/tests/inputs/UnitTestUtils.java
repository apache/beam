package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class UnitTestUtils {
  /**
   * Used in {@link NewEmployeeEmployeeInputFormat} and {@link ReuseEmployeeEmpployeeInputFormat} for computing
   * splits.
   */
  public static final long NUMBER_OF_RECORDS_IN_EACH_SPLIT = 5L;
  public static final long NUMBER_OF_SPLITS = 3L;

  /**
   * Returns List of {@link KV} of employee data. Key is employee id and value contains employee
   * name and address separated by _. This is input to {@link NewEmployeeEmployeeInputFormat} and
   * {@link ReuseEmployeeEmpployeeInputFormat}.
   */
  public static List<KV<String, String>> populateEmployeeDataNew() {
    List<KV<String, String>> data = new ArrayList<KV<String, String>>();
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
   * This is helper function used in unit tests for validating data against data read using
   * {@link NewEmployeeEmployeeInputFormat} and {@link ReuseEmployeeEmpployeeInputFormat}.
   */
  public static List<KV<Text, Employee>> getEmployeeData() {

    return Lists.transform(populateEmployeeDataNew(),
        new Function<KV<String, String>, KV<Text, Employee>>() {
          @Override
          public KV<Text, Employee> apply(KV<String, String> input) {
            String[] empData = input.getValue().split("_");
            return KV.of(new Text(input.getKey()), new Employee(empData[0], empData[1]));
          }
        });

  }

}
