package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.operator.test.OperatorTest.TestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

class Util {

  static <T> List<T> sorted(Collection<T> xs, Comparator<T> c) {
    ArrayList<T> list = new ArrayList<>(xs);
    list.sort(c);
    return list;
  }
  
  static TestCase bounded(AbstractTestCase testCase) {
    return new AbstractTestCase() {
      @Override
      public int getNumOutputPartitions() {
        return testCase.getNumOutputPartitions();
      }
      @Override
      public void validate(List partitions) {
        testCase.validate(partitions);
      }
      @Override
      protected Dataset getOutput(Dataset input) {
        return testCase.getOutput(input);
      }
      @Override
      protected DataSource getDataSource() {
        return ((ListDataSource)testCase.getDataSource()).toBounded();
      }
    };
  }
}
