package com.cloudera.dataflow.spark;

import org.junit.Assert;
import org.junit.Test;

public class TestSparkPipelineOptionsFactory {
  @Test
  public void testDefaultCreateMethod() {
    SparkPipelineOptions actualOptions = SparkPipelineOptionsFactory.create();
    Assert.assertEquals(actualOptions.getSparkMaster(), "local[1]");
  }

  @Test
  public void testSettingCustomOptions() {
    SparkPipelineOptions actualOptions = SparkPipelineOptionsFactory.create();
    actualOptions.setSparkMaster("spark://207.184.161.138:7077");
    Assert.assertEquals(actualOptions.getSparkMaster(), "spark://207.184.161.138:7077");
  }
}
