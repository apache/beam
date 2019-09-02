package org.apache.beam.runners.spark.structuredstreaming.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
/**
 * Test of the wrapping of Beam Coders as Spark ExpressionEncoders.
 */
public class EncodersTest {

  @Test
  public void beamCoderToSparkEncoderTest() {
    SparkSession sparkSession = SparkSession.builder().appName("beamCoderToSparkEncoderTest")
        .master("local[4]").getOrCreate();
    List<Integer> data = new ArrayList<>();
    data.add(1);
    data.add(2);
    data.add(3);
    sparkSession.createDataset(data, EncoderHelpers.fromBeamCoder(VarIntCoder.of()));
//    sparkSession.createDataset(data, EncoderHelpers.genericEncoder());
  }
}
