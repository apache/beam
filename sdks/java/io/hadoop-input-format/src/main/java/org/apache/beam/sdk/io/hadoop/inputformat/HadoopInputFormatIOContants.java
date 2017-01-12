package org.apache.beam.sdk.io.hadoop.inputformat;

public class HadoopInputFormatIOContants {
  public static final String NULL_CONFIGURATION_ERROR_MSG = "Configuration cannot be null.";
  public static final String NULL_KEYTRANSLATIONFUNC_ERROR_MSG =
      "Simple function for key translation cannot be null.";
  public static final String NULL_VALUETRANSLATIONFUNC_ERROR_MSG =
      "Simple function for value translation cannot be null.";
  public static final String MISSING_CONFIGURATION_ERROR_MSG =
      "Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().";
  public static final String MISSING_INPUTFORMAT_ERROR_MSG = "Hadoop InputFormat class property "
      + "\"mapreduce.job.inputformat.class\" is not set in configuration.";
  public static final String MISSING_INPUTFORMATKEYCLASS_ERROR_MSG =
      "Configuration property \"key.class\" is not set.";
  public static final String MISSING_INPUTFORMATVALUECLASS_ERROR_MSG =
      "Configuration property \"value.class\" is not set.";
  public static final String WRONG_KEYTRANSLATIONFUNC_ERROR_MSG =
      "Key translation's input type is not same as hadoop input format : %s key class : %s";
  public static final String WRONG_VALUETRANSLATIONFUNC_ERROR_MSG =
      "Value translation's input type is not same as hadoop input format :  %s value class : %s";
  public static final String CANNOT_FIND_CODER_ERROR_MSG = "Cannot find coder for %s  : ";
  public static final String COMPUTESPLITS_NULLSPLITS_ERROR_MSG =
      "Error in computing splits, getSplits() returns null.";
  public static final String COMPUTESPLITS_EMPTYSPLITS_ERROR_MSG =
      "Error in computing splits, getSplits() returns a empty list";
  public static final String CREATEREADER_UNSPLITSOURCE_ERROR_MSG =
      "Cannot create reader as source is not split yet.";
  public static final String CREATEREADER_NULLSPLIT_ERROR_MSG =
      "Cannot read data as split is null.";
  public static final String NULL_CREATE_RECORDREADER_ERROR_MSG =
      "null RecordReader object returned by %s";
  public static final String GETFRACTIONSCONSUMED_ERROR_MSG =
      "Error in computing the fractions consumed as RecordReader.getProgress() throws an exception : ";
  public static final String SERIALIZABLES_PLIT_WRITABLE_ERROR_MSG =
      "Split is not of type Writable: %s";
}
