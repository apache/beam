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
package org.apache.beam.sdk.io.hadoop.inputformat;

/**
 * 
 * All Constants are maintained in this file.
 *
 */
public class HadoopInputFormatIOContants {
	public static final String MISSING_CONFIGURATION_SOURCE_ERROR_MSG = "Configuration of HadoopInputFormatSource missing. Please set the configuration.";
	public static final String MISSING_KEY_CODER_SOURCE_ERROR_MSG = "KeyCoder cannot not be null in HadoopInputFormatSource.";
	public static final String MISSING_VALUE_CODER_SOURCE_ERROR_MSG = "ValueCoder should not be null in HadoopInputFormatSource.";

	public static final String NULL_CONFIGURATION_ERROR_MSG = "Configuration cannot be null.";
	public static final String NULL_KEYTRANSLATIONFUNC_ERROR_MSG = "Simple function for key translation cannot be null.";
	public static final String NULL_VALUETRANSLATIONFUNC_ERROR_MSG = "Simple function for value translation cannot be null.";
	public static final String MISSING_CONFIGURATION_ERROR_MSG = "Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().";
	public static final String MISSING_INPUTFORMAT_ERROR_MSG = "Hadoop InputFormat class property "
			+ "\"mapreduce.job.inputformat.class\" is not set in configuration.";
	public static final String MISSING_INPUTFORMATKEYCLASS_ERROR_MSG = "Configuration property \"key.class\" is not set.";
	public static final String MISSING_INPUTFORMATVALUECLASS_ERROR_MSG = "Configuration property \"value.class\" is not set.";
	public static final String WRONG_KEYTRANSLATIONFUNC_ERROR_MSG = "Key translation's input type is not same as hadoop input format : %s key class : %s";
	public static final String WRONG_VALUETRANSLATIONFUNC_ERROR_MSG = "Value translation's input type is not same as hadoop input format :  %s value class : %s";
	public static final String CANNOT_FIND_CODER_ERROR_MSG = "Cannot find coder for %s  : ";
	public static final String COMPUTESPLITS_NULLSPLITS_ERROR_MSG = "Error in computing splits, getSplits() returns null.";
	public static final String COMPUTESPLITS_EMPTYSPLITS_ERROR_MSG = "Error in computing splits, getSplits() returns a empty list";
	public static final String CREATEREADER_UNSPLITSOURCE_ERROR_MSG = "Cannot create Reader as source is not split yet.";
	public static final String CREATEREADER_NULLSPLIT_ERROR_MSG = "Cannot read data as split is null.";
	public static final String NULL_RECORDREADER_ERROR_MSG = "Null RecordReader object returned by %s";
	public static final String GETFRACTIONSCONSUMED_ERROR_MSG = "Error in computing fractions consumed as RecordReader.getProgress() throws an exception ";
	public static final String SERIALIZABLE_SPLIT_WRITABLE_ERROR_MSG = "Split is not of type Writable: %s";
}
