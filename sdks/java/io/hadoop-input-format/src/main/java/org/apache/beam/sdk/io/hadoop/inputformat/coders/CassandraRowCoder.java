package org.apache.beam.sdk.io.hadoop.inputformat.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.MyCassandraRow;

public class CassandraRowCoder extends StandardCoder<MyCassandraRow> implements Serializable{

	private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

	public CassandraRowCoder() {
		StringUtf8Coder.of();
	}

	@Override
	public void encode(MyCassandraRow value, OutputStream outStream,
			org.apache.beam.sdk.coders.Coder.Context context)
			throws CoderException, IOException {
		 Context nested = context.nested();
		 stringCoder.encode(value.getSubscriberEmail(), outStream, nested);

	}

	@Override
	public MyCassandraRow decode(InputStream inStream,
			org.apache.beam.sdk.coders.Coder.Context context)
			throws CoderException, IOException {
		 Context nested = context.nested();
		    return new MyCassandraRow(
		        stringCoder.decode(inStream, nested));
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return stringCoder.getCoderArguments();
	}

	@Override
	public void verifyDeterministic()
			throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
		stringCoder.verifyDeterministic();
	}
	public static CassandraRowCoder of() {
	    return new CassandraRowCoder();
	  }

	public <MyCassandraRow> List<Object> getInstanceComponents(MyCassandraRow exampleValue){
		return Collections.emptyList();
	}

}
