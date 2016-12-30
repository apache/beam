package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.coders;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.HadoopInputFormatIOTest;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.HadoopInputFormatIOTest.Employee;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;


public class EmployeeCoder extends StandardCoder<Employee> implements Serializable{
	HadoopInputFormatIOTest hadoopInputFormatIOTest=new HadoopInputFormatIOTest();
	private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

	public EmployeeCoder() {
		StringUtf8Coder.of();
	}

	@Override
	public void encode(Employee value, OutputStream outStream,
			org.apache.beam.sdk.coders.Coder.Context context)
					throws CoderException, IOException {
		Context nested = context.nested();
		stringCoder.encode(value.getEmpID()+"###"+value.getEmpName(), outStream, nested);

	}

	@Override
	public Employee decode(InputStream inStream,
			org.apache.beam.sdk.coders.Coder.Context context)
					throws CoderException, IOException {
		Context nested = context.nested();
		String data[]=stringCoder.decode(inStream, nested).split("###");
		return hadoopInputFormatIOTest.new Employee(data[0],data[1]);
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
	public static EmployeeCoder of() {
		return new EmployeeCoder();
	}

	public <Employee> List<Object> getInstanceComponents(Employee exampleValue){
		return Collections.emptyList();
	}

}
