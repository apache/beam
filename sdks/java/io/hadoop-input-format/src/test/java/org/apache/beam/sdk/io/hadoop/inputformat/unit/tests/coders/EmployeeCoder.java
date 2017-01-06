package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.coders;


import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.Employee;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;


public class EmployeeCoder extends StandardCoder<Employee> implements Serializable{
	private static final long serialVersionUID = 1L;
	private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

	public EmployeeCoder() {
		StringUtf8Coder.of();
	}

	@Override
	public void encode(Employee value, OutputStream outStream,
			org.apache.beam.sdk.coders.Coder.Context context)
					throws CoderException, IOException {
		Context nested = context.nested();
		stringCoder.encode(value.getEmpName()+"###"+value.getEmpAddress(), outStream, nested);

	}

	@Override
	public Employee decode(InputStream inStream,
			org.apache.beam.sdk.coders.Coder.Context context)
					throws CoderException, IOException {
		Context nested = context.nested();
		String data[]=stringCoder.decode(inStream, nested).split("###");
		return new Employee(data[0],data[1]);
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

	public  List<Employee> getInstanceComponents(Employee exampleValue){
		return Collections.emptyList();
	}

}
