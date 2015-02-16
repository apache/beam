package com.dataartisans.flink.dataflow.translation.utils;

import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.OutputStream;

public class DataOutputStreamWrapper extends OutputStream {
	
	private DataOutputView outputView;

	public DataOutputStreamWrapper(DataOutputView outputView) {
		this.outputView = outputView;
	}

	public void setOutputView(DataOutputView outputView) {
		this.outputView = outputView;
	}

	@Override
	public void write(int b) throws IOException {
		outputView.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		outputView.write(b, off, len);
	}
}
