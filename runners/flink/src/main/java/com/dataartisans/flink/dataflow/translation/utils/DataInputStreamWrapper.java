package com.dataartisans.flink.dataflow.translation.utils;

import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;
import java.io.InputStream;

public class DataInputStreamWrapper extends InputStream {

	private DataInputView inputView;

	public DataInputStreamWrapper(DataInputView inputView) {
		this.inputView = inputView;
	}

	public void setInputView(DataInputView inputView) {
		this.inputView = inputView;
	}

	@Override
	public int read() throws IOException {
		return inputView.readByte();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return inputView.read(b, off, len);
	}
}
