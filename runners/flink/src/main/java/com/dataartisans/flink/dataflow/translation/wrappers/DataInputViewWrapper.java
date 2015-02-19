package com.dataartisans.flink.dataflow.translation.wrappers;

import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper for {@link DataInputView}. We need this because Flink reads data using a
 * {@link org.apache.flink.core.memory.DataInputView} while
 * Dataflow {@link com.google.cloud.dataflow.sdk.coders.Coder}s expect an
 * {@link java.io.InputStream}.
 */
public class DataInputViewWrapper extends InputStream {

	private DataInputView inputView;

	public DataInputViewWrapper(DataInputView inputView) {
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
