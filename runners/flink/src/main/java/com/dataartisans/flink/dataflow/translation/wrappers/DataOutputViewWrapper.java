package com.dataartisans.flink.dataflow.translation.wrappers;

import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Wrapper for {@link org.apache.flink.core.memory.DataOutputView}. We need this because
 * Flink writes data using a {@link org.apache.flink.core.memory.DataInputView} while
 * Dataflow {@link com.google.cloud.dataflow.sdk.coders.Coder}s expect an
 * {@link java.io.OutputStream}.
 */
public class DataOutputViewWrapper extends OutputStream {
	
	private DataOutputView outputView;

	public DataOutputViewWrapper(DataOutputView outputView) {
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
