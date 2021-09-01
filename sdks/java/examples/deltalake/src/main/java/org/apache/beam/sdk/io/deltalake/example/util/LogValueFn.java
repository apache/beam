package org.apache.beam.sdk.io.deltalake.example.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogValueFn<T> extends DoFn<T, T>
{
    private static final long serialVersionUID = -6563760597373800573L;
    private transient Logger printLogger = null;
    private final SerializableFunction<T, String> formatter;

    public LogValueFn(SerializableFunction<T, String> formatter) {
        this.formatter = formatter;
    }

    @Setup
    public void setup() {
        printLogger = LoggerFactory.getLogger(LogValueFn.class);
    }

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        T value = c.element();
        String msg = formatter.apply(value);
        printLogger.info(msg);
        c.output(value);
    }
}