package org.apache.beam.runners.tez.translation.io;

import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * Abstract Output Manager that adds before and after methods to the {@link DoFnRunners.OutputManager}
 * interface so that outputs that require them can be added and used with the TezRunner.
 */
public abstract class TezOutputManager implements DoFnRunners.OutputManager {

  private WindowedValue currentElement;
  private KeyValueWriter writer;
  private LogicalOutput output;

  public TezOutputManager(LogicalOutput output){
    this.output = output;
  }

  public void before() {}

  public void after() {}

  public void setCurrentElement(WindowedValue currentElement) {
    this.currentElement = currentElement;
  }

  public WindowedValue getCurrentElement(){
    return currentElement;
  }

  public void setWriter(KeyValueWriter writer) {
    this.writer = writer;
  }

  public KeyValueWriter getWriter() {
    return writer;
  }

  public LogicalOutput getOutput() {
    return output;
  }
}
