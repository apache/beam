package org.apache.beam.runners.tez.translation;

import com.google.common.collect.Iterables;
import java.util.LinkedList;
import org.apache.beam.fn.harness.fake.FakeStepContext;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.tez.translation.io.MROutputManager;
import org.apache.beam.runners.tez.translation.io.NoOpOutputManager;
import org.apache.beam.runners.tez.translation.io.OrderedPartitionedKVOutputManager;
import org.apache.beam.runners.tez.translation.io.OutputManagerFactory;
import org.apache.beam.runners.tez.translation.io.TezOutputManager;
import org.apache.beam.runners.tez.translation.io.UnorderedKVEdgeOutputManager;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

/**
 * TezDoFnProcessor is the Tez Wrapper to wrap user defined functions for Tez processing
 * The DoFn is received through the {@link UserPayload} and then run using the simple {@link DoFnRunner}
 */
public class TezDoFnProcessor extends SimpleProcessor {

  private static final String OUTPUT_TAG = "OUTPUT_TAG";
  private static final String DO_FN_INSTANCE_TAG = "DO_FN_INSTANCE";

  private DoFn<?,?> theDoFn;
  private String outputTag;

  public TezDoFnProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    Configuration config = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    outputTag = config.get(OUTPUT_TAG, null);
    String doFnInstance = config.get(DO_FN_INSTANCE_TAG, null);
    theDoFn = (DoFn) TranslatorUtil.fromString(doFnInstance);
    super.initialize();
  }

  @Override
  public void run() throws Exception {
    //Setup Reader
    KeyValueReader kvReader = null;
    KeyValuesReader kvsReader = null;
    LogicalInput input = Iterables.getOnlyElement(getInputs().values());
    Reader reader = input.getReader();
    if (reader instanceof KeyValueReader) {
      kvReader = (KeyValueReader) reader;
    } else if (reader instanceof KeyValuesReader) {
      kvsReader = (KeyValuesReader) reader;
    } else {
      throw new RuntimeException("UNSUPPORTED READER!");
    }

    //Setup Writer
    TezOutputManager outputManager;
    if (getOutputs().size() == 1){
      LogicalOutput output = Iterables.getOnlyElement(getOutputs().values());
      outputManager = OutputManagerFactory.createOutputManager(output);
      outputManager.before();
    } else if (getOutputs().size() == 0){
      outputManager = new NoOpOutputManager();
    } else {
      throw new RuntimeException("Multiple outputs not yet supported");
    }

    //Initialize DoFnRunner
    DoFnRunner runner = DoFnRunners.simpleRunner(PipelineOptionsFactory.create(), theDoFn, NullSideInputReader
        .empty(), outputManager, new TupleTag<>(outputTag), new LinkedList<>(),
        new FakeStepContext(), WindowingStrategy.globalDefault());
    runner.startBundle();

    //Start Runner
    if (kvsReader != null){
      while (kvsReader.next()){
        outputManager.setCurrentElement(WindowedValue.valueInGlobalWindow(TranslatorUtil.convertToJavaType(kvsReader.getCurrentKey())));
        runner.processElement(WindowedValue.valueInGlobalWindow(KV.of(TranslatorUtil.convertToJavaType(kvsReader.getCurrentKey()),
            TranslatorUtil.convertIteratorToJavaType(kvsReader.getCurrentValues()))));
      }
    } else if (kvReader != null){
      while (kvReader.next()){
        WindowedValue value = WindowedValue.valueInGlobalWindow(TranslatorUtil.convertToJavaType(kvReader.getCurrentKey()));
        outputManager.setCurrentElement(value);
        runner.processElement(WindowedValue.valueInGlobalWindow(TranslatorUtil.convertToJavaType(kvReader.getCurrentValue())));
      }
    } else {
      throw new RuntimeException("UNSUPPORTED READER!");
    }

    outputManager.after();
    runner.finishBundle();
  }

}
