package org.apache.beam.io.iceberg;


import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;

@SuppressWarnings("all") //TODO: Remove this once development is stable.
public class PrepareWrite<InputT,DestinationT,OutputT>
    extends PTransform<PCollection<InputT>,PCollection<KV<DestinationT,OutputT>>> {


  private DynamicDestinations<InputT,DestinationT> dynamicDestinations;
  private SerializableFunction<InputT,OutputT> formatFunction;
  private Coder outputCoder;

  public PrepareWrite(
      DynamicDestinations<InputT,DestinationT> dynamicDestinations,
      SerializableFunction<InputT,OutputT> formatFunction,
      Coder outputCoder) {
    this.dynamicDestinations = dynamicDestinations;
    this.formatFunction = formatFunction;
    this.outputCoder = outputCoder;
  }


  @Override
  public PCollection<KV<DestinationT, OutputT>> expand(PCollection<InputT> input) {

    final Coder destCoder;
    try {
      destCoder = KvCoder.of(
          dynamicDestinations.getDestinationCoderWithDefault(input.getPipeline().getCoderRegistry()),
          outputCoder
          );
    } catch(Exception e) {
      RuntimeException e1 = new RuntimeException("Unable to expand PrepareWrite");
      e1.addSuppressed(e);
      throw e1;
    }
    return input.apply(ParDo.of(new DoFn<InputT,KV<DestinationT,OutputT>>() {

      @ProcessElement
      public void processElement(
          ProcessContext c,
          @Element InputT element,
          @Timestamp Instant timestamp,
          BoundedWindow window,
          PaneInfo pane) throws IOException {
        ValueInSingleWindow<InputT> windowedElement =
            ValueInSingleWindow.of(element,timestamp,window,pane);
        dynamicDestinations.setSideInputProcessContext(c);
        DestinationT tableDestination = dynamicDestinations.getDestination(windowedElement);
        OutputT outputValue = formatFunction.apply(element);
        c.output(KV.of(tableDestination,outputValue));
      }
    }).withSideInputs(dynamicDestinations.getSideInputs())).setCoder(destCoder);
  }
}
