package org.apache.beam.testinfra.pipelines.conversions;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.testinfra.pipelines.dataflow.DataflowRequestError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowRequestErrorsToString<T extends GeneratedMessageV3>
    extends PTransform<PCollection<DataflowRequestError<T>>, PCollection<DataflowRequestError<String>>> {

  public static <T extends GeneratedMessageV3> DataflowRequestErrorsToString<T> create() {
    return new DataflowRequestErrorsToString<>();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataflowRequestErrorsToString.class);

  @Override
  public PCollection<DataflowRequestError<String>> expand(PCollection<DataflowRequestError<T>> input) {
    return input.apply("DataflowRequestError source to JSON", ParDo.of(new ToStringFn<T>()));
  }

  private static final class ToStringFn<T extends GeneratedMessageV3> extends DoFn<
        DataflowRequestError<T>, DataflowRequestError<String>> {
    @ProcessElement
    public void process(
        @Element DataflowRequestError<T> element,
        OutputReceiver<DataflowRequestError<String>> receiver
    ) {
      try {
        String json = JsonFormat.printer().print(element.getRequest());
        receiver.output(
            DataflowRequestError.<String>builder()
                .setRequest(json)
                .setObservedTime(element.getObservedTime())
                .setMessage(element.getMessage())
                .setStackTrace(element.getStackTrace())
                .build()
        );
      } catch (InvalidProtocolBufferException e) {
        LOG.warn("error converting {} to JSON string: {}", element, e);
      }
    }
  }
}
