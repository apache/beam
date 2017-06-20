package org.apache.beam.runners.tez.translation;

import com.google.common.collect.Iterables;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PValue;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.apache.beam.sdk.transforms.ParDo} translation to Tez {@link Vertex}.
 */
class ParDoTranslator<InputT, OutputT> implements TransformTranslator<MultiOutput<InputT, OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(ParDoTranslator.class);
  private static final String OUTPUT_TAG = "OUTPUT_TAG";
  private static final String DO_FN_INSTANCE_TAG = "DO_FN_INSTANCE";

  @Override
  public void translate(MultiOutput<InputT, OutputT> transform, TranslationContext context) {
    //Prepare input/output targets
    if (context.getCurrentInputs().size() > 1){
      throw new NotImplementedException("Multiple Inputs are not yet supported");
    } else if (context.getCurrentOutputs().size() > 1){
      throw new NotImplementedException("Multiple Outputs are not yet supported");
    }
    PValue input = Iterables.getOnlyElement(context.getCurrentInputs().values());
    PValue output = Iterables.getOnlyElement(context.getCurrentOutputs().values());

    //Prepare UserPayload Configuration
    DoFn doFn = transform.getFn();
    String doFnInstance;
    try {
      doFnInstance = TranslatorUtil.toString(doFn);
    } catch ( IOException e){
      throw new RuntimeException("DoFn failed to serialize: " + e.getMessage());
    }
    Configuration config = new Configuration();
    config.set(OUTPUT_TAG, transform.getMainOutputTag().getId());
    config.set(DO_FN_INSTANCE_TAG, doFnInstance);

    //Check for shuffle input
    boolean shuffle = false;
    for (Pair<PValue, PValue> pair : context.getShuffleSet()){
      if (pair.getRight().equals(input)){
        shuffle = true;
      }
    }

    //Create Vertex with Payload
    try {
      UserPayload payload = TezUtils.createUserPayloadFromConf(config);
      Vertex vertex;
      if (shuffle) {
        vertex = Vertex.create(context.getCurrentName(), ProcessorDescriptor.create(TezDoFnProcessor.class.getName()).setUserPayload(payload), 1);
        //TODO: add customizable parallelism
      } else {
        vertex = Vertex.create(context.getCurrentName(), ProcessorDescriptor.create(TezDoFnProcessor.class.getName()).setUserPayload(payload));
      }
      context.addVertex(context.getCurrentName(), vertex, input, output);
    } catch (Exception e){
      throw new RuntimeException("Vertex Translation Failure from: " + e.getMessage());
    }
  }
}