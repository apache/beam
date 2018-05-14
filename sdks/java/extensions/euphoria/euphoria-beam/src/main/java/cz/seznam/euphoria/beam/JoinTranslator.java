package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class JoinTranslator implements OperatorTranslator<Join> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(Join operator, BeamExecutorContext context) {
    return doTranslate(operator, context);
  }


  public <K, LeftT, RightT, OutputT, W extends Window<W>> PCollection<KV<K, KV<LeftT, RightT>>>
    doTranslate(Join<LeftT, RightT, K, OutputT, W> operator, BeamExecutorContext context) {




    throw new UnsupportedOperationException("Not supported yet");
  }
}
