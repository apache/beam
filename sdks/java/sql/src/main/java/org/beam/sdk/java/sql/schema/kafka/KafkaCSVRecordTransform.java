package org.beam.sdk.java.sql.schema.kafka;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class KafkaCSVRecordTransform {
  
  
  public static class ReaderTransform extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<BeamSQLRow>>{
    /**
     * 
     */
    private static final long serialVersionUID = 7613394830984433222L;

    @Override
    public PCollection<BeamSQLRow> expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  public static class SinkerTransform extends PTransform<PCollection<BeamSQLRow>, PCollection<KafkaRecord<byte[], byte[]>>>{

    /**
     * 
     */
    private static final long serialVersionUID = -722396312765710736L;

    @Override
    public PCollection<KafkaRecord<byte[], byte[]>> expand(PCollection<BeamSQLRow> input) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }

}
