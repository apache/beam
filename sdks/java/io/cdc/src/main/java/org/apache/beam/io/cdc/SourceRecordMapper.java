package org.apache.beam.io.cdc;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

@FunctionalInterface
public interface SourceRecordMapper<T> extends Serializable {
    T mapSourceRecord(SourceRecord sourceRecord) throws Exception;
}
