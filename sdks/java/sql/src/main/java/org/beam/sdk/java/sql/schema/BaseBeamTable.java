package org.beam.sdk.java.sql.schema;

import java.io.Serializable;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;

/**
 * Each IO in Beam has one table schema, by extending {@link BaseBeamTable}.
 */
public abstract class BaseBeamTable<T> implements ScannableTable, Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = -1262988061830914193L;
  private RelProtoDataType protoRowType;
  // A transform to convert from a rawRecord of input
  private PTransform<PCollection<T>, PCollection<BeamSQLRow>> sourceConverter;
  // A transform to convert one record to a rawRecord for output
  private PTransform<PCollection<BeamSQLRow>, PCollection<T>> sinkConcerter;

  public BaseBeamTable(RelProtoDataType protoRowType,
      PTransform<PCollection<T>, PCollection<BeamSQLRow>> sourceConverter,
      PTransform<PCollection<BeamSQLRow>, PCollection<T>> sinkConcerter) {
    this.protoRowType = protoRowType;
    this.sourceConverter = sourceConverter;
    this.sinkConcerter = sinkConcerter;
  }

  /**
   * In Beam SQL, there's no difference between a batch query and a streaming
   * query. {@link BeamIOType} is used to validate the sources.
   */
  public abstract BeamIOType getSourceType();

  /**
   * create a READ PTransform.
   * 
   * @return
   */
  public abstract PTransform<? super PBegin, PCollection<T>> buildReadTransform();

  /**
   * create a WRITE PTransform
   * 
   * @return
   */
  public abstract PTransform<? super PCollection<T>, PDone> buildWriteTransform();

  public PTransform<PCollection<T>, PCollection<BeamSQLRow>> getSourceConverter() {
    return sourceConverter;
  }

  public PTransform<PCollection<BeamSQLRow>, PCollection<T>> getSinkConcerter() {
    return sinkConcerter;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    // not used as Beam SQL uses its own execution engine
    return null;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  /**
   * Not used {@link Statistic} to optimize the plan
   */
  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  /**
   * all sources are treated as TABLE in Beam SQL.
   */
  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }

}
