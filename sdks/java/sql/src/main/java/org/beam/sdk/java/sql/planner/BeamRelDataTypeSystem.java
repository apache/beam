package org.beam.sdk.java.sql.planner;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;

/**
 * customized data type in Beam.
 *
 */
public class BeamRelDataTypeSystem extends RelDataTypeSystemImpl {
  public static final RelDataTypeSystem BEAM_REL_DATATYPE_SYSTEM = new BeamRelDataTypeSystem();

  @Override
  public int getMaxNumericScale() {
    return 38;
  }

  @Override
  public int getMaxNumericPrecision() {
    return 38;
  }

}
