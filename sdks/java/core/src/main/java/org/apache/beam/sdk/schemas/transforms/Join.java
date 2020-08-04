/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.schemas.transforms;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A transform that performs equijoins across two schema {@link PCollection}s.
 *
 * <p>This transform allows joins between two input PCollections simply by specifying the fields to
 * join on. The resulting {@code PCollection<Row>} will have two fields named "lhs" and "rhs"
 * respectively, each with the schema of the corresponding input PCollection.
 *
 * <p>For example, the following demonstrates joining two PCollections using a natural join on the
 * "user" and "country" fields, where both the left-hand and the right-hand PCollections have fields
 * with these names.
 *
 * <pre>
 * {@code PCollection<Row> joined = pCollection1.apply(Join.innerJoin(pCollection2).using("user", "country"));
 * }</pre>
 *
 * <p>If the right-hand PCollection contains fields with different names to join against, you can
 * specify them as follows:
 *
 * <pre>{@code PCollection<Row> joined = pCollection1.apply(Join.innerJoin(pCollection2)
 *       .on(FieldsEqual.left("user", "country").right("otherUser", "otherCountry")));
 * }</pre>
 *
 * <p>Full outer joins, left outer joins, and right outer joins are also supported.
 */
@Experimental(Kind.SCHEMAS)
public class Join {
  public static final String LHS_TAG = "lhs";
  public static final String RHS_TAG = "rhs";

  /** Predicate object to specify fields to compare when doing an equi-join. */
  public static class FieldsEqual {
    public static Impl left(String... fieldNames) {
      return new Impl(
          FieldAccessDescriptor.withFieldNames(fieldNames), FieldAccessDescriptor.create());
    }

    public static Impl left(Integer... fieldIds) {
      return new Impl(FieldAccessDescriptor.withFieldIds(fieldIds), FieldAccessDescriptor.create());
    }

    public static Impl left(FieldAccessDescriptor fieldAccessDescriptor) {
      return new Impl(fieldAccessDescriptor, FieldAccessDescriptor.create());
    }

    public Impl right(String... fieldNames) {
      return new Impl(
          FieldAccessDescriptor.create(), FieldAccessDescriptor.withFieldNames(fieldNames));
    }

    public Impl right(Integer... fieldIds) {
      return new Impl(FieldAccessDescriptor.create(), FieldAccessDescriptor.withFieldIds(fieldIds));
    }

    public Impl right(FieldAccessDescriptor fieldAccessDescriptor) {
      return new Impl(FieldAccessDescriptor.create(), fieldAccessDescriptor);
    }

    /** Implementation class for FieldsEqual. */
    public static class Impl implements Serializable {
      private FieldAccessDescriptor lhs;
      private FieldAccessDescriptor rhs;

      private Impl(FieldAccessDescriptor lhs, FieldAccessDescriptor rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
      }

      public Impl left(String... fieldNames) {
        return new Impl(FieldAccessDescriptor.withFieldNames(fieldNames), rhs);
      }

      public Impl left(Integer... fieldIds) {
        return new Impl(FieldAccessDescriptor.withFieldIds(fieldIds), rhs);
      }

      public Impl left(FieldAccessDescriptor fieldAccessDescriptor) {
        return new Impl(fieldAccessDescriptor, rhs);
      }

      public Impl right(String... fieldNames) {
        return new Impl(lhs, FieldAccessDescriptor.withFieldNames(fieldNames));
      }

      public Impl right(Integer... fieldIds) {
        return new Impl(lhs, FieldAccessDescriptor.withFieldIds(fieldIds));
      }

      public Impl right(FieldAccessDescriptor fieldAccessDescriptor) {
        return new Impl(lhs, fieldAccessDescriptor);
      }

      private Impl resolve(Schema lhsSchema, Schema rhsSchema) {
        return new Impl(lhs.resolve(lhsSchema), rhs.resolve(rhsSchema));
      }
    }
  }

  /** Perform an inner join. */
  public static <LhsT, RhsT> Impl<LhsT, RhsT> innerJoin(PCollection<RhsT> rhs) {
    return new Impl<>(JoinType.INNER, rhs);
  }

  /** Perform a full outer join. */
  public static <LhsT, RhsT> Impl<LhsT, RhsT> fullOuterJoin(PCollection<RhsT> rhs) {
    return new Impl<>(JoinType.OUTER, rhs);
  }

  /** Perform a left outer join. */
  public static <LhsT, RhsT> Impl<LhsT, RhsT> leftOuterJoin(PCollection<RhsT> rhs) {
    return new Impl<>(JoinType.LEFT_OUTER, rhs);
  }

  /** Perform a right outer join. */
  public static <LhsT, RhsT> Impl<LhsT, RhsT> rightOuterJoin(PCollection<RhsT> rhs) {
    return new Impl<>(JoinType.RIGHT_OUTER, rhs);
  };

  /** Perform an inner join, broadcasting the right side. */
  public static <LhsT, RhsT> Impl<LhsT, RhsT> innerBroadcastJoin(PCollection<RhsT> rhs) {
    return new Impl<>(JoinType.INNER_BROADCAST, rhs);
  }

  /** Perform a left outer join, broadcasting the right side. */
  public static <LhsT, RhsT> Impl<LhsT, RhsT> leftOuterBroadcastJoin(PCollection<RhsT> rhs) {
    return new Impl<>(JoinType.LEFT_OUTER_BROADCAST, rhs);
  }

  private enum JoinType {
    INNER,
    OUTER,
    LEFT_OUTER,
    RIGHT_OUTER,
    INNER_BROADCAST,
    LEFT_OUTER_BROADCAST,
  };

  /** Implementation class . */
  public static class Impl<LhsT, RhsT> extends PTransform<PCollection<LhsT>, PCollection<Row>> {
    private final JoinType joinType;
    private final transient PCollection<RhsT> rhs;
    private final FieldsEqual.@Nullable Impl predicate;

    private Impl(JoinType joinType, PCollection<RhsT> rhs) {
      this(joinType, rhs, null);
    }

    private Impl(JoinType joinType, PCollection<RhsT> rhs, FieldsEqual.Impl predicate) {
      this.joinType = joinType;
      this.rhs = rhs;
      this.predicate = predicate;
    }

    /**
     * Perform a natural join between the PCollections. The fields are expected to exist in both
     * PCollections
     */
    public Impl<LhsT, RhsT> using(String... fieldNames) {
      return new Impl<>(joinType, rhs, FieldsEqual.left(fieldNames).right(fieldNames));
    }

    /**
     * Perform a natural join between the PCollections. The fields are expected to exist in both
     * PCollections
     */
    public Impl<LhsT, RhsT> using(Integer... fieldIds) {
      return new Impl<>(joinType, rhs, FieldsEqual.left(fieldIds).right(fieldIds));
    }

    /**
     * Perform a natural join between the PCollections. The fields are expected to exist in both
     * PCollections
     */
    public Impl<LhsT, RhsT> using(FieldAccessDescriptor fieldAccessDescriptor) {
      return new Impl<>(
          joinType, rhs, FieldsEqual.left(fieldAccessDescriptor).right(fieldAccessDescriptor));
    }

    /** Join the PCollections using the provided predicate. */
    public Impl<LhsT, RhsT> on(FieldsEqual.Impl predicate) {
      return new Impl<>(joinType, rhs, predicate);
    }

    @Override
    public PCollection<Row> expand(PCollection lhs) {
      FieldsEqual.Impl resolvedPredicate = predicate.resolve(lhs.getSchema(), rhs.getSchema());
      PCollectionTuple tuple = PCollectionTuple.of(LHS_TAG, lhs).and(RHS_TAG, rhs);
      switch (joinType) {
        case INNER:
          return tuple.apply(
              CoGroup.join(LHS_TAG, CoGroup.By.fieldAccessDescriptor(resolvedPredicate.lhs))
                  .join(RHS_TAG, CoGroup.By.fieldAccessDescriptor(resolvedPredicate.rhs))
                  .crossProductJoin());
        case INNER_BROADCAST:
          return tuple.apply(
              CoGroup.join(LHS_TAG, CoGroup.By.fieldAccessDescriptor(resolvedPredicate.lhs))
                  .join(
                      RHS_TAG,
                      CoGroup.By.fieldAccessDescriptor(resolvedPredicate.rhs).withSideInput())
                  .crossProductJoin());
        case OUTER:
          return tuple.apply(
              CoGroup.join(
                      LHS_TAG,
                      CoGroup.By.fieldAccessDescriptor(resolvedPredicate.lhs)
                          .withOptionalParticipation())
                  .join(
                      RHS_TAG,
                      CoGroup.By.fieldAccessDescriptor(resolvedPredicate.rhs)
                          .withOptionalParticipation())
                  .crossProductJoin());
        case LEFT_OUTER:
          return tuple.apply(
              CoGroup.join(LHS_TAG, CoGroup.By.fieldAccessDescriptor(resolvedPredicate.lhs))
                  .join(
                      RHS_TAG,
                      CoGroup.By.fieldAccessDescriptor(resolvedPredicate.rhs)
                          .withOptionalParticipation())
                  .crossProductJoin());
        case LEFT_OUTER_BROADCAST:
          return tuple.apply(
              CoGroup.join(LHS_TAG, CoGroup.By.fieldAccessDescriptor(resolvedPredicate.lhs))
                  .join(
                      RHS_TAG,
                      CoGroup.By.fieldAccessDescriptor(resolvedPredicate.rhs)
                          .withOptionalParticipation()
                          .withSideInput())
                  .crossProductJoin());
        case RIGHT_OUTER:
          return tuple.apply(
              CoGroup.join(
                      LHS_TAG,
                      CoGroup.By.fieldAccessDescriptor(resolvedPredicate.lhs)
                          .withOptionalParticipation())
                  .join(RHS_TAG, CoGroup.By.fieldAccessDescriptor(resolvedPredicate.rhs))
                  .crossProductJoin());
        default:
          throw new RuntimeException("Unexpected join type");
      }
    }
  }
}
