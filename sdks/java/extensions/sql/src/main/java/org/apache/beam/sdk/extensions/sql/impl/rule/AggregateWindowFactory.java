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

package org.apache.beam.sdk.extensions.sql.impl.rule;

import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.joda.time.Duration;

/**
 * Creates {@link WindowFn} wrapper based on HOP/TUMBLE/SESSION call in a query.
 */
class AggregateWindowFactory {

  /**
   * Returns optional of {@link AggregateWindowField} which represents a
   * windowing function specified by HOP/TUMBLE/SESSION in the SQL query.
   *
   * <p>If no known windowing function is specified in the query, then {@link Optional#empty()}
   * is returned.
   *
   * <p>Throws {@link UnsupportedOperationException} if it cannot convert SQL windowing function
   * call to Beam model, see {@link #getWindowFieldAt(RexCall, int)} for details.
   */
  static Optional<AggregateWindowField> getWindowFieldAt(RexCall call, int groupField) {

    Optional<WindowFn> windowFnOptional = createWindowFn(call.operands, call.op.kind);

    return
        windowFnOptional
            .map(windowFn ->
                     AggregateWindowField
                         .builder()
                         .setFieldIndex(groupField)
                         .setWindowFn(windowFn)
                         .build());
  }

  /**
   * Returns a {@link WindowFn} based on the SQL windowing function defined by {#code operatorKind}.
   * Supported {@link SqlKind}s:
   * <ul>
   *   <li>{@link SqlKind#TUMBLE}, mapped to {@link FixedWindows};</li>
   *   <li>{@link SqlKind#HOP}, mapped to {@link SlidingWindows};</li>
   *   <li>{@link SqlKind#SESSION}, mapped to {@link Sessions};</li>
   * </ul>
   *
   * <p>For example:
   * <pre>{@code
   *   SELECT event_timestamp, COUNT(*)
   *   FROM PCOLLECTION
   *   GROUP BY TUMBLE(event_timestamp, INTERVAL '1' HOUR)
   * }</pre>
   *
   * <p>SQL window functions support optional window_offset parameter which indicates a
   * how window definition is offset from the event time. Offset is zero if not specified.
   *
   * <p>Beam model does not support offset for session windows, so this method will throw
   * {@link UnsupportedOperationException} if offset is specified
   * in SQL query for {@link SqlKind#SESSION}.
   */
  private static Optional<WindowFn> createWindowFn(List<RexNode> parameters, SqlKind operatorKind) {
    switch (operatorKind) {
      case TUMBLE:

        // Fixed-size, non-intersecting time-based windows, for example:
        //   every hour aggregate elements from the previous hour;
        //
        // SQL Syntax:
        //   TUMBLE(monotonic_field, window_size [, window_offset])
        //
        // Example:
        //   TUMBLE(event_timestamp_field, INTERVAL '1' HOUR)

        FixedWindows fixedWindows = FixedWindows.of(durationParameter(parameters, 1));
        if (parameters.size() == 3) {
          fixedWindows = fixedWindows.withOffset(durationParameter(parameters, 2));
        }

        return Optional.of(fixedWindows);
      case HOP:

        // Sliding, fixed-size, intersecting time-based windows, for example:
        //   every minute aggregate elements from the previous hour;
        //
        // SQL Syntax:
        //   HOP(monotonic_field, emit_frequency, window_size [, window_offset])
        //
        // Example:
        //   HOP(event_timestamp_field, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)

        SlidingWindows slidingWindows = SlidingWindows
            .of(durationParameter(parameters, 2))
            .every(durationParameter(parameters, 1));

        if (parameters.size() == 4) {
          slidingWindows = slidingWindows.withOffset(durationParameter(parameters, 3));
        }

        return Optional.of(slidingWindows);
      case SESSION:

        // Session windows, for example:
        //   aggregate events after a gap of 1 minute of no events;
        //
        // SQL Syntax:
        //   SESSION(monotonic_field, session_gap)
        //
        // Example:
        //   SESSION(event_timestamp_field, INTERVAL '1' MINUTE)

        Sessions sessions = Sessions.withGapDuration(durationParameter(parameters, 1));
        if (parameters.size() == 3) {
          throw new UnsupportedOperationException(
              "Specifying alignment (offset) is not supported for session windows");
        }

        return Optional.of(sessions);
      default:
        return Optional.empty();
    }
  }

  private static Duration durationParameter(List<RexNode> parameters, int parameterIndex) {
    return Duration.millis(intValue(parameters.get(parameterIndex)));
  }

  private static long intValue(RexNode operand) {
    if (operand instanceof RexLiteral) {
      return RexLiteral.intValue(operand);
    } else {
      throw new IllegalArgumentException(String.format("[%s] is not valid.", operand));
    }
  }
}
