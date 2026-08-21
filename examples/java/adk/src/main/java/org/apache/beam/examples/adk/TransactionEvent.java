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
package org.apache.beam.examples.adk;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.math.BigDecimal;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class TransactionEvent implements Serializable {
  public abstract String getTransactionId();

  public abstract String getSenderId();

  public abstract String getReceiverId();

  public abstract BigDecimal getAmount();

  @javax.annotation.Nullable
  public abstract String getStatus();

  @javax.annotation.Nullable
  public abstract String getRiskReason();

  public abstract String getTimestamp();

  @javax.annotation.Nullable
  public abstract String getReviewedAt();

  public static TransactionEvent create(
      String transactionId,
      String senderId,
      String receiverId,
      BigDecimal amount,
      @javax.annotation.Nullable String status,
      @javax.annotation.Nullable String riskReason,
      String timestamp,
      @javax.annotation.Nullable String reviewedAt) {
    return new AutoValue_TransactionEvent(
        transactionId, senderId, receiverId, amount, status, riskReason, timestamp, reviewedAt);
  }
}
