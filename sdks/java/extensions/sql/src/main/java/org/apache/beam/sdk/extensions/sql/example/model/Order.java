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
package org.apache.beam.sdk.extensions.sql.example.model;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Describes an order. */
@DefaultSchema(JavaBeanSchema.class)
public class Order implements Serializable {
  private int id;
  private int customerId;

  public Order(int id, int customerId) {
    this.id = id;
    this.customerId = customerId;
  }

  public Order() {}

  public int getId() {
    return id;
  }

  public int getCustomerId() {
    return customerId;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setCustomerId(int customerId) {
    this.customerId = customerId;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Order order = (Order) o;
    return id == order.id && customerId == order.customerId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, customerId);
  }

  @Override
  public String toString() {
    return "Order{" + "id=" + id + ", customerId=" + customerId + '}';
  }
}
