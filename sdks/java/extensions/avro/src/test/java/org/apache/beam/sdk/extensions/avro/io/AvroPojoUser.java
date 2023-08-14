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
package org.apache.beam.sdk.extensions.avro.io;

import java.util.Objects;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;

public class AvroPojoUser {
  private String name;

  @Nullable
  @AvroName("favorite_number")
  private Long favoriteNumber;

  @Nullable
  @AvroName("favorite_color")
  private String favoriteColor;

  public AvroPojoUser() {}

  public AvroPojoUser(String name, Long favoriteNumber, String favoriteColor) {
    this.name = name;
    this.favoriteColor = favoriteColor;
    this.favoriteNumber = favoriteNumber;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Long getFavoriteNumber() {
    return favoriteNumber;
  }

  public void setFavoriteNumber(Long favoriteNumber) {
    this.favoriteNumber = favoriteNumber;
  }

  public String getFavoriteColor() {
    return favoriteColor;
  }

  public void setFavoriteColor(String favoriteColor) {
    this.favoriteColor = favoriteColor;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("favoriteNumber", favoriteNumber)
        .add("favoriteColor", favoriteColor)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof AvroPojoUser) {
      AvroPojoUser that = (AvroPojoUser) o;
      return this.name.equals(that.name)
          && Objects.equals(this.favoriteNumber, that.favoriteNumber)
          && Objects.equals(this.favoriteColor, that.favoriteColor);
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (favoriteNumber != null ? favoriteNumber.hashCode() : 0);
    result = 31 * result + (favoriteColor != null ? favoriteColor.hashCode() : 0);
    return result;
  }
}
