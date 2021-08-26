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

package org.apache.beam.learning.katas.coretransforms.sideinput;

import java.io.Serializable;
import java.util.Objects;

public class Person implements Serializable {

  private String name;
  private String city;
  private String country;

  public Person(String name, String city) {
    this.name = name;
    this.city = city;
  }

  public Person(String name, String city, String country) {
    this.name = name;
    this.city = city;
    this.country = country;
  }

  public String getName() {
    return name;
  }

  public String getCity() {
    return city;
  }

  public String getCountry() {
    return country;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Person person = (Person) o;

    return Objects.equals(name, person.name) &&
            Objects.equals(city, person.city) &&
            Objects.equals(country, person.country);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, city, country);
  }

  @Override
  public String toString() {
    return "Person{" +
            "name='" + name + '\'' +
            ", city='" + city + '\'' +
            ", country='" + country + '\'' +
            '}';
  }

}
