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
package com.azure.cosmos.examples.common;

import java.io.Serializable;

public class Families implements Serializable {

  public static Family getAndersenFamilyItem() {
    Family andersenFamily = new Family();
    andersenFamily.setId("Andersen-" + System.currentTimeMillis());
    andersenFamily.setLastName("Andersen");

    Parent parent1 = new Parent();
    parent1.setFirstName("Thomas");

    Parent parent2 = new Parent();
    parent2.setFirstName("Mary Kay");

    andersenFamily.setParents(new Parent[] {parent1, parent2});

    Child child1 = new Child();
    child1.setFirstName("Henriette Thaulow");
    child1.setGender("female");
    child1.setGrade(5);

    Pet pet1 = new Pet();
    pet1.setGivenName("Fluffy");

    child1.setPets(new Pet[] {pet1});

    andersenFamily.setDistrict("WA5");
    Address address = new Address();
    address.setCity("Seattle");
    address.setCounty("King");
    address.setState("WA");

    andersenFamily.setAddress(address);
    andersenFamily.setRegistered(true);

    return andersenFamily;
  }

  public static Family getWakefieldFamilyItem() {
    Family wakefieldFamily = new Family();
    wakefieldFamily.setId("Wakefield-" + System.currentTimeMillis());
    wakefieldFamily.setLastName("Wakefield");

    Parent parent1 = new Parent();
    parent1.setFamilyName("Wakefield");
    parent1.setFirstName("Robin");

    Parent parent2 = new Parent();
    parent2.setFamilyName("Miller");
    parent2.setFirstName("Ben");

    wakefieldFamily.setParents(new Parent[] {parent1, parent2});

    Child child1 = new Child();
    child1.setFirstName("Jesse");
    child1.setFamilyName("Merriam");
    child1.setGrade(8);

    Pet pet1 = new Pet();
    pet1.setGivenName("Goofy");

    Pet pet2 = new Pet();
    pet2.setGivenName("Shadow");

    child1.setPets(new Pet[] {pet1, pet2});

    Child child2 = new Child();
    child2.setFirstName("Lisa");
    child2.setFamilyName("Miller");
    child2.setGrade(1);
    child2.setGender("female");

    wakefieldFamily.setChildren(new Child[] {child1, child2});

    Address address = new Address();
    address.setCity("NY");
    address.setCounty("Manhattan");
    address.setState("NY");

    wakefieldFamily.setAddress(address);
    wakefieldFamily.setDistrict("NY23");
    wakefieldFamily.setRegistered(true);
    return wakefieldFamily;
  }

  public static Family getJohnsonFamilyItem() {
    Family andersenFamily = new Family();
    andersenFamily.setId("Johnson-" + System.currentTimeMillis());
    andersenFamily.setLastName("Johnson");

    Parent parent1 = new Parent();
    parent1.setFirstName("John");

    Parent parent2 = new Parent();
    parent2.setFirstName("Lili");

    return andersenFamily;
  }

  public static Family getSmithFamilyItem() {
    Family andersenFamily = new Family();
    andersenFamily.setId("Smith-" + System.currentTimeMillis());
    andersenFamily.setLastName("Smith");

    Parent parent1 = new Parent();
    parent1.setFirstName("John");

    Parent parent2 = new Parent();
    parent2.setFirstName("Cynthia");

    return andersenFamily;
  }
}
