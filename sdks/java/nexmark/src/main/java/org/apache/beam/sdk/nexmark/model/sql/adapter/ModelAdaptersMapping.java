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

package org.apache.beam.sdk.nexmark.model.sql.adapter;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;

/**
 * Maps Java model classes to Beam SQL record types.
 */
public class ModelAdaptersMapping {

  public static final Map<Class, ModelFieldsAdapter> ADAPTERS =
      ImmutableMap.<Class, ModelFieldsAdapter>builder()
          .put(Auction.class, auctionAdapter())
          .put(Bid.class, bidAdapter())
          .put(Person.class, personAdapter())
          .build();

  private static ModelFieldsAdapter<Person> personAdapter() {
    return new ModelFieldsAdapter<Person>(
        RowSqlType.builder()
            .withBigIntField("id")
            .withVarcharField("name")
            .withVarcharField("emailAddress")
            .withVarcharField("creditCard")
            .withVarcharField("city")
            .withVarcharField("state")
            .withTimestampField("dateTime")
            .withVarcharField("extra")
            .build()) {
      @Override
      public List<Object> getFieldsValues(Person p) {
        return Collections.unmodifiableList(
            Arrays.asList(
                p.id,
                p.name,
                p.emailAddress,
                p.creditCard,
                p.city,
                p.state,
                new Date(p.dateTime),
                p.extra));
      }
    };
  }

  private static ModelFieldsAdapter<Bid> bidAdapter() {
    return new ModelFieldsAdapter<Bid>(
        RowSqlType.builder()
            .withBigIntField("auction")
            .withBigIntField("bidder")
            .withBigIntField("price")
            .withTimestampField("dateTime")
            .withVarcharField("extra")
            .build()) {
      @Override
      public List<Object> getFieldsValues(Bid b) {
        return Collections.unmodifiableList(
            Arrays.asList(
                b.auction,
                b.bidder,
                b.price,
                new Date(b.dateTime),
                b.extra));
      }
    };
  }

  private static ModelFieldsAdapter<Auction> auctionAdapter() {
    return new ModelFieldsAdapter<Auction>(
        RowSqlType.builder()
            .withBigIntField("id")
            .withVarcharField("itemName")
            .withVarcharField("description")
            .withBigIntField("initialBid")
            .withBigIntField("reserve")
            .withTimestampField("dateTime")
            .withTimestampField("expires")
            .withBigIntField("seller")
            .withBigIntField("category")
            .withVarcharField("extra")
            .build()) {
      @Override
      public List<Object> getFieldsValues(Auction a) {
        return Collections.unmodifiableList(
            Arrays.asList(
                a.id,
                a.itemName,
                a.description,
                a.initialBid,
                a.reserve,
                new Date(a.dateTime),
                new Date(a.expires),
                a.seller,
                a.category,
                a.extra));
      }
    };
  }
}
