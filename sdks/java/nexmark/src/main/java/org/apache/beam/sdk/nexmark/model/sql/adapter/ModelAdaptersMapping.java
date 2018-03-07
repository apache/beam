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
import org.apache.beam.sdk.nexmark.model.AuctionCount;
import org.apache.beam.sdk.nexmark.model.AuctionPrice;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.NameCityStateId;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.values.Row;

/**
 * Maps Java model classes to Beam SQL record types.
 */
public class ModelAdaptersMapping {

  public static final Map<Class, ModelFieldsAdapter> ADAPTERS =
      ImmutableMap.<Class, ModelFieldsAdapter>builder()
          .put(Auction.class, auctionAdapter())
          .put(Bid.class, bidAdapter())
          .put(Person.class, personAdapter())
          .put(AuctionCount.class, auctionCountAdapter())
          .put(AuctionPrice.class, auctionPriceAdapter())
          .put(NameCityStateId.class, nameCityStateIdAdapter())
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
      @Override
      public Person getRowModel(Row row) {
        return new Person(
           row.getLong("id"),
           row.getString("name"),
           row.getString("emailAddress"),
           row.getString("creditCard"),
           row.getString("city"),
           row.getString("state"),
           row.getDate("dateTime").getTime(),
           row.getString("extra"));
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
      @Override
      public Bid getRowModel(Row row) {
        return new Bid(
            row.getLong("auction"),
            row.getLong("bidder"),
            row.getLong("price"),
            row.getDate("dateTime").getTime(),
            row.getString("extra"));
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
      @Override
      public Auction getRowModel(Row row) {
        return new Auction(
            row.getLong("id"),
            row.getString("itemName"),
            row.getString("description"),
            row.getLong("initialBid"),
            row.getLong("reserve"),
            row.getDate("dateTime").getTime(),
            row.getDate("expires").getTime(),
            row.getLong("seller"),
            row.getLong("category"),
            row.getString("extra"));
      }
    };
  }

  private static ModelFieldsAdapter<AuctionCount> auctionCountAdapter() {
    return new ModelFieldsAdapter<AuctionCount>(
        RowSqlType.builder()
            .withBigIntField("auction")
            .withBigIntField("num")
            .build()) {
      @Override
      public List<Object> getFieldsValues(AuctionCount a) {
        return Collections.unmodifiableList(
            Arrays.asList(
                a.auction,
                a.num));
      }
      @Override
      public AuctionCount getRowModel(Row row) {
        return new AuctionCount(
            row.getLong("auction"),
            row.getLong("num"));
      }
    };
  }

  private static ModelFieldsAdapter<AuctionPrice> auctionPriceAdapter() {
    return new ModelFieldsAdapter<AuctionPrice>(
        RowSqlType.builder()
            .withBigIntField("auction")
            .withBigIntField("price")
            .build()) {
      @Override
      public List<Object> getFieldsValues(AuctionPrice a) {
        return Collections.unmodifiableList(
            Arrays.asList(
                a.auction,
                a.price));
      }
      @Override
      public AuctionPrice getRowModel(Row row) {
        return new AuctionPrice(
            row.getLong("auction"),
            row.getLong("price"));
      }
    };
  }

  private static ModelFieldsAdapter<NameCityStateId> nameCityStateIdAdapter() {
    return new ModelFieldsAdapter<NameCityStateId>(
        RowSqlType.builder()
            .withVarcharField("name")
            .withVarcharField("city")
            .withVarcharField("state")
            .withBigIntField("id")
            .build()) {
      @Override
      public List<Object> getFieldsValues(NameCityStateId a) {
        return Collections.unmodifiableList(
            Arrays.asList(
                a.name,
                a.city,
                a.state,
                a.id));
      }
      @Override
      public NameCityStateId getRowModel(Row row) {
        return new NameCityStateId(
            row.getString("name"),
            row.getString("city"),
            row.getString("state"),
            row.getLong("id"));
      }
    };
  }
}
