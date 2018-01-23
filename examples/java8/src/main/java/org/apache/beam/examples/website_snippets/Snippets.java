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
package org.apache.beam.examples;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Code snippets used in webdocs.
 */
public class Snippets {

  /* Helper function to format results in coGroupByKeyTuple */
  public static String formatCoGbkResults(
      String name, Iterable<String> emails, Iterable<String> phones) {

    List<String> emailsList = new ArrayList<>();
    for (String elem : emails) {
      emailsList.add("'" + elem + "'");
    }
    Collections.sort(emailsList);
    String emailsStr = "[" + String.join(", ", emailsList) + "]";

    List<String> phonesList = new ArrayList<>();
    for (String elem : phones) {
      phonesList.add("'" + elem + "'");
    }
    Collections.sort(phonesList);
    String phonesStr = "[" + String.join(", ", phonesList) + "]";

    return name + "; " + emailsStr + "; " + phonesStr;
  }

  public static PCollection<String> coGroupByKeyTuple(
      TupleTag<String> emailsTag,
      TupleTag<String> phonesTag,
      PCollection<KV<String, String>> emails,
      PCollection<KV<String, String>> phones) {

    // [START CoGroupByKeyTuple]
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(emailsTag, emails)
            .and(phonesTag, phones)
            .apply(CoGroupByKey.create());

    PCollection<String> contactLines = results.apply(ParDo.of(
      new DoFn<KV<String, CoGbkResult>, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<String, CoGbkResult> e = c.element();
          String name = e.getKey();
          Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
          Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
          String formattedResult = Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
          c.output(formattedResult);
        }
      }
    ));
    // [END CoGroupByKeyTuple]
    return contactLines;
  }
}
