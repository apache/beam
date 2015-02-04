/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.runners.worker.StreamingDataflowWorker;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class responsible for fetching state from the windmill server.
 */
public class StateFetcher {
  private WindmillServerStub server;

  public StateFetcher(WindmillServerStub server) {
    this.server = server;
  }

  public Map<CodedTupleTag<?>, Object> fetch(
      String computation, ByteString key, long workToken, String prefix,
      List<? extends CodedTupleTag<?>> tags) throws CoderException, IOException {
    Map<CodedTupleTag<?>, Object> resultMap = new HashMap<>();
    if (tags.isEmpty()) {
      return resultMap;
    }

    Windmill.KeyedGetDataRequest.Builder requestBuilder = Windmill.KeyedGetDataRequest.newBuilder()
        .setKey(key)
        .setWorkToken(workToken);

    Map<ByteString, CodedTupleTag<?>> tagMap = new HashMap<>();
    for (CodedTupleTag<?> tag : tags) {
      ByteString tagString = ByteString.copyFromUtf8(prefix + tag.getId());
      requestBuilder.addValuesToFetch(
          Windmill.TagValue.newBuilder()
          .setTag(tagString)
          .build());
      tagMap.put(tagString, tag);
    }

    Windmill.GetDataResponse response = server.getData(
        Windmill.GetDataRequest.newBuilder()
        .addRequests(
            Windmill.ComputationGetDataRequest.newBuilder()
            .setComputationId(computation)
            .addRequests(requestBuilder.build())
            .build())
        .build());

    if (response.getDataCount() != 1
        || !response.getData(0).getComputationId().equals(computation)
        || response.getData(0).getDataCount() != 1
        || !response.getData(0).getData(0).getKey().equals(key)) {
      throw new IOException("Invalid data response, expected single computation and key");
    }
    Windmill.KeyedGetDataResponse keyResponse = response.getData(0).getData(0);
    if (keyResponse.getFailed()) {
      throw new StreamingDataflowWorker.KeyTokenInvalidException(key.toStringUtf8());
    }

    for (Windmill.TagValue tv : keyResponse.getValuesList()) {
      CodedTupleTag<?> tag = tagMap.get(tv.getTag());
      if (tag != null) {
        if (tv.getValue().hasData() && !tv.getValue().getData().isEmpty()) {
          resultMap.put(tag, tag.getCoder().decode(tv.getValue().getData().newInput(),
                  Coder.Context.OUTER));
        } else {
          resultMap.put(tag, null);
        }
      }
    }

    return resultMap;
  }

  public <T> List<T> fetchList(
      String computation, ByteString key, long workToken, String prefix, CodedTupleTag<T> tag)
      throws IOException {

    ByteString tagString = ByteString.copyFromUtf8(prefix + tag.getId());
    Windmill.GetDataRequest request = Windmill.GetDataRequest.newBuilder()
        .addRequests(
            Windmill.ComputationGetDataRequest.newBuilder()
            .setComputationId(computation)
            .addRequests(
                Windmill.KeyedGetDataRequest.newBuilder()
                .setKey(key)
                .setWorkToken(workToken)
                .addListsToFetch(
                    Windmill.TagList.newBuilder()
                    .setTag(tagString)
                    .setEndTimestamp(Long.MAX_VALUE)
                    .build())
                .build())
            .build())
        .build();

    Windmill.GetDataResponse response = server.getData(request);

    if (response.getDataCount() != 1
        || !response.getData(0).getComputationId().equals(computation)
        || response.getData(0).getDataCount() != 1
        || !response.getData(0).getData(0).getKey().equals(key)) {
      throw new IOException("Invalid data response, expected single computation and key\n");
    }

    Windmill.KeyedGetDataResponse keyResponse = response.getData(0).getData(0);
    if (keyResponse.getFailed()) {
      throw new StreamingDataflowWorker.KeyTokenInvalidException(key.toStringUtf8());
    }
    if (keyResponse.getListsCount() != 1
        || !keyResponse.getLists(0).getTag().equals(tagString)) {
      throw new IOException("Expected single list for tag " + tagString);
    }
    Windmill.TagList tagList = keyResponse.getLists(0);
    List<T> result = new ArrayList<>();
    for (Windmill.Value value : tagList.getValuesList()) {
      result.add(tag.getCoder().decode(value.getData().newInput(), Coder.Context.OUTER));
    }

    return result;
  }
}
