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
package org.apache.beam.sdk.extensions.smb.json;

import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import com.google.api.services.bigquery.model.TableRow;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link JsonFileOperations}. */
public class JsonFileOperationsTest {
  @Rule public final TemporaryFolder output = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    final JsonFileOperations fileOperations = new JsonFileOperations();
    final ResourceId file =
        fromFolder(output).resolve("file.json", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

    final List<TableRow> records =
        IntStream.range(0, 10)
            .mapToObj(i -> new TableRow().set("user", String.format("user%02d", i)).set("age", i))
            .collect(Collectors.toList());
    final FileOperations.Writer<TableRow> writer = fileOperations.createWriter();
    writer.prepareWrite(FileSystems.create(file, writer.getMimeType()));
    for (TableRow record : records) {
      writer.write(record);
    }
    writer.finishWrite();

    final List<TableRow> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }
}
