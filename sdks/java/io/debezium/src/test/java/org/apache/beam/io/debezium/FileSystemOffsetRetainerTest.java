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
package org.apache.beam.io.debezium;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FileSystemOffsetRetainer}. */
@RunWith(JUnit4.class)
public class FileSystemOffsetRetainerTest {

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
  }

  @Test
  public void testLoadOffsetReturnNullWhenFileIsMissing() {
    String path = tmpFolder.getRoot().getAbsolutePath() + "/nonexistent.json";
    assertNull(FileSystemOffsetRetainer.of(path).loadOffset());
  }

  @Test
  public void testLoadOffsetThrowsWhenFileIsUnreadable() throws Exception {
    String path = tmpFolder.getRoot().getAbsolutePath() + "/offset.json";
    FileSystemOffsetRetainer retainer = FileSystemOffsetRetainer.of(path);
    retainer.saveOffset(ImmutableMap.of("lsn", "100"));

    // Corrupt the file so JSON parsing fails.
    Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8).close(); // truncate to empty
    assertThrows(RuntimeException.class, retainer::loadOffset);
  }

  @Test
  public void testSaveAndLoadOffsetRoundTrip() {
    String path = tmpFolder.getRoot().getAbsolutePath() + "/offset.json";
    FileSystemOffsetRetainer retainer = FileSystemOffsetRetainer.of(path);

    retainer.saveOffset(ImmutableMap.of("file", "binlog.000001", "pos", "156"));

    Map<String, Object> loaded = retainer.loadOffset();
    assertNotNull(loaded);
    assertEquals("binlog.000001", loaded.get("file"));
    assertEquals("156", loaded.get("pos"));
  }

  @Test
  public void testSaveOffsetSkipsWriteWhenOffsetUnchanged() throws Exception {
    String path = tmpFolder.getRoot().getAbsolutePath() + "/offset.json";
    FileSystemOffsetRetainer retainer = FileSystemOffsetRetainer.of(path);
    Map<String, Object> offset = ImmutableMap.of("lsn", "100");

    retainer.saveOffset(offset);
    long modifiedAfterFirstSave = new File(path).lastModified();

    // Second call with the same offset should not touch the file.
    Thread.sleep(10); // ensure mtime would differ if a write occurred
    retainer.saveOffset(offset);
    assertEquals(modifiedAfterFirstSave, new File(path).lastModified());
  }

  @Test
  public void testSaveOffsetLeavesNoTmpFile() {
    String path = tmpFolder.getRoot().getAbsolutePath() + "/offset.json";
    FileSystemOffsetRetainer.of(path).saveOffset(ImmutableMap.of("lsn", "28160840"));

    assertTrue("Final offset file should exist", new File(path).exists());
    assertFalse("Temp file should not remain after rename", new File(path + ".tmp").exists());
  }

  @Test
  public void testSerializedRetainerCanLoadAfterDeserialization() throws Exception {
    String path = tmpFolder.getRoot().getAbsolutePath() + "/offset.json";
    FileSystemOffsetRetainer original = FileSystemOffsetRetainer.of(path);
    original.saveOffset(ImmutableMap.of("lsn", "12345"));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(original);
    }
    FileSystemOffsetRetainer deserialized;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      deserialized = (FileSystemOffsetRetainer) ois.readObject();
    }

    Map<String, Object> loaded = deserialized.loadOffset();
    assertNotNull(loaded);
    assertEquals("12345", loaded.get("lsn"));
  }
}
