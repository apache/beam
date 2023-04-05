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
package org.apache.beam.sdk.io;

import static java.nio.channels.Channels.newInputStream;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClassLoaderFileSystemTest {

  private static final String SOME_CLASS =
      "classpath://org/apache/beam/sdk/io/ClassLoaderFileSystem.class";

  @Test
  public void testOpen() throws IOException {
    ClassLoaderFileSystem filesystem = new ClassLoaderFileSystem();
    ReadableByteChannel channel = filesystem.open(filesystem.matchNewResource(SOME_CLASS, false));
    checkIsClass(channel);
  }

  @Test
  public void testRegistrar() throws IOException {
    ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource(SOME_CLASS, false));
    checkIsClass(channel);
  }

  @Test
  public void testResolve() throws IOException {
    ClassLoaderFileSystem filesystem = new ClassLoaderFileSystem();
    ClassLoaderFileSystem.ClassLoaderResourceId original =
        filesystem.matchNewResource(SOME_CLASS, false);
    ClassLoaderFileSystem.ClassLoaderResourceId parent = original.getCurrentDirectory();
    ClassLoaderFileSystem.ClassLoaderResourceId grandparent = parent.getCurrentDirectory();
    assertEquals("classpath://org/apache/beam/sdk", grandparent.getFilename());
    ClassLoaderFileSystem.ClassLoaderResourceId resource =
        grandparent
            .resolve("io", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(
                "ClassLoaderFileSystem.class", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    ReadableByteChannel channel = filesystem.open(resource);
    checkIsClass(channel);
  }

  public void checkIsClass(ReadableByteChannel channel) throws IOException {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    InputStream inputStream = newInputStream(channel);
    byte[] magic = new byte[4];
    inputStream.read(magic);
    assertArrayEquals(magic, new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE});
  }
}
