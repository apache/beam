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
package org.apache.beam.runners.core.construction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.util.DoFnWithExecutionInformation;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.extism.sdk.Plugin;
import org.extism.sdk.manifest.Manifest;
import org.extism.sdk.wasm.PathWasmSource;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ExtismUtils {

  // static {
  //   // boolean b = true;
  //   // if (b) {
  //   //   throw new RuntimeException("Intentional failure");
  //   // }
  //   // System.load("/usr/local/lib/libextism.so");
  //   // System.loadLibrary("extism");
  // }

  static class WASMDoFnWrapper extends DoFn<byte[], byte[]> {

    private transient Plugin plugin;

    public WASMDoFnWrapper(String wasmFileName) {
      ArrayList<PathWasmSource> paths = new ArrayList<>();
      paths.add(new PathWasmSource("dofn_path", wasmFileName, "dummy_hash"));
      Manifest manifest = new Manifest((ArrayList) paths);
      plugin = new Plugin(manifest, false, null);
    }

    @ProcessElement
    public void processElement(@Element byte[] element, OutputReceiver<byte[]> receiver) {
      byte[] output = plugin.call("processElement", element);
      receiver.output(output);
    }
  }

  public static DoFnWithExecutionInformation createWasmDoFnWrapper(String wasmFileName) {
    return new DoFnWithExecutionInformation() {

      @Override
      public DoFn<?, ?> getDoFn() {
        return new WASMDoFnWrapper(wasmFileName);
      }

      @Override
      public TupleTag<?> getMainOutputTag() {
        return new TupleTag<>("main");
      }

      @Override
      public Map<String, PCollectionView<?>> getSideInputMapping() {
        return Collections.emptyMap();
      }

      @Override
      public DoFnSchemaInformation getSchemaInformation() {
        return DoFnSchemaInformation.create();
      }
    };
  }
}
