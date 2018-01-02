/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.executor.io.GenericSpillTools;
import cz.seznam.euphoria.core.executor.io.SerializerFactory;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkSerializerFactory;
import org.apache.flink.api.common.ExecutionConfig;

/**
 * An implementation of {@code SpillTools} for flink environment.
 */
public class FlinkSpillTools extends GenericSpillTools {

  public FlinkSpillTools(
      ExecutionConfig conf,
      Settings settings) {

    super(serializerFactory(conf), settings);
  }

  private static SerializerFactory serializerFactory(ExecutionConfig conf) {
    return new FlinkSerializerFactory(conf);
  }

}
