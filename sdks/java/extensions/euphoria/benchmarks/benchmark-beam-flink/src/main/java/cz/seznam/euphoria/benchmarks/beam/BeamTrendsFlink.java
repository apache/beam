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
package cz.seznam.euphoria.benchmarks.beam;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.TestFlinkRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.ArrayUtils;

public class BeamTrendsFlink {

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: Stream <properties-file-path> <exec>");
      System.exit(1);
    }
    
    Parameters params = Parameters.fromFile(args[0]);
    String exec = args[1];
    
    String[] beamArgs = ArrayUtils.subarray(args, 2, args.length);
    PipelineOptions opts = getOpts(exec, beamArgs);

    BeamTrends.main(opts, params, exec);
  }

  private static PipelineOptions getOpts(String exec, String[] args) {
    switch (exec) {
      case "flink":
        PipelineOptions ret1 = PipelineOptionsFactory.fromArgs(args).as(FlinkPipelineOptions.class);
        ret1.setRunner(FlinkRunner.class);
        return ret1;
      case "flink-test":
        PipelineOptions ret2 = PipelineOptionsFactory.fromArgs(args).as(FlinkPipelineOptions.class);
        ret2.setRunner(TestFlinkRunner.class);
        return ret2;
    }
    throw new IllegalArgumentException("Unknown executor: " + exec);
  }
}
