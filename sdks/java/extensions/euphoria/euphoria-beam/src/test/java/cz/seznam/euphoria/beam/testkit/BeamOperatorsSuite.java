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
package cz.seznam.euphoria.beam.testkit;

import cz.seznam.euphoria.operator.test.FlatMapTest;
import cz.seznam.euphoria.operator.test.ReduceByKeyTest;
import cz.seznam.euphoria.operator.test.UnionTest;
import cz.seznam.euphoria.operator.test.junit.ExecutorProvider;
import cz.seznam.euphoria.operator.test.junit.ExecutorProviderRunner;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This is a copy of {@link cz.seznam.euphoria.operator.test.AllOperatorsSuite} to allow us
 * track progress on incrementally implementing operator and their tests.
 * TODO: When done, this class should go away and original should be used instead
 */
@RunWith(ExecutorProviderRunner.class)
@Suite.SuiteClasses({
//    CountByKeyTest.class,
//    DistinctTest.class,
//    FilterTest.class,
    FlatMapTest.class,
//    JoinTest.class,
//    JoinWindowEnforcementTest.class,
//    MapElementsTest.class,
//    ReduceByKeyTest.class,
//    ReduceStateByKeyTest.class,
//    SumByKeyTest.class,
//    TopPerKeyTest.class,
//    SortTest.class,
    UnionTest.class,
//    WindowingTest.class,
//    WatermarkTest.class,
})
public abstract class BeamOperatorsSuite implements ExecutorProvider {

}
