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
import com.intellij.codeInsight.lookup.LookupElement;
import com.intellij.testFramework.fixtures.LightJavaCodeInsightFixtureTestCase;
import org.jetbrains.annotations.NotNull;
import com.intellij.testFramework.LightProjectDescriptor;
import com.intellij.testFramework.fixtures.DefaultLightProjectDescriptor;


// Each test method test a feature as a whole rather than each function or method.
public class BeamCompletionContributorTestCase extends LightJavaCodeInsightFixtureTestCase {
    @Override
    protected String getTestDataPath() {
        return "src/test/testData";
    }

    @Override
    protected @NotNull LightProjectDescriptor getProjectDescriptor() {
        return new DefaultLightProjectDescriptor().withRepositoryLibrary("org.apache.beam:beam-sdks-java-core:2.48.0");
    }

    public void testCompletionsSuccess() throws Throwable {
        myFixture.configureByFile("TestCompletions.java");
        LookupElement[] result = myFixture.completeBasic();
        LookupElement scenarioOutlineLookupElement = null;
        for (LookupElement lookupElement : result) {
            if (lookupElement.getLookupString().equals("Filter")) {
                scenarioOutlineLookupElement = lookupElement;
                break;
            }
        }
        assert scenarioOutlineLookupElement != null;
    }
}
