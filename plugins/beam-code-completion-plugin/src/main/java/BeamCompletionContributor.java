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
import com.intellij.codeInsight.completion.*;

import com.intellij.patterns.PatternCondition;
import com.intellij.patterns.PsiJavaElementPattern;
import com.intellij.patterns.PsiJavaPatterns;
import static com.intellij.patterns.PlatformPatterns.psiElement;

import com.intellij.psi.*;

import com.intellij.codeInsight.lookup.LookupElementBuilder;
import com.intellij.util.ProcessingContext;
import org.jetbrains.annotations.NotNull;

/**
 * The `BeamCompletionContributor` class is a subclass of `CompletionContributor` that provides code completion
 * suggestions specifically for Apache Beam pipelines.
 */
public class BeamCompletionContributor extends CompletionContributor {
    /**
     * A pattern condition that matches method call expressions with the name "apply" in the context of the
     * Apache Beam `Pipeline` class.
     */
    public static final PatternCondition<PsiMethodCallExpression> APPLY_METHOD_PATTERN = new PatternCondition<>("") {
        @Override
        public boolean accepts(@NotNull PsiMethodCallExpression psiMethodCallExpression, ProcessingContext context) {
            String referenceName = psiMethodCallExpression.getMethodExpression().getReferenceName();
            if (referenceName != null){
                if (!referenceName.equals("apply")) {
                    return false;
                }
                PsiMethod resolvedMethod = psiMethodCallExpression.resolveMethod();
                if (resolvedMethod != null){
                    PsiClass containingClass = resolvedMethod.getContainingClass();
                    if (containingClass != null){
                        return "org.apache.beam.sdk.Pipeline".equals(containingClass.getQualifiedName()) ||
                                "org.apache.beam.sdk.values.PCollection".equals(containingClass.getQualifiedName());
                    }
                }
            }
            return false;
        }
    };

    /**
     * A pattern that matches an identifier after a method call expression that satisfies the `APPLY_METHOD_PATTERN`.
     */
    private static final PsiJavaElementPattern.Capture<PsiIdentifier> AFTER_METHOD_CALL_PATTERN =
            PsiJavaPatterns.psiElement(PsiIdentifier.class)
                    .withParent(
                            psiElement(PsiReferenceExpression.class)
                                    .withParent(
                                            psiElement(PsiExpressionList.class)
                                                    .withParent(
                                                            psiElement(PsiMethodCallExpression.class)
                                                                    .with(APPLY_METHOD_PATTERN)
                                                    )
                                    )
                    );

    /**
     * Fills the completion variants for the given completion parameters and result set.
     */
    @Override
    public void fillCompletionVariants(@NotNull final CompletionParameters parameters, @NotNull CompletionResultSet result) {
        final PsiElement position = parameters.getPosition();
        PrefixMatcher matcher = result.getPrefixMatcher();
        PsiElement parent = position.getParent();
        if (AFTER_METHOD_CALL_PATTERN.accepts(parameters.getPosition())){
            result.addElement(LookupElementBuilder.create("TestCompletionElement"));
        }
    }
}
