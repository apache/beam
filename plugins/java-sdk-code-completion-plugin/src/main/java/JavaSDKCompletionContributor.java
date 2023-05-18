import com.intellij.codeInsight.completion.CompletionContributor;
import com.intellij.codeInsight.completion.CompletionType;
import com.intellij.patterns.PsiJavaPatterns;
import com.intellij.psi.PsiElement;
import org.jetbrains.annotations.NotNull;


public class JavaSDKCompletionContributor extends CompletionContributor {
    public JavaSDKCompletionContributor() {
        // completions for java files
        extend(
                CompletionType.BASIC,
                PsiJavaPatterns.psiElement(),
                new JavaSDKCompletionProvider()
        );
    }
    @Override
    public boolean invokeAutoPopup(@NotNull PsiElement position, char typeChar)
    {
        if (typeChar == '.'){return true;}
        return false;
    }
}
