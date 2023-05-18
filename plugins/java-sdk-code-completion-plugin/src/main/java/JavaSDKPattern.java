import com.intellij.patterns.PatternCondition;
import com.intellij.psi.PsiElement;
import com.intellij.util.ProcessingContext;
import org.jetbrains.annotations.NotNull;


public class JavaSDKPattern extends PatternCondition<PsiElement> {
    JavaSDKPattern() {
        super("javaSDKPattern()");
    }

    @Override
    public boolean accepts(@NotNull PsiElement psiElement, ProcessingContext context) {
        return false;
    }
}
