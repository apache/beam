package org.apache.beam.sdk.io;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

public class FileSystemUtils {

    /**
     * Expands glob expressions to regular expressions.
     *
     * @param globExp the glob expression to expand
     * @return a string with the regular expression this glob expands to
     */
    @VisibleForTesting
    public static String wildcardToRegexp(String globExp) {
        StringBuilder dst = new StringBuilder();
        char[] src = globExp.replace("**/*", "**").toCharArray();
        int i = 0;
        while (i < src.length) {
            char c = src[i++];
            switch (c) {
                case '*':
                    // One char lookahead for **
                    if (i < src.length && src[i] == '*') {
                        dst.append(".*");
                        ++i;
                    } else {
                        dst.append("[^/]*");
                    }
                    break;
                case '?':
                    dst.append("[^/]");
                    break;
                case '.':
                case '+':
                case '{':
                case '}':
                case '(':
                case ')':
                case '|':
                case '^':
                case '$':
                    // These need to be escaped in regular expressions
                    dst.append('\\').append(c);
                    break;
                case '\\':
                    i = doubleSlashes(dst, src, i);
                    break;
                default:
                    dst.append(c);
                    break;
            }
        }
        return dst.toString();
    }

    private static int doubleSlashes(StringBuilder dst, char[] src, int i) {
        // Emit the next character without special interpretation
        dst.append("\\\\");
        if ((i - 1) != src.length) {
            dst.append(src[i]);
            i++;
        } else {
            // A backslash at the very end is treated like an escaped backslash
            dst.append('\\');
        }
        return i;
    }
}
