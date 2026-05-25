import globals from "globals";
import typescriptParser from "@typescript-eslint/parser";
import typescriptPlugin from "@typescript-eslint/eslint-plugin";

export default {

    plugins: {
        "@typescript-eslint": typescriptPlugin,
    },

    languageOptions: {
        globals: {
            ...globals.node,
        },

        ecmaVersion: 9,
        sourceType: "module",

        parser: typescriptParser,
        parserOptions: {
            project: "./tsconfig.json",
        },
    },

    files: ['**/*.ts', '**/*.tsx'],

    rules: {
        camelcase: "off",
        semi: "off",

        "@typescript-eslint/explicit-member-accessibility": ["error", {
            accessibility: "no-public",
        }],

        "@typescript-eslint/no-require-imports": "error",
        "@typescript-eslint/array-type": "error",
        "@typescript-eslint/await-thenable": "error",

        "@typescript-eslint/explicit-function-return-type": ["error", {
            allowExpressions: true,
        }],

        "@typescript-eslint/no-array-constructor": "error",
        "@typescript-eslint/no-empty-interface": "error",
        "@typescript-eslint/no-explicit-any": "error",
        "@typescript-eslint/no-extraneous-class": "error",
        "@typescript-eslint/no-for-in-array": "error",
        "@typescript-eslint/no-inferrable-types": "error",
        "@typescript-eslint/no-misused-new": "error",
        "@typescript-eslint/no-namespace": "error",
        "@typescript-eslint/no-non-null-assertion": "off",
        "@typescript-eslint/no-shadow": "error",
        "@typescript-eslint/no-unnecessary-qualifier": "error",
        "@typescript-eslint/no-unnecessary-type-assertion": "error",
        "@typescript-eslint/no-useless-constructor": "error",
        "@typescript-eslint/no-var-requires": "error",
        "@typescript-eslint/prefer-for-of": "warn",
        "@typescript-eslint/prefer-function-type": "warn",
        "@typescript-eslint/prefer-includes": "error",
        "@typescript-eslint/prefer-string-starts-ends-with": "error",
        "@typescript-eslint/promise-function-async": "error",

        "@typescript-eslint/require-array-sort-compare": ["error", {
            ignoreStringArrays: true,
        }],

        "@typescript-eslint/restrict-plus-operands": "error",
        "@typescript-eslint/unbound-method": "error",
    },
};