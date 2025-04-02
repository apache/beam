const { configure } = require('@testing-library/react');
require('@testing-library/jest-dom');

configure({
    asyncUtilTimeout: 5000,
    react: { version: 'detect' }
});

globalThis.IS_REACT_ACT_ENVIRONMENT = true;