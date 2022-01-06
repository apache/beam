var assert = require('assert');
var service = require("../dist/apache_beam/utils/exp_service");

describe('java expansion service', function() {
    describe('constructor', function() {
        const serviceRunner = new service.JavaExpansionServiceRunner(":sdks:java:fake:runService", "VERSION")
        it('should contain the passed gradle target', function () {
            assert.equal(serviceRunner.gradleTarget, ":sdks:java:fake:runService");
        });
        it('should generate the correct jar name', function() {
            assert.equal(serviceRunner.jarName, "beam-sdks-java-fake-VERSION.jar")
        });
    });
});
