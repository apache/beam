const beam = require("../dist/apache_beam");
// TODO(pabloem): Fix installation.

describe("core module", function() {
    describe("basic ptransform", function() {
        it("runs a basic expansion", function() {
            var p = new beam.Pipeline();
            var res2 = p.apply(new beam.Impulse())
                .apply(new beam.ParDo(function(v) {return v*2;}));
            console.log(res2);
        });
    });     
});