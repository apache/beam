import * as beam from '../src/apache_beam';

describe("core module", function() {
    describe("basic ptransform", function() {
        it("runs a basic expansion", function() {
            var p = new beam.Pipeline('pipeline-name');
            var res2 = p.apply(new beam.Impulse())
                .apply(new beam.ParDo(function(v) {return v*2;}));
            console.log(res2);
        });
    }); 
});