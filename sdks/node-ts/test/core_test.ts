import * as beam from '../src/apache_beam';
import * as assert from 'assert';
import {BytesCoder} from '../src/apache_beam/coders/standard_coders';
// TODO(pabloem): Fix installation.

describe('core module', () => {
  describe('runs a basic impulse expansion', () => {
    it('runs a basic Impulse expansion', () => {
      const p = new beam.Pipeline();
      const res = p.apply(new beam.Impulse());

      assert.equal(res.type, 'pcollection');
      console.log('res.proto.coderId', res.proto.coderId);
      assert.deepEqual(p.getCoder(res.proto.coderId), new BytesCoder());
    });
    it('runs a ParDo expansion', () => {
      const p = new beam.Pipeline();
      const res = p.apply(new beam.Impulse()).map(v => {
        return v * 2;
      });
    });
    it('runs a GroupBy expansion', () => {});
  });
});
