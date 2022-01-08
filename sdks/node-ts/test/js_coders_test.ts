import * as beam from '../src/apache_beam';
import * as assert from 'assert';
import {Context} from '../src/apache_beam/coders/coders';
import {
  BytesCoder,
  IterableCoder,
  KVCoder,
  StrUtf8Coder,
  VarIntCoder,
} from '../src/apache_beam/coders/standard_coders';
import {GroupBy} from '../src/apache_beam/transforms/core';
import {
  BsonObjectCoder,
  GeneralObjectCoder,
} from '../src/apache_beam/coders/js_coders';
import {BufferReader, BufferWriter} from 'protobufjs';

describe('JavaScript native coders', () => {
  describe('BSON Object Coder', () => {
    const bsonCoder = new BsonObjectCoder();
    it('encodes and decodes an object properly', () => {
      const inputObject = {
        str: 'astring',
        int: 1,
        float: 1.2345,
        obj: {any: 'any'},
        null: null,
        bool: true,
        // 'undef': undefined,  // TODO(pabloem): Figure out how to support undefined encoding/decoding.
        bigint: Number.MAX_SAFE_INTEGER + 100,
      };
      const writer = new BufferWriter();

      const encoded = bsonCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        bsonCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
  });

  describe('general object coder', () => {
    const objCoder = new GeneralObjectCoder();
    it('encodes and decodes an object properly', () => {
      const inputObject = {
        str: 'astring',
        int: 1,
        float: 1.2345,
        obj: {any: 'any'},
        null: null,
        bool: true,
        // 'undef': undefined,  // TODO(pabloem): Figure out how to support undefined encoding/decoding.
        bigint: Number.MAX_SAFE_INTEGER + 100,
      };
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it('encodes and decodes a string properly', () => {
      const inputObject = 'abcde';
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.equal(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it('encodes and decodes a number properly', () => {
      const inputObject = 12345678;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it('encodes and decodes a BigInt properly', function () {
      // TODO(pabloem): THIS TEST NEEDS TO BE sKIPPED BECAUSE VERY LARGE INTS ARE BROKEN
      this.skip();
      const inputObject = Number.MAX_SAFE_INTEGER + 123456789;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.deepEqual(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it('encodes and decodes a true boolean properly', () => {
      const inputObject = true;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.equal(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
    it('encodes and decodes a false boolean properly', () => {
      const inputObject = false;
      const writer = new BufferWriter();

      const encoded = objCoder.encode(
        inputObject,
        writer,
        Context.needsDelimiters
      );

      const buffer = writer.finish();
      const reader = new BufferReader(buffer);
      assert.equal(
        objCoder.decode(reader, Context.needsDelimiters),
        inputObject
      );
    });
  });
});
