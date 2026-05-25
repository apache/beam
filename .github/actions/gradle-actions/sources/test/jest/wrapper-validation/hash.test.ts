import * as path from 'path'
import * as hash from '../../../src/wrapper-validation/hash'
import {expect, test} from '@jest/globals'

test('can sha256 files', async () => {
  const sha = await hash.sha256File(
    path.resolve('test/jest/wrapper-validation/data/invalid/gradle-wrapper.jar')
  )
  expect(sha).toEqual(
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
  )
})
