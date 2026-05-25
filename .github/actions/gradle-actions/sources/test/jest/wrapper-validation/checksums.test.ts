import * as checksums from '../../../src/wrapper-validation/checksums'
import nock from 'nock'
import {afterEach, describe, expect, test, jest} from '@jest/globals'

jest.setTimeout(60000)

const CHECKSUM_8_1 = 'ed2c26eba7cfb93cc2b7785d05e534f07b5b48b5e7fc941921cd098628abca58'

function knownChecksumsWithout8_1(): checksums.WrapperChecksums {
  const knownChecksums = new checksums.WrapperChecksums()
  // iterate over all known checksums and add them to the knownChecksums object
  for (const [checksum, versions] of checksums.KNOWN_CHECKSUMS.checksums) {
    if (checksum !== CHECKSUM_8_1) {
      for (const version of versions) {
        knownChecksums.add(version, checksum)
      }
    }
  }
  return knownChecksums
}


test('has loaded hardcoded wrapper jars checksums', async () => {
  // Sanity check that generated checksums file is not empty and was properly imported
  expect(checksums.KNOWN_CHECKSUMS.checksums.size).toBeGreaterThan(10)
  // Verify that checksums of arbitrary versions are contained
  expect(
    checksums.KNOWN_CHECKSUMS.checksums.get(
      '660ab018b8e319e9ae779fdb1b7ac47d0321bde953bf0eb4545f14952cfdcaa3'
    )
  ).toEqual(new Set(['4.10.3']))
  expect(
    checksums.KNOWN_CHECKSUMS.checksums.get(
      '28b330c20a9a73881dfe9702df78d4d78bf72368e8906c70080ab6932462fe9e'
    )
  ).toEqual(new Set(['6.0-rc-1', '6.0-rc-2', '6.0-rc-3', '6.0', '6.0.1']))
})

test('fetches wrapper jar checksums that are missing from hardcoded set', async () => {
  const unknownChecksums = await checksums.fetchUnknownChecksums(false, knownChecksumsWithout8_1())

  expect(unknownChecksums.checksums.size).toBeGreaterThan(0)
  expect(unknownChecksums.checksums.has(CHECKSUM_8_1)).toBe(true)
  expect(unknownChecksums.checksums.get(CHECKSUM_8_1)).toEqual(new Set(['8.1-rc-1', '8.1-rc-2', '8.1-rc-3', '8.1-rc-4', '8.1', '8.1.1']))
})

test('fetches wrapper jar checksums for snapshots', async () => {
  const knownChecksums = knownChecksumsWithout8_1()
  const nonSnapshotChecksums = await checksums.fetchUnknownChecksums(false, knownChecksums)
  const allValidChecksums = await checksums.fetchUnknownChecksums(true, knownChecksums)

  // Should always be many more snapshot versions
  expect(allValidChecksums.versions.size - nonSnapshotChecksums.versions.size).toBeGreaterThan(20)
  // May not be any unique snapshot checksums
  expect(allValidChecksums.checksums.size).toBeGreaterThanOrEqual(nonSnapshotChecksums.checksums.size)
})

describe('retry', () => {
  afterEach(() => {
    nock.cleanAll()
  })

  describe('for /versions/all API', () => {
    test('retry three times', async () => {
      nock('https://services.gradle.org', {allowUnmocked: true})
        .get('/versions/all')
        .times(3)
        .replyWithError({
          message: 'connect ECONNREFUSED 104.18.191.9:443',
          code: 'ECONNREFUSED'
        })

      const validChecksums = await checksums.fetchUnknownChecksums(false, knownChecksumsWithout8_1())
      expect(validChecksums.checksums.size).toBeGreaterThan(0)
      nock.isDone()
    })
  })
})
