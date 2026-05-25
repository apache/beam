import * as path from 'path'
import * as fs from 'fs'
import * as validate from '../../../src/wrapper-validation/validate'
import {expect, test, jest} from '@jest/globals'
import { WrapperChecksums, KNOWN_CHECKSUMS } from '../../../src/wrapper-validation/checksums'
import { ChecksumCache } from '../../../src/wrapper-validation/cache'
import { ACTION_METADATA_DIR } from '../../../src/configuration'

jest.setTimeout(30000)

const baseDir = path.resolve('./test/jest/wrapper-validation')
const tmpDir = path.resolve('./test/jest/tmp')

const CHECKSUM_3888 = '3888c76faa032ea8394b8a54e04ce2227ab1f4be64f65d450f8509fe112d38ce'

function knownChecksumsWithout3888(): WrapperChecksums {
  const knownChecksums = new WrapperChecksums()
  // iterate over all known checksums and add them to the knownChecksums object
  for (const [checksum, versions] of KNOWN_CHECKSUMS.checksums) {
    if (checksum !== CHECKSUM_3888) {
      for (const version of versions) {
        knownChecksums.add(version, checksum)
      }
    }
  }
  return knownChecksums
}

test('succeeds if all found wrapper jars are valid', async () => {
  const result = await validate.findInvalidWrapperJars(baseDir, false, [
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
  ])

  expect(result.isValid()).toBe(true)
  // Only hardcoded and explicitly allowed checksums should have been used
  expect(result.fetchedChecksums).toBe(false)

  expect(result.toDisplayString()).toBe(
    '✓ Found known Gradle Wrapper JAR files:\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradle-wrapper.jar\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradlе-wrapper.jar\n' + // homoglyph
      '  3888c76faa032ea8394b8a54e04ce2227ab1f4be64f65d450f8509fe112d38ce data/valid/gradle-wrapper.jar'
  )
})

test('succeeds if all found wrapper jars are previously valid', async () => {
  const result = await validate.findInvalidWrapperJars(baseDir, false, [], [
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    '3888c76faa032ea8394b8a54e04ce2227ab1f4be64f65d450f8509fe112d38ce'
  ])

  expect(result.isValid()).toBe(true)
  // Only hardcoded and explicitly allowed checksums should have been used
  expect(result.fetchedChecksums).toBe(false)

  expect(result.toDisplayString()).toBe(
    '✓ Found known Gradle Wrapper JAR files:\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradle-wrapper.jar\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradlе-wrapper.jar\n' + // homoglyph
      '  3888c76faa032ea8394b8a54e04ce2227ab1f4be64f65d450f8509fe112d38ce data/valid/gradle-wrapper.jar'
  )
})

test('succeeds if all found wrapper jars are valid (and checksums are fetched from Gradle API)', async () => {
  const knownValidChecksums = knownChecksumsWithout3888()
  const result = await validate.findInvalidWrapperJars(
    baseDir,
    false,
    ['e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'],
    [],
    knownValidChecksums
  )
  console.log(`fetchedChecksums = ${result.fetchedChecksums}`)

  expect(result.isValid()).toBe(true)
  // Should have fetched checksums because no known checksums were provided
  expect(result.fetchedChecksums).toBe(true)

  expect(result.toDisplayString()).toBe(
    '✓ Found known Gradle Wrapper JAR files:\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradle-wrapper.jar\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradlе-wrapper.jar\n' + // homoglyph
      '  3888c76faa032ea8394b8a54e04ce2227ab1f4be64f65d450f8509fe112d38ce data/valid/gradle-wrapper.jar'
  )
})

test('fails if invalid wrapper jars are found', async () => {
  const result = await validate.findInvalidWrapperJars(baseDir, false, [])

  expect(result.isValid()).toBe(false)

  expect(result.valid).toEqual([
    new validate.WrapperJar(
      'data/valid/gradle-wrapper.jar',
      '3888c76faa032ea8394b8a54e04ce2227ab1f4be64f65d450f8509fe112d38ce'
    )
  ])

  expect(result.invalid).toEqual([
    new validate.WrapperJar(
      'data/invalid/gradle-wrapper.jar',
      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    ),
    new validate.WrapperJar(
      'data/invalid/gradlе-wrapper.jar', // homoglyph
      'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    )
  ])

  expect(result.toDisplayString()).toBe(
    '✗ Found unknown Gradle Wrapper JAR files:\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradle-wrapper.jar\n' +
      '  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 data/invalid/gradlе-wrapper.jar\n' + // homoglyph
      '✓ Found known Gradle Wrapper JAR files:\n' +
      '  3888c76faa032ea8394b8a54e04ce2227ab1f4be64f65d450f8509fe112d38ce data/valid/gradle-wrapper.jar'
  )
})

test('can save and load checksums', async () => {
  const cacheDir = path.join(tmpDir, 'wrapper-validation-cache')
  fs.rmSync(cacheDir, {recursive: true, force: true})

  const checksumCache = new ChecksumCache(cacheDir)

  expect(checksumCache.load()).toEqual([])

  checksumCache.save(['123', '456'])

  expect(checksumCache.load()).toEqual(['123', '456'])
  expect(fs.existsSync(cacheDir)).toBe(true)
})

test('can load empty checksum file', async () => {
    const cacheDir = path.join(tmpDir, 'empty-wrapper-validation-cache')
    const metadataDir = path.join(cacheDir, ACTION_METADATA_DIR)
    const emptyChecksumFile = path.join(metadataDir, 'valid-wrappers.json')
    fs.mkdirSync(metadataDir, { recursive: true });
    fs.writeFileSync(emptyChecksumFile, '')
    const checksumCache = new ChecksumCache(cacheDir)

    expect(checksumCache.load()).toEqual([])
})
