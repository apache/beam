import * as path from 'path'
import * as find from '../../../src/wrapper-validation/find'
import {expect, test} from '@jest/globals'

test('finds test data wrapper jars', async () => {
  const repoRoot = path.resolve('./test/jest/wrapper-validation')
  const wrapperJars = await find.findWrapperJars(repoRoot)
  expect(wrapperJars.length).toBe(3)
  expect(wrapperJars).toContain('data/valid/gradle-wrapper.jar')
  expect(wrapperJars).toContain('data/invalid/gradle-wrapper.jar')
  expect(wrapperJars).toContain('data/invalid/gradlе-wrapper.jar') // homoglyph
})
