import {describe, expect, it} from '@jest/globals'

import * as cacheUtils from '../../src/caching/cache-utils'

describe('cacheUtils-utils', () => {
    describe('can hash', () => {
        it('a string', async () => {
            const hash = cacheUtils.hashStrings(['foo'])
            expect(hash).toBe('acbd18db4cc2f85cedef654fccc4a4d8')
        })
        it('multiple strings', async () => {
            const hash = cacheUtils.hashStrings(['foo', 'bar', 'baz'])
            expect(hash).toBe('6df23dc03f9b54cc38a0fc1483df6e21')
        })
        it('normalized filenames', async () => {
            const fileNames = ['/foo/bar/baz.zip', '../boo.html']
            const posixHash = cacheUtils.hashFileNames(fileNames)
            const windowsHash = cacheUtils.hashFileNames(fileNames)
            expect(posixHash).toBe(windowsHash)
        })
    })
})
