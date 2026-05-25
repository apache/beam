import {describe, expect, it} from '@jest/globals'

import {CacheEntryListener, CacheListener} from '../../src/caching/cache-reporting'

describe('caching report', () => {
    describe('reports not fully restored', () => {
        it('with one requested entry report', async () => {
            const report = new CacheListener()
            report.entry('foo').markRequested('1', ['2'])
            report.entry('bar').markRequested('3').markRestored('4', 500, 1000)
            expect(report.fullyRestored).toBe(false)
        })
    })
    describe('reports fully restored', () => {
        it('when empty', async () => {
            const report = new CacheListener()
            expect(report.fullyRestored).toBe(true)
        })
        it('with empty entry reports', async () => {
            const report = new CacheListener()
            report.entry('foo')
            report.entry('bar')
            expect(report.fullyRestored).toBe(true)
        })
        it('with restored entry report', async () => {
            const report = new CacheListener()
            report.entry('bar').markRequested('3').markRestored('4', 300, 1000)
            expect(report.fullyRestored).toBe(true)
        })
        it('with multiple restored entry reportss', async () => {
            const report = new CacheListener()
            report.entry('foo').markRestored('4', 3300, 111)
            report.entry('bar').markRequested('3').markRestored('4', 333, 1000)
            expect(report.fullyRestored).toBe(true)
        })
    })
    describe('can be stringified and rehydrated', () => {
        it('when empty', async () => {
            const report = new CacheListener()

            const stringRep = report.stringify()
            const reportClone: CacheListener = CacheListener.rehydrate(stringRep)

            expect(reportClone.cacheEntries).toEqual([])

            // Can call methods on rehydrated
            expect(reportClone.entry('foo')).toBeInstanceOf(CacheEntryListener)
        })
        it('with entry reports', async () => {
            const report = new CacheListener()
            report.entry('foo')
            report.entry('bar')
            report.entry('baz')

            const stringRep = report.stringify()
            const reportClone: CacheListener = CacheListener.rehydrate(stringRep)

            expect(reportClone.cacheEntries.length).toBe(3)
            expect(reportClone.cacheEntries[0].entryName).toBe('foo')
            expect(reportClone.cacheEntries[1].entryName).toBe('bar')
            expect(reportClone.cacheEntries[2].entryName).toBe('baz')

            expect(reportClone.entry('foo')).toBe(reportClone.cacheEntries[0])
        })
        it('with rehydrated entry report', async () => {
            const report = new CacheListener()
            const entryReport = report.entry('foo')
            entryReport.markRequested('1', ['2', '3'])
            entryReport.markSaved('4', 100, 1000)

            const stringRep = report.stringify()
            const reportClone: CacheListener = CacheListener.rehydrate(stringRep)
            const entryClone = reportClone.entry('foo')

            expect(entryClone.requestedKey).toBe('1')
            expect(entryClone.requestedRestoreKeys).toEqual(['2', '3'])
            expect(entryClone.savedKey).toBe('4')
            expect(entryClone.savedSize).toBe(100)
            expect(entryClone.savedTime).toBe(1000)
        })
        it('with live entry report', async () => {
            const report = new CacheListener()
            const entryReport = report.entry('foo')
            entryReport.markRequested('1', ['2', '3'])

            const stringRep = report.stringify()
            const reportClone: CacheListener = CacheListener.rehydrate(stringRep)
            const entryClone = reportClone.entry('foo')

            // Check type and call method on rehydrated entry report
            expect(entryClone).toBeInstanceOf(CacheEntryListener)
            entryClone.markSaved('4', 100, 1000)

            expect(entryClone.requestedKey).toBe('1')
            expect(entryClone.requestedRestoreKeys).toEqual(['2', '3'])
            expect(entryClone.savedKey).toBe('4')
        })
    })
})
