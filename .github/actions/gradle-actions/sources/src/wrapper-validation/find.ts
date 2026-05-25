import * as util from 'util'
import * as path from 'path'
import * as fs from 'fs'
import unhomoglyph from 'unhomoglyph'

const readdir = util.promisify(fs.readdir)

export async function findWrapperJars(baseDir: string): Promise<string[]> {
    const files = await recursivelyListFiles(baseDir)
    return files
        .filter(file => unhomoglyph(file).endsWith('gradle-wrapper.jar'))
        .map(wrapperJar => path.relative(baseDir, wrapperJar))
        .sort((a, b) => a.localeCompare(b))
}

async function recursivelyListFiles(baseDir: string): Promise<string[]> {
    const childrenNames = await readdir(baseDir)
    const childrenPaths = await Promise.all(
        childrenNames.map(async childName => {
            const childPath = path.resolve(baseDir, childName)
            const stat = fs.lstatSync(childPath, {throwIfNoEntry: false})
            if (stat === undefined) {
                return []
            } else if (stat.isDirectory()) {
                return recursivelyListFiles(childPath)
            } else {
                return new Promise(resolve => resolve([childPath]))
            }
        })
    )
    return Array.prototype.concat(...childrenPaths)
}
