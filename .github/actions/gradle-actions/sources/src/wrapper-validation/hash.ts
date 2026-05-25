import * as crypto from 'crypto'
import * as fs from 'fs'

export async function sha256File(path: string): Promise<string> {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256')
        const stream = fs.createReadStream(path)
        stream.on('data', data => hash.update(data))
        stream.on('end', () => {
            stream.destroy()
            resolve(hash.digest('hex'))
        })
        stream.on('error', error => {
            stream.destroy()
            reject(error)
        })
    })
}
