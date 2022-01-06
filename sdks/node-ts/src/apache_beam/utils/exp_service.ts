const fs = require('fs')
//const https = require('https')
const os = require('os')
const path = require('path')

const APACHE_REPOSITORY = 'https://repo.maven.apache.org/maven2'
const BEAM_GROUP_ID = 'org/apache/beam'
const JAR_CACHE = '~/.apache_beam/cache/jars'

const HOME_DIR = os.homedir();

export class JavaExpansionServiceRunner {
    gradleTarget: string;
    jarUrl: string;
    jarName: string;
    jarCache: string = path.join(HOME_DIR, JAR_CACHE.substring(2));

    constructor(target: string, version: string) {
        this.gradleTarget = target
        var targetSplit: number = target.lastIndexOf(':')
        var splitTarget = target.slice(0, targetSplit).split(":")
        var targetPath: string = 'beam' + splitTarget.join('-')
        this.jarName = targetPath + '-' + version + '.jar'
        var arr = new Array(BEAM_GROUP_ID, target, version, this.jarName)
        this.jarUrl = arr.join('/')
    }

    getBeamJar(jarName: string): string {
        // Ensure that the cache directory exists
        fs.mkdirSync(this.jarCache, {recursive: true})

        var jarPath: string = path.join(this.jarCache, this.jarName)
        // Return if the JAR is already in memory
        if(fs.existsSync(jarPath)) {
            return jarPath
        }

        if(jarName.includes('.dev')){
            throw new Error('Cannot pull dev versions of JARs, please run gradlew ' + this.gradleTarget + ' to start your expansion service.')
        }

        this.downloadJar()
        return jarPath
    }

    // TODO: make HTTPS request, save non-error response body as jarName.
    private downloadJar() {
        throw new Error('Method has not been implemented')
    }

    // TODO: start process using `java -jar ${jarPath}`, ensure that it doesn't
    // exit immediately, retain reference to process for stopping later.
    runJar() {
        throw new Error('Method has not been implemented')
    }

    // TODO: check if process exits and is alive, if so kill it.
    stopJar() {
        throw new Error('Method has not been implemented')
    }
}
