//
//  FileIOTests.swift
//  
//
//  Created by Byron Ellis on 8/22/23.
//
import ApacheBeam
import Logging
import XCTest

final class FileIOTests: XCTestCase {

    override func setUpWithError() throws {
    }

    override func tearDownWithError() throws {
    }

    func testGoogleStorageListFiles() async throws {
        throw XCTSkip()
        try await PCollectionTest(PCollection<KV<String,String>>().listFiles(in: GoogleStorage.self)) { log,inputs,outputs in
            log.info("Sending value")
            try inputs[0].emit(value:KV("dataflow-samples","shakespeare"))
            log.info("Value sent")
            inputs[0].finish()
            for try await (output,_,_) in outputs[0] {
                log.info("Output: \(output)")
            }
        }.run()
    }

    func testGoogleStorageReadFiles() async throws {
        try await PCollectionTest(PCollection<KV<String,String>>().readFiles(in: GoogleStorage.self)) { log,inputs,outputs in
            throw XCTSkip()
            log.info("Sending value")
            try inputs[0].emit(value:KV("dataflow-samples","shakespeare/asyoulikeit.txt"))
            log.info("Value sent")
            inputs[0].finish()
            for try await (output,_,_) in outputs[0] {
                log.info("Output: \(String(data:output as! Data,encoding:.utf8)!)")
            }
        }.run()
    }

    func testShakespeareWordcount() async throws {
        try await Pipeline { pipeline in
            let contents = pipeline
                .create(["dataflow-samples/shakespeare"])
                .map({ value in
                    let parts = value.split(separator: "/",maxSplits: 1)
                    return KV(parts[0].lowercased(),parts[1].lowercased())
                })
                .listFiles(in: GoogleStorage.self)
                .readFiles(in: GoogleStorage.self)
            
            // Simple ParDo that takes advantage of enumerateLines. No name to test name generation of pardos
            let lines = contents.pstream { contents,lines in
                for await (content,ts,w) in contents {
                    String(data:content,encoding:.utf8)!.enumerateLines { line,_ in
                        lines.emit(line,timestamp:ts,window:w)
                    }
                }
            }
            
            // Our first group by operation
            let baseCount = lines
                .flatMap({ $0.components(separatedBy: .whitespaces) })
                .groupBy({ ($0,1) })
                .sum()
                .log(prefix:"INTERMEDIATE OUTPUT")
            
            let normalizedCounts = baseCount.groupBy {
                ($0.key.lowercased().trimmingCharacters(in: .punctuationCharacters),
                 $0.value ?? 1)
            }.sum()
            
            normalizedCounts.log(prefix:"COUNT OUTPUT")
            
        }.run(PortableRunner(loopback:true))
    }
    

}
