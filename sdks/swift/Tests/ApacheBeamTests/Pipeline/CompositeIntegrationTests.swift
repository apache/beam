//
//  CompositeIntegrationTests.swift
//  
//
//  Created by Byron Ellis on 8/22/23.
//

import ApacheBeam
import XCTest

/// Simple composite. Interesting the type resolution in composites doesn't work as well as other things? Not sure why that is. Not a huge deal.
public struct FixtureWordCount : PTransform {
    
    let fixtures: [String]
    public init(fixtures:[String]) {
        self.fixtures = fixtures
    }
    
    public var expand: some PTransform {
        
        let (contents,errors) = create(self.fixtures)
            .pardo(name:"Read Files") { (filenames,output:PCollectionStream<String>,errors:PCollectionStream<String>) in
            for await (filename,_,_) in filenames {
                do {
                    output.emit(String(decoding:try fixtureData(filename),as:UTF8.self))
                } catch {
                    errors.emit("Unable to read \(filename): \(error)")
                }
            }
        }
        
        let baseCount = contents.pardo { (contents,lines:PCollectionStream<String>) in
            for await (content,_,_) in contents {
                content.enumerateLines { line,_ in
                    lines.emit(line)
                }
            }
        }
        .flatMap({ $0.components(separatedBy: .whitespaces) })
        .groupBy({ ($0,1) })
        .sum()
        
        Output("counts") {
            baseCount.groupBy {
                ($0.key.lowercased().trimmingCharacters(in: .punctuationCharacters),
                 $0.value ?? 1)
            }.sum()
        }
        
        Output("errors") {
            errors
        }


    }
}


/// Test cases for composite test
final class CompositeIntegrationTests: XCTestCase {

    override func setUpWithError() throws {
    }

    override func tearDownWithError() throws {
    }

    func testCompositeWordCount() async throws {
        throw XCTSkip()
        try await Pipeline {
            FixtureWordCount(fixtures: ["file1.txt","file2.txt","missing.txt"])
        }.run(PortableRunner(loopback:true))

    }


}
