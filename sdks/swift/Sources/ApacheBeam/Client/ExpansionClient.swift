//
//  File.swift
//  
//
//  Created by Byron Ellis on 9/15/23.
//

import Foundation
import GRPC

public struct ExpansionClient {
    let client: Org_Apache_Beam_Model_Expansion_V1_ExpansionServiceAsyncClient
    
    public init(endpoint: ApiServiceDescriptor) throws {
        client = Org_Apache_Beam_Model_Expansion_V1_ExpansionServiceAsyncClient(channel: try GRPCChannelPool.with(endpoint: endpoint, eventLoopGroup: PlatformSupport.makeEventLoopGroup(loopCount: 1)))
    }
    
    public func transforms() async throws -> [String:([String],[String],Schema)]  {
        let call = try await client.makeDiscoverSchemaTransformCall(.with { _ in }).response
        var transforms: [String:([String],[String],Schema)] = [:]
        for (k,v) in call.schemaTransformConfigs {
            transforms[k] = (v.inputPcollectionNames,v.outputPcollectionNames,.from(v.configSchema))
        }
        return transforms
    }
    
}
