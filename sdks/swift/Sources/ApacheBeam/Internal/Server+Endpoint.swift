//
//  File.swift
//  
//
//  Created by Byron Ellis on 8/9/23.
//

import GRPC

extension Server {
    var endpoint: ApiServiceDescriptor {
        get throws {
            let address = self.channel.localAddress!
            if let pathname = address.pathname {
                return ApiServiceDescriptor(unixAddress: pathname)
            }
            if let host = address.ipAddress,let port = address.port {
                return ApiServiceDescriptor(host: host, port: port)
            }
            throw ApacheBeamError.runtimeError("Can't determine endpoint for \(address)")
        }
    }
}

