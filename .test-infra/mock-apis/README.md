# Overview

This directory holds code and related artifacts to support API related
integration tests.



# Development Dependencies

| Dependency                                          | Reason                                                                                 |
|-----------------------------------------------------|----------------------------------------------------------------------------------------|
| [go](https://go.dev)                                | For making code changes in this directory. See [go.mod](go.mod) for required version.  |
| [buf](https://github.com/bufbuild/buf#installation) | Optional for when making changes to proto.                                             |
| [ko](https://ko.build/install/)                     | To easily build Go container images.                                                   |

# Testing

## Unit

To run unit tests in this project, execute the following command:

```
go test ./src/main/go/internal/...
```

## Integration

TODO: See https://github.com/apache/beam/issues/28859

# Local Usage

## Requirements

To execute the services on your local machine, you'll need [redis](https://redis.io/docs/getting-started/installation/).

## Execute

1. Start redis
    
    Start redis using the following command.
    ```
    redis-server
    ```
2. Start refresher
    Start the refresher service:
    ```
    go run ./src/go/main/cmd/refresher
    ```
3. 

