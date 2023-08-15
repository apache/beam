# Swift SDK for Beam

The Swift SDK for Beam is a "portable" Beam implementation written in native Swift. 

## Usage

To use the Swift SDK for Beam you should add it to your own executable package as a dependency:
```
let package = Package(
    dependencies:[
        .package(url:"https://github.com/apache/beam/sdks/swift, from: "2.51.0")
    ],
    targets:[
        // targets
    ]
)
```


