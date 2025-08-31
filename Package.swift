// swift-tools-version:5.7
import PackageDescription

let package = Package(
  name: "SyncEngine",
  platforms: [
    .iOS(.v16)
  ],
  products: [
    .library(name: "SyncEngine", targets: ["SyncEngine"]) // expose Swift wrapper
  ],
  targets: [
    .binaryTarget(
      name: "SyncEngineCore",
      url: "https://github.com/Dominik7787/Sync-Engine-Client-Side/releases/download/v0.1.1/SyncEngine.xcframework.zip",
      checksum: "0dff93b2ea9d1a79d4255e40421663f5eaa085b982b3e566631a1479d8473a5f"
    ),
    .target(
      name: "SyncEngine",
      dependencies: ["SyncEngineCore"],
      path: "swift/Sources/SyncEngine"
    )
  ]
)


