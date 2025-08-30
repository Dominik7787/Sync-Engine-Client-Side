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
      url: "https://github.com/REPLACE_OWNER/sync-engine/releases/download/0.1.0/SyncEngine.xcframework.zip",
      checksum: "REPLACE_CHECKSUM"
    ),
    .target(
      name: "SyncEngine",
      dependencies: ["SyncEngineCore"],
      path: "swift/Sources/SyncEngine"
    )
  ]
)


