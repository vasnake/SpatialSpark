# Changelog

## [Unreleased]
Rewrite BroadcastSpatialJoin: added more options and flexibility, support for WGS84 (lon,lat)
coordinates in WithinDistance predicate.
Add a possibility to test spark-submit and standalone apps in Docker environment.

### Added
- Docker containers for apps testing.
- Support for arbitrary objects in BroadcastSpatialJoin datasets: `RDD[(T, Geom)]`.
- Support for optional extra condition in BroadcastSpatialJoin for filtering matching records: 
`condition: Option[(L, R) => Boolean]`.

### Changed
- build.sbt, split project to lib and app parts.
- BroadcastSpatialJoin index: assuming coordinates are always in WGS84, for WithinDistance
predicate, approximate distance in meters are used.

### Fixed
- SpatialJoinApp minor discrepancies.

### Removed
- assembly.sbt

### Deprecated
- ???

## [1.1.1-beta-SNAPSHOT] - 2017-03-07
Fixing bugs, Spark 2.0.2, etc.

[Unreleased]: https://github.com/vasnake/SpatialSpark/compare/f9f726df75fe8e6113692b923a5cc6751112a982...vasnake:broadcast-spatial-join
[1.1.1-beta-SNAPSHOT]: https://github.com/vasnake/SpatialSpark/compare/1.0...f9f726df75fe8e6113692b923a5cc6751112a982
