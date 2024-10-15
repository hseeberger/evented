# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.4](https://github.com/hseeberger/evented/compare/evented-v0.2.3...evented-v0.2.4) - 2024-10-15

### Other

- update secrecy to 0.10 ([#119](https://github.com/hseeberger/evented/pull/119))
- *(deps)* bump tower from 0.4.13 to 0.5.1 ([#118](https://github.com/hseeberger/evented/pull/118))
- *(deps)* bump serde_with from 3.10.0 to 3.11.0 ([#115](https://github.com/hseeberger/evented/pull/115))
- *(deps)* bump serde_with from 3.9.0 to 3.10.0 ([#110](https://github.com/hseeberger/evented/pull/110))

## [0.2.3](https://github.com/hseeberger/evented/compare/evented-v0.2.2...evented-v0.2.3) - 2024-08-20

### Other
- use latest sqlx ([#79](https://github.com/hseeberger/evented/pull/79))
- update telemetry dependencies ([#80](https://github.com/hseeberger/evented/pull/80))
- *(deps)* bump tokio from 1.39.2 to 1.39.3 ([#77](https://github.com/hseeberger/evented/pull/77))
- *(deps)* bump api-version from `f6a2851` to `c36ef8c` ([#76](https://github.com/hseeberger/evented/pull/76))
- *(deps)* bump serde from 1.0.207 to 1.0.208 ([#75](https://github.com/hseeberger/evented/pull/75))
- *(deps)* bump serde_json from 1.0.124 to 1.0.125 ([#74](https://github.com/hseeberger/evented/pull/74))
- *(deps)* bump serde from 1.0.206 to 1.0.207 ([#73](https://github.com/hseeberger/evented/pull/73))
- *(deps)* bump serde_json from 1.0.122 to 1.0.124 ([#72](https://github.com/hseeberger/evented/pull/72))
- *(deps)* bump serde from 1.0.205 to 1.0.206 ([#71](https://github.com/hseeberger/evented/pull/71))
- *(deps)* bump serde from 1.0.204 to 1.0.205 ([#70](https://github.com/hseeberger/evented/pull/70))
- *(deps)* bump api-version from `b4533f8` to `f6a2851` ([#69](https://github.com/hseeberger/evented/pull/69))
- *(deps)* bump api-version from `e45e8ee` to `b4533f8` ([#68](https://github.com/hseeberger/evented/pull/68))
- *(deps)* bump serde_json from 1.0.121 to 1.0.122 ([#67](https://github.com/hseeberger/evented/pull/67))
- *(deps)* bump api-version from `d0420f9` to `e45e8ee` ([#66](https://github.com/hseeberger/evented/pull/66))
- *(deps)* bump tokio from 1.39.0 to 1.39.2 ([#65](https://github.com/hseeberger/evented/pull/65))
- *(deps)* bump serde_json from 1.0.120 to 1.0.121 ([#64](https://github.com/hseeberger/evented/pull/64))
- *(deps)* bump api-version from `82ee85f` to `d0420f9` ([#63](https://github.com/hseeberger/evented/pull/63))
- *(deps)* bump tokio from 1.38.1 to 1.39.0 ([#61](https://github.com/hseeberger/evented/pull/61))
- *(deps)* bump api-version from `86b5992` to `82ee85f` ([#59](https://github.com/hseeberger/evented/pull/59))
- *(deps)* bump thiserror from 1.0.62 to 1.0.63 ([#60](https://github.com/hseeberger/evented/pull/60))
- *(deps)* bump tokio from 1.38.0 to 1.38.1 ([#58](https://github.com/hseeberger/evented/pull/58))
- *(deps)* bump api-version from `396853b` to `86b5992` ([#57](https://github.com/hseeberger/evented/pull/57))
- *(deps)* bump serde_with from 3.8.3 to 3.9.0 ([#56](https://github.com/hseeberger/evented/pull/56))
- *(deps)* bump api-version from `f961401` to `396853b` ([#55](https://github.com/hseeberger/evented/pull/55))
- *(deps)* bump thiserror from 1.0.61 to 1.0.62 ([#54](https://github.com/hseeberger/evented/pull/54))
- *(deps)* bump zerovec-derive from 0.10.2 to 0.10.3 ([#53](https://github.com/hseeberger/evented/pull/53))
- *(deps)* bump uuid from 1.9.1 to 1.10.0 ([#52](https://github.com/hseeberger/evented/pull/52))

## [0.2.2](https://github.com/hseeberger/evented/compare/evented-v0.2.1...evented-v0.2.2) - 2024-07-03

### Fixed
- add type_name filter for event log filters ([#38](https://github.com/hseeberger/evented/pull/38))
- use more stable postgres testcontainers setup ([#43](https://github.com/hseeberger/evented/pull/43))

### Other
- *(deps)* bump serde_json from 1.0.119 to 1.0.120 ([#41](https://github.com/hseeberger/evented/pull/41))

## [0.2.1](https://github.com/hseeberger/evented/compare/evented-v0.2.0...evented-v0.2.1) - 2024-06-26

### Fixed
- record last version after recovery ([#32](https://github.com/hseeberger/evented/pull/32))

### Other
- make EventWithMetadata fields public ([#36](https://github.com/hseeberger/evented/pull/36))
- *(deps)* bump serde_json from 1.0.117 to 1.0.118 ([#35](https://github.com/hseeberger/evented/pull/35))
- *(deps)* bump uuid from 1.9.0 to 1.9.1 ([#34](https://github.com/hseeberger/evented/pull/34))
- *(deps)* bump uuid from 1.8.0 to 1.9.0 ([#33](https://github.com/hseeberger/evented/pull/33))
- *(deps)* bump api-version from `db05a23` to `f961401` ([#29](https://github.com/hseeberger/evented/pull/29))
- update deps ([#27](https://github.com/hseeberger/evented/pull/27))
- add rusty-accounts example ([#23](https://github.com/hseeberger/evented/pull/23))

## [0.0.2](https://github.com/hseeberger/evented/compare/v0.0.1...v0.0.2) - 2024-05-29

### Added
- event metadata ([#8](https://github.com/hseeberger/evented/pull/8))

### Other
- update deps ([#5](https://github.com/hseeberger/evented/pull/5))
- Fix typo ([#4](https://github.com/hseeberger/evented/pull/4))
