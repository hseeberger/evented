# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.3](https://github.com/hseeberger/evented/compare/v0.0.2...v0.0.3) - 2024-05-30

### Added
- use bigserial seq_no as PK ([#10](https://github.com/hseeberger/evented/pull/10))

### Fixed
- use ORDER BY seq_no ASC to query events ([#12](https://github.com/hseeberger/evented/pull/12))

### Other
- move code to entity module ([#14](https://github.com/hseeberger/evented/pull/14))

## [0.0.2](https://github.com/hseeberger/evented/compare/v0.0.1...v0.0.2) - 2024-05-29

### Added
- event metadata ([#8](https://github.com/hseeberger/evented/pull/8))

### Other
- update deps ([#5](https://github.com/hseeberger/evented/pull/5))
- Fix typo ([#4](https://github.com/hseeberger/evented/pull/4))
