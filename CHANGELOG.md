# Changelog

## [0.5.0](https://github.com/reqfleet/engine-sdk/compare/v0.4.0...v0.5.0) (2026-11-10)


### Features

* **containerstats:** Expose readers and fix cgroup v1 CPU units ([#19](https://github.com/reqfleet/engine-sdk/issues/19)) ([84a065b](https://github.com/reqfleet/engine-sdk/commit/84a065b4c46836cda0e76a101c169cc2584dc11f))

## [0.4.0](https://github.com/reqfleet/engine-sdk/compare/v0.3.0...v0.4.0) (2026-11-06)


### Features

* **agent:** accept in-process metrics handlers ([#16](https://github.com/reqfleet/engine-sdk/issues/16)) ([0be3f54](https://github.com/reqfleet/engine-sdk/commit/0be3f54c6200873999ea941ceef582b5812aa488))
* **agent:** expose reusable environment option parsing ([#13](https://github.com/reqfleet/engine-sdk/issues/13)) ([73e4817](https://github.com/reqfleet/engine-sdk/commit/73e4817c04d753f8d9423bbe18f3730831fc64c7)), closes [#12](https://github.com/reqfleet/engine-sdk/issues/12)

## [0.3.0](https://github.com/reqfleet/engine-sdk/compare/v0.2.1...v0.3.0) (2026-10-31)


### ⚠ BREAKING CHANGES

* Validators accept Options and use Skipped instead of Unsupported. Return LabelCount instead of Analysis. Refs: reqfleet/shibuya#1185 Co-Authored-By: openai-codex/gpt-6-astra

### Features

* Add preflight analysis and shared label admission ([#8](https://github.com/reqfleet/engine-sdk/issues/8)) ([adececb](https://github.com/reqfleet/engine-sdk/commit/adececb48e3b0477530293b46eca335988f287aa))

## [0.2.1](https://github.com/reqfleet/engine-sdk/compare/v0.2.0...v0.2.1) (2026-10-29)


### Bug Fixes

* **agent:** Allow Windows clients to import the SDK ([#6](https://github.com/reqfleet/engine-sdk/issues/6)) ([b59fdf2](https://github.com/reqfleet/engine-sdk/commit/b59fdf2bbc4390bd0859926d965acef52444a380))

## [0.2.0](https://github.com/reqfleet/engine-sdk/compare/v0.1.1...v0.2.0) (2026-10-27)


### Features

* **validation:** Add public upload validation contract ([#3](https://github.com/reqfleet/engine-sdk/issues/3)) ([70e780c](https://github.com/reqfleet/engine-sdk/commit/70e780c15a8a0f9d6696d6f232fc3b63f0d6d21c))
