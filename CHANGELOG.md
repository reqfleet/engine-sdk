# Changelog

## [0.6.0](https://github.com/reqfleet/engine-sdk/compare/v0.5.0...v0.6.0) (2026-11-13)


### ⚠ BREAKING CHANGES

* **metrics:** Call MetricsRecorder.Reset(run) before first ingestion and each new run. Recorder samples no longer supply identity. Quiesce prior-run callbacks before reset. Refs: #34 Refs: #35 Co-Authored-By: GPT-6 Astra
* **agent:** Replace ShibuyaMetric.ToPrometheus with MetricsRegistry.RecordEvent. Refs: #28 Co-Authored-By: GPT-6 Astra

### Features

* **agent:** Share resource sampling and SDK metrics ingestion ([#30](https://github.com/reqfleet/engine-sdk/issues/30)) ([33e8857](https://github.com/reqfleet/engine-sdk/commit/33e8857959007ca6361c1422213c3dd9833f66fa))
* **conformance:** Add optional adapter conformance toolkit ([#36](https://github.com/reqfleet/engine-sdk/issues/36)) ([126a81e](https://github.com/reqfleet/engine-sdk/commit/126a81e3e04d454189d66da4ee1a0aac2615133b)), closes [#26](https://github.com/reqfleet/engine-sdk/issues/26)
* **issue-31:** Add independent latency and status recording ([#37](https://github.com/reqfleet/engine-sdk/issues/37)) ([21d1250](https://github.com/reqfleet/engine-sdk/commit/21d125015afad8b83edd7f1b39224a59251d1ff3)), closes [#31](https://github.com/reqfleet/engine-sdk/issues/31)
* **metrics:** Add run-scoped recording with shared admission ([#38](https://github.com/reqfleet/engine-sdk/issues/38)) ([aa857a8](https://github.com/reqfleet/engine-sdk/commit/aa857a8a9abcb4b3c49bc2fd7289c7831cee4816)), closes [#31](https://github.com/reqfleet/engine-sdk/issues/31) [#32](https://github.com/reqfleet/engine-sdk/issues/32) [#33](https://github.com/reqfleet/engine-sdk/issues/33)
* **metrics:** Bind recorder identity and reset run state ([#40](https://github.com/reqfleet/engine-sdk/issues/40)) ([b53aa17](https://github.com/reqfleet/engine-sdk/commit/b53aa17df9fd9342e4d4fd4f89d00ee948a7f02d)), closes [#34](https://github.com/reqfleet/engine-sdk/issues/34)
* **metricschema:** Expose canonical histogram boundaries ([#29](https://github.com/reqfleet/engine-sdk/issues/29)) ([22f3359](https://github.com/reqfleet/engine-sdk/commit/22f33598bc076d4e04c6634a81218023fd738323)), closes [#25](https://github.com/reqfleet/engine-sdk/issues/25)
* **metrics:** Share request-name budget admission ([#24](https://github.com/reqfleet/engine-sdk/issues/24)) ([f8c584e](https://github.com/reqfleet/engine-sdk/commit/f8c584e2d6d4a366919035b7d2b132fd6bb0413a))
* **sdk:** Expose public limit and default constants ([#21](https://github.com/reqfleet/engine-sdk/issues/21)) ([fb007ae](https://github.com/reqfleet/engine-sdk/commit/fb007ae11552d3f0bcd64f28b4f5fd9631c76781))

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
