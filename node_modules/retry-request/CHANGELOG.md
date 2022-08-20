# Changelog

## [5.0.1](https://github.com/googleapis/retry-request/compare/v5.0.0...v5.0.1) (2022-06-09)


### Bug Fixes

* respect totalTimeout and do not retry if nextRetryDelay is <= 0 ([#38](https://github.com/googleapis/retry-request/issues/38)) ([9501a42](https://github.com/googleapis/retry-request/commit/9501a42d06a620282dcd2ff9990fd0b5033a990b))

## [5.0.0](https://github.com/googleapis/retry-request/compare/v4.2.2...v5.0.0) (2022-05-06)


### ⚠ BREAKING CHANGES

* drop node 10 (#68)

### Build System

* drop node 10 ([#68](https://github.com/googleapis/retry-request/issues/68)) ([00ec90c](https://github.com/googleapis/retry-request/commit/00ec90c4d3cb29245ca746e0e133fcddc22d2251))

### [4.2.1](https://github.com/googleapis/retry-request/compare/v4.2.0...v4.2.1) (2022-04-12)


### Bug Fixes

* add new retry options to types ([#36](https://github.com/googleapis/retry-request/issues/36)) ([3f10798](https://github.com/googleapis/retry-request/commit/3f10798f47c03b50f1ba352b04d09ea3d0458b9c))
* use extend instead of object.assign ([#37](https://github.com/googleapis/retry-request/issues/37)) ([8c8dcdd](https://github.com/googleapis/retry-request/commit/8c8dcdd7d6262ce305c93fa4a8a7b2630e984824))

## [4.2.0](https://github.com/googleapis/retry-request/compare/v4.1.0...v4.2.0) (2022-04-06)


### Features

* support enhanced retry settings ([#35](https://github.com/googleapis/retry-request/issues/35)) ([0184676](https://github.com/googleapis/retry-request/commit/0184676dee36596fb939fb4559af11d0a14f64bd))


### Bug Fixes

* add new retry options to types ([#36](https://github.com/googleapis/retry-request/issues/36)) ([3f10798](https://github.com/googleapis/retry-request/commit/3f10798f47c03b50f1ba352b04d09ea3d0458b9c))
* correctly calculate retry attempt ([#33](https://github.com/googleapis/retry-request/issues/33)) ([4c852e2](https://github.com/googleapis/retry-request/commit/4c852e2ba22a7f75edfb3c905bd37a7e9913e67d))
* use extend instead of object.assign ([#37](https://github.com/googleapis/retry-request/issues/37)) ([8c8dcdd](https://github.com/googleapis/retry-request/commit/8c8dcdd7d6262ce305c93fa4a8a7b2630e984824))
