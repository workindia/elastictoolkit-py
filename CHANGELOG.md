# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),

## [Unreleased]

## 0.3.0 (2024-12-20)
### Changed
- `allowed_engine_cls` to `allowed_engine_cls_name` for class name based validation in `CustomMatchDirective`
### Added
- Support for ScriptMatchDirective based on provided `mandatory_params_keys`
- `TextMatchDirective` for text based search
- Support for adding extra-args in generated DSL Query


## 0.2.0 (2024-12-11)
### Changed
- Updated value parser and null-checks while query generation

## 0.1.0 (2024-09-19)
### Added
- Initial Release
