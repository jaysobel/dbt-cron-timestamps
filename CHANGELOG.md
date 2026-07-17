# Changelog

All notable changes to this project are documented here. The project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2026-07-17

### Changed

- Generate the large exact-match fixture during validation instead of distributing it with the package.
- Replace the credential-shaped CI placeholder with a generic Snowflake external-browser profile.
- Remove personal attribution from the distributed license notice.

### Security

- Audited tracked files, repository history, release contents, and public project metadata for machine-specific paths, credentials, account identifiers, and private organizational context.

## [1.1.0] - 2026-07-17

### Added

- A row-level `cron_start_end_to_timestamps` macro with timezone-aware interval bounds.
- Vixie, contains, union, and intersect day-matching modes.
- Support for Sunday aliases, named months and weekdays, tabs and repeated whitespace, and SQL start-date expressions.
- Independent fixture generation cross-checked with `cronsim` and `croniter`.
- Exact-match and stress integration suites covering 1,128 cron expressions.
- dbt Core and Fusion compatibility parsing in CI.

### Changed

- Hardened parsing and candidate generation while preserving the constructive SQL approach.
- Made the repository safe to install as a macro-only dbt package.
- Documented the supported cron dialect, known divergence, public API, and Snowflake support.

[1.1.1]: https://github.com/jaysobel/dbt-cron-timestamps/releases/tag/1.1.1
[1.1.0]: https://github.com/jaysobel/dbt-cron-timestamps/releases/tag/1.1.0
