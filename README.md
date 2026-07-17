# dbt Cron to Timestamps

[![CI](https://github.com/jaysobel/dbt-cron-timestamps/actions/workflows/ci.yml/badge.svg)](https://github.com/jaysobel/dbt-cron-timestamps/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Generate matching timestamps from five-field cron expressions in pure Snowflake SQL.

The macros are constructive: they expand the values selected by each cron field and combine only those candidates. They do not generate every minute in the requested date range and filter it afterward.

## Supported dialect

This package supports Snowflake. Its implementation intentionally uses Snowflake-native SQL to construct candidate timestamps efficiently rather than scanning every minute in the requested range.

The package targets the Vixie/ISC cron behavior documented by [Crontab Guru](https://crontab.guru/), using the [Open Cron Pattern Specification 1.0](https://github.com/open-source-cron/ocps/blob/main/specifications/OCPS-1.0.md) as a reference for the core five-field grammar:

- five fields: minute, hour, day of month, month, day of week;
- wildcards, lists, ranges, and steps (`*`, `,`, `-`, `/`);
- case-insensitive three-letter month and weekday names;
- either `0` or `7` for Sunday, including their use in ranges and steps;
- a compatibility extension for open-ended steps such as `5/10` (from `5` through the field maximum);
- one or more spaces or tabs between fields; and
- the historical Vixie rule for combining day of month and day of week.

Seconds, year fields, aliases such as `@daily`, and the Quartz-style `L`, `W`, `#`, and `?` modifiers are not supported.

The macros evaluate valid schedules; they are not cron validators. Malformed or out-of-range expressions can raise a Snowflake conversion error or return no matches.

### Day matching

The default `day_match_mode='vixie'` reproduces the daemon behavior precisely:

- if either day field starts with `*`, day of month and day of week are intersected (`AND`);
- otherwise, the two fields are unioned (`OR`).

The first-character check is intentional. It preserves the long-standing behavior described in [Crontab Guru's cron bug article](https://crontab.guru/cron-bug.html).

Other modes are available when consuming a different dialect:

- `contains`: intersect if `*` appears anywhere in either day field;
- `intersect`: always use `AND`; or
- `union`: always use `OR`.

## Installation

Add the package to `packages.yml`:

```yaml
packages:
  - git: https://github.com/jaysobel/dbt-cron-timestamps.git
    revision: 1.1.0
```

Then run `dbt deps`.

The `main` branch contains unreleased development. Pin a release tag so package installation remains reproducible.

## Public API

The package exposes two public macros:

- `dbt_cron_timestamps.cron_to_timestamps`
- `dbt_cron_timestamps.cron_start_end_to_timestamps`

Other implementation details are not considered part of the stable API.

## Generate timestamps for a date range

`cron_to_timestamps` reads cron expressions from a preceding CTE. It returns distinct `cron, trigger_at_utc` pairs in the half-open range `[start_date, start_date + days_forward)`.

```sql
with crons as (
  select cron_code as cron
  from {{ ref('schedules') }}
)

, cron_timestamps as (
  {{ dbt_cron_timestamps.cron_to_timestamps(
      'crons',
      'cron',
      'current_date',
      days_forward=60,
      day_match_mode='vixie'
  ) }}
)

select *
from cron_timestamps
```

For a literal date, pass the ISO string directly (`'2024-01-01'` as the Jinja argument). SQL date expressions such as `dateadd('day', -10, current_date)` are also accepted.

## Generate timestamps for row-level intervals

`cron_start_end_to_timestamps` applies a separate half-open `[start_at, end_at)` range to each source row and returns the source identifier with every match.

```sql
with schedule_versions as (
  select schedule_id, cron, start_at, end_at
  from {{ ref('schedule_versions') }}
)

, cron_timestamps as (
  {{ dbt_cron_timestamps.cron_start_end_to_timestamps(
      'schedule_versions',
      'cron',
      'start_at',
      'end_at',
      unique_id='schedule_id',
      max_date_range=1095,
      day_match_mode='vixie',
      input_timezone='UTC'
  ) }}
)

select *
from cron_timestamps
```

`input_timezone` is the IANA timezone used to interpret timezone-naive bounds before converting them to UTC. It defaults to `UTC`. `max_date_range` is a safety limit for the number of days fanned out per interval.

## Correctness testing

The committed exact-match fixture covers 128 curated and deterministically randomized expressions over leap year 2024, totaling 67,165 expected timestamps. A second 1,000-expression stress fixture compares per-expression counts and ordered timestamp fingerprints. Fixture expectations must agree between:

1. a small independent reference evaluator;
2. `cronsim`, which targets Debian cron behavior; and
3. `croniter` with `implement_cron_bug=True`.

There is one explicit dialect divergence: `1/2` in the day-of-week field. Cronie rejects this non-standard open-ended-step syntax. Of the libraries that accept it, `cronsim` and this package expand over the literal `0-7` range to `1,3,5,7` (including Sunday), while `croniter` stops at `5`. The choice is pinned by a named fixture rather than treated as portable cron behavior.

Generate and cross-check fixtures:

```shell
uv run python integration_tests/generate_fixtures.py
```

Run the Snowflake suite:

```shell
dbt deps --project-dir integration_tests --profile <your_snowflake_profile>
dbt seed --project-dir integration_tests --profile <your_snowflake_profile> --full-refresh
dbt test --project-dir integration_tests --profile <your_snowflake_profile>
```

The integration project installs the repository root as a local package. CI regenerates the independent fixtures and parses the integration project across supported dbt runtimes; maintainers run the Snowflake execution suite before a release.

The Snowflake tests compare exact timestamp sets and also exercise:

- Sunday `0`/`7` aliases in ranges and steps;
- Vixie and `contains` day modes;
- spaces and tabs between fields;
- leap day and month-name parsing;
- row-level start/end boundaries and timezone handling; and
- non-default Snowflake `WEEK_START` values.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the local workflow and release checks. Release history is recorded in [CHANGELOG.md](CHANGELOG.md).
