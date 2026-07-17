# Contributing

Issues and pull requests are welcome. Changes to cron semantics should include a named fixture that makes the intended dialect behavior explicit.

## Development setup

Install [uv](https://docs.astral.sh/uv/), then create the locked development environment:

```shell
uv sync
```

Regenerate the reference fixtures and verify agreement across the independent implementations:

```shell
uv run python integration_tests/generate_fixtures.py
git diff --exit-code -- integration_tests/seeds
```

Parse the downstream integration project:

```shell
uv run dbt deps --project-dir integration_tests --profiles-dir integration_tests/ci_profiles
uv run dbt parse --project-dir integration_tests --profiles-dir integration_tests/ci_profiles
```

## Snowflake integration tests

Use a Snowflake profile with permission to create tables and views in an isolated development schema:

```shell
dbt deps --project-dir integration_tests --profile <your_snowflake_profile>
dbt seed --project-dir integration_tests --profile <your_snowflake_profile> --full-refresh
dbt test --project-dir integration_tests --profile <your_snowflake_profile>
```

Run the full Snowflake suite before merging changes that affect SQL generation or before creating a release.

## Public API and releases

The two namespaced macros documented in the README are the stable public API. Follow Semantic Versioning when changing their arguments, output columns, or documented behavior, and update `CHANGELOG.md` with every release.
