"""Generate deterministic cross-implementation fixtures for the dbt package.

The supported dialect is five-field Vixie cron, including its historical rule
that day-of-month and day-of-week are ANDed when either field starts with `*`.
The fixture is accepted only when an intentionally simple reference evaluator,
croniter, and cronsim all produce the same timestamps, apart from explicitly
documented dialect differences.
"""

from __future__ import annotations

import csv
import hashlib
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

from croniter import CroniterBadDateError, croniter
from cronsim import CronSim, CronSimError


START = datetime(2024, 1, 1)
END = datetime(2025, 1, 1)
RANDOM_SEED = 20240717

# `N/step` is a non-standard extension: Cronie rejects it unless N is `*` or
# an explicit range. For the day-of-week field, cronsim and this package expand
# `1/2` over the literal 0-7 field range (1,3,5,7), while croniter normalizes
# Sunday first and stops at 5.
KNOWN_DIALECT_DIFFERENCES = {"1 15 * * 1/2"}

MONTH_NAMES = {
    name: index
    for index, name in enumerate(
        ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"),
        start=1,
    )
}
WEEKDAY_NAMES = {
    name: index
    for index, name in enumerate(("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"))
}


CURATED_EXPRESSIONS = (
    "0 0 * * *",
    "59 23 31 12 *",
    "*/17 1-23/7 * * *",
    "5,17,41 2,11,20 * * *",
    "7 3 1,15 * *",
    "11 4 29 FEB *",
    "13 5 30 APR,JUN,SEP,NOV *",
    "19 6 * JAN,MAR-APR,DEC *",
    "23 7 * * SUN",
    "29 8 * * 7",
    "31 9 * * 0-6",
    "37 10 * * 0-7",
    "41 11 * * 1-7",
    "43 12 * * MON-SAT",
    "47 13 * * sun,tUe,THU",
    "53 14 * * */2",
    "1 15 * * 1/2",
    "2 16 1 * MON",
    "3 17 1-7 * TUE",
    "4 18 */32,1-7 * WED",
    "5 19 1,*/2 * THU",
    "6 20 */5 * 1-7",
    "7 21 10-20/3 2-11/3 FRI-SAT",
    "8 22 28-31 * SUN",
    "9 0 1-31/10 JAN-DEC/2 0,7",
    "10 1 1,10,20,30 * 1,3,5",
    "11 2 */7 */2 */3",
    "12 3 2/9 2/3 2/2",
    "13 4 1-5,20-25/2 MAR-MAY MON-WED",
    "14 5 1-31/4 JUL-DEC/2 WED-FRI/2",
    "  17   6\t*   *\tMON  ",
    "18 7 1,*/10 * TUE",
)


def _parse_value(raw: str, names: dict[str, int]) -> int:
    return names.get(raw.upper(), int(raw) if raw.isdigit() else -1)


def _expand_field(
    expression: str,
    minimum: int,
    maximum: int,
    names: dict[str, int] | None = None,
    normalize_sunday: bool = False,
) -> set[int]:
    names = names or {}
    values: set[int] = set()

    for item in expression.upper().split(","):
        base, separator, step_text = item.partition("/")
        step = int(step_text) if separator else 1

        if base == "*":
            range_start, range_end = minimum, maximum
        elif "-" in base:
            start_text, end_text = base.split("-", maxsplit=1)
            range_start = _parse_value(start_text, names)
            range_end = _parse_value(end_text, names)
        else:
            range_start = _parse_value(base, names)
            range_end = maximum if separator else range_start

        if not minimum <= range_start <= range_end <= maximum or step <= 0:
            raise ValueError(f"Unsupported fixture field: {expression}")
        values.update(range(range_start, range_end + 1, step))

    if normalize_sunday:
        return {value % 7 for value in values}
    return values


def reference_occurrences(expression: str) -> list[datetime]:
    """Evaluate the documented dialect without using either comparison package."""

    minute, hour, day_of_month, month, day_of_week = expression.upper().split()
    minutes = _expand_field(minute, 0, 59)
    hours = _expand_field(hour, 0, 23)
    days = _expand_field(day_of_month, 1, 31)
    months = _expand_field(month, 1, 12, MONTH_NAMES)
    weekdays = _expand_field(day_of_week, 0, 7, WEEKDAY_NAMES, normalize_sunday=True)
    intersect_days = day_of_month.startswith("*") or day_of_week.startswith("*")

    occurrences: list[datetime] = []
    current_date = START.date()
    while current_date < END.date():
        python_weekday_as_cron = (current_date.weekday() + 1) % 7
        matches_dom = current_date.day in days
        matches_dow = python_weekday_as_cron in weekdays
        matches_day = matches_dom and matches_dow if intersect_days else matches_dom or matches_dow
        if current_date.month in months and matches_day:
            occurrences.extend(
                datetime.combine(current_date, time(hour_value, minute_value))
                for hour_value in sorted(hours)
                for minute_value in sorted(minutes)
            )
        current_date += timedelta(days=1)
    return occurrences


def croniter_occurrences(expression: str) -> list[datetime]:
    iterator = croniter(
        expression,
        START - timedelta(minutes=1),
        ret_type=datetime,
        implement_cron_bug=True,
    )
    occurrences: list[datetime] = []
    while True:
        try:
            occurrence = iterator.get_next(datetime)
        except CroniterBadDateError:
            return occurrences
        if occurrence >= END:
            return occurrences
        occurrences.append(occurrence)


def cronsim_occurrences(expression: str) -> list[datetime]:
    iterator = CronSim(expression, START - timedelta(minutes=1))
    occurrences: list[datetime] = []
    while True:
        try:
            occurrence = next(iterator)
        except StopIteration:
            return occurrences
        if occurrence >= END:
            return occurrences
        occurrences.append(occurrence)


def _random_term(rng: random.Random, minimum: int, maximum: int) -> str:
    mode = rng.choice(("single", "single", "list", "range", "range_step", "star_step"))
    if mode == "single":
        return str(rng.randint(minimum, maximum))
    if mode == "list":
        return ",".join(str(value) for value in sorted(rng.sample(range(minimum, maximum + 1), 2)))
    if mode == "star_step":
        return f"*/{rng.randint(2, min(11, maximum - minimum + 1))}"
    start = rng.randint(minimum, maximum - 1)
    end = rng.randint(start + 1, maximum)
    if mode == "range_step":
        return f"{start}-{end}/{rng.randint(2, min(7, end - start + 1))}"
    return f"{start}-{end}"


def random_expressions(count: int = 96) -> list[str]:
    rng = random.Random(RANDOM_SEED)
    expressions: set[str] = set()
    while len(expressions) < count:
        # Keep generated schedules reasonably sparse so a full leap-year fixture
        # remains reviewable while still varying every field independently.
        minute = _random_term(rng, 0, 59)
        hour = _random_term(rng, 0, 23)
        if len(_expand_field(minute, 0, 59)) * len(_expand_field(hour, 0, 23)) > 24:
            continue
        day = rng.choice(("*", _random_term(rng, 1, 31), f"1,*/{rng.randint(2, 12)}"))
        month = rng.choice(("*", _random_term(rng, 1, 12)))
        weekday = rng.choice(("*", _random_term(rng, 0, 7)))
        expression = f"{minute} {hour} {day} {month} {weekday}"
        try:
            CronSim(expression, START)
        except CronSimError:
            continue
        expressions.add(expression)
    return sorted(expressions)


def validated_occurrences(expression: str) -> list[datetime]:
    expected = reference_occurrences(expression)
    croniter_result = croniter_occurrences(expression)
    cronsim_result = cronsim_occurrences(expression)
    croniter_matches = expected == croniter_result
    if (not croniter_matches and expression not in KNOWN_DIALECT_DIFFERENCES) or expected != cronsim_result:
        raise AssertionError(
            f"Cross-implementation disagreement for {expression!r}: "
            f"reference={len(expected)}, croniter={len(croniter_result)}, "
            f"cronsim={len(cronsim_result)}"
        )
    if croniter_matches and expression in KNOWN_DIALECT_DIFFERENCES:
        raise AssertionError(f"Remove resolved croniter exception for {expression!r}")
    return expected


def write_fixtures() -> None:
    seed_directory = Path(__file__).parent / "seeds"
    seed_directory.mkdir(exist_ok=True)
    expressions = list(dict.fromkeys((*CURATED_EXPRESSIONS, *random_expressions())))

    with (seed_directory / "cron_cases.csv").open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(("cron",))
        writer.writerows((expression,) for expression in expressions)

    with (seed_directory / "cron_expected.csv").open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(("cron", "trigger_at_utc"))
        for expression in expressions:
            writer.writerows(
                (expression, occurrence.isoformat(sep=" "))
                for occurrence in validated_occurrences(expression)
            )

    with (seed_directory / "cron_stress.csv").open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(("cron", "expected_count", "expected_md5"))
        for expression in random_expressions(count=1_000):
            occurrences = validated_occurrences(expression)
            serialized = "|".join(occurrence.isoformat(sep=" ") for occurrence in occurrences)
            writer.writerow(
                (
                    expression,
                    len(occurrences),
                    hashlib.md5(serialized.encode(), usedforsecurity=False).hexdigest(),
                )
            )


if __name__ == "__main__":
    write_fixtures()
