"""The prolly adapter's differences from Doltgres', against a fake connection."""

from unittest import mock

import pytest

from dblib.db_api import DBToolSuite
from dblib.prolly import ProllyToolSuite


def suite_answering(answers: dict) -> tuple[ProllyToolSuite, list]:
    """A suite whose queries are answered by the first key each starts with."""
    suite = object.__new__(ProllyToolSuite)
    seen = []

    def execute(self, query, vars=None):
        seen.append((query, vars))
        for prefix, rows in answers.items():
            if query.startswith(prefix):
                return rows
        return []

    patcher = mock.patch.object(DBToolSuite, "_execute", execute)
    patcher.start()
    return suite, seen, patcher


@pytest.fixture
def answering():
    patchers = []

    def make(answers):
        suite, seen, patcher = suite_answering(answers)
        patchers.append(patcher)
        return suite, seen

    yield make
    for p in patchers:
        p.stop()


def test_flush_analyzes_only_tables_never_analyzed(answering) -> None:
    suite, seen = answering({"SELECT c.relname FROM pg_class": [("customer",), ("new_order",)]})
    suite._flush_impl()
    assert [q for q, _ in seen if q.startswith("ANALYZE")] == ['ANALYZE "customer";', 'ANALYZE "new_order";']


def test_conflict_tables_drop_the_schema_prefix(answering) -> None:
    suite, _ = answering({"SELECT table_name, current_schema()": [("public.task", "public"), ("other", "public")]})
    assert suite._conflict_tables() == ["task", "other"]


def test_conflict_rows_spread_each_side_into_dolt_columns(answering) -> None:
    suite, _ = answering({
        "SELECT row_to_json(base_row)": [('{"id": 1, "v": "a"}', '{"id": 1, "v": "b"}', None,
                                          "modified", "removed")],
        "SELECT column_name FROM information_schema.columns": [("id",), ("v",)],
    })
    assert suite._conflict_rows("task") == [{
        "base_id": 1, "base_v": "a", "our_id": 1, "our_v": "b", "their_id": None, "their_v": None,
        "our_diff_type": "modified", "their_diff_type": "removed"}]


def test_a_rebase_step_stopped_by_conflicts_raises(answering) -> None:
    suite, _ = answering({"SELECT dolt_rebase('--continue')": [("{1,\"data conflict in task\"}",)]})
    with mock.patch("dblib.prolly._cells", return_value=["1", "data conflict in task"]):
        with pytest.raises(RuntimeError, match="rebase conflict"):
            suite._execute("SELECT dolt_rebase('--continue');")
