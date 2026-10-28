"""PostgreSQL prolly backend: PostgreSQL with Dolt's version control,
built into the server (tables USING prolly in a VERSIONED schema).

It speaks the Postgres wire protocol and the same dolt_* SQL functions as
Doltgres, so it is the Doltgres backend (dblib/dolt.py) but for:

- setup: the benchmark database's public schema is made VERSIONED and
  prolly made its default table access method, so the generated schema's
  plain CREATE TABLEs make versioned prolly tables, and its history window
  is set to hold the whole run (HISTORY_WINDOW): every commit in it, each
  with a page root;
- conflicts: committing them is the dolt.allow_commit_conflicts setting,
  and a table's conflict rows come from dolt_conflicts(NULL::table), each
  side a whole row (base_row, our_row, their_row) rather than Dolt's
  base_*/our_*/their_* columns, into which they are spread here;
- storage: pg_database_size(), which counts every file of the database
  (its tables, their page store and its chunk store), over SQL;
- analysis: tables never analyzed are ANALYZEd at each flush (the runners
  flush once the seed is committed, before the workload starts), so its
  first queries aren't planned without statistics;
- multi-branch statements read another branch's tables by revision name,
  "<db>/<ref>".public.<table>, as Doltgres does: a branch's working set,
  or a commit's tables. Reads only; a script that writes to another
  branch is refused by the server.
"""

import json
import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib import util as dbutil
from dblib.dolt import DoltToolSuite, _cells

PROLLY_USER = os.environ.get("PROLLY_USER", "postgres")
PROLLY_PASSWORD = os.environ.get("PROLLY_PASSWORD", "password")
PROLLY_HOST = os.environ.get("PROLLY_HOST") or os.environ.get("DBFORK_SERVER_HOST", "localhost")
PROLLY_PORT = int(os.environ.get("PROLLY_PORT", "5432"))


# The history window every run is measured in, as vc_bench sets it: a span
# far longer than any run, so every commit a run makes stays in it; a page
# root kept at every commit, so a move to one gives its pages back rather
# than writing rows; no cap on the roots or the chunk store, and nothing
# severed in the background. Set on the database before any history is
# made, so they hold whatever the server's defaults are.
HISTORY_WINDOW = {
    "prolly.history_window": "100 years",
    "prolly.history_window_action": "none",
    "prolly.history_max_size": "",
    "prolly.history_root_max_size": "",
    "prolly.root_interval": "0",
    "prolly.root_interval_commits": "1",
}


def prepare_database(db_uri: str, db_name: str) -> None:
    """Make a new, empty database a prolly one: its public schema
    VERSIONED, prolly the default access method of its tables, and the
    history window (HISTORY_WINDOW) set to hold the whole run."""
    conn = psycopg2.connect(db_uri)
    try:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute("SELECT nspversioned FROM pg_namespace WHERE nspname = 'public'")
            row = cur.fetchone()
            if not (row and row[0]):
                cur.execute("DROP SCHEMA IF EXISTS public")
                cur.execute("CREATE SCHEMA public VERSIONED")
            cur.execute(f'ALTER DATABASE "{db_name}" SET default_table_access_method = prolly')
            for name, value in HISTORY_WINDOW.items():
                cur.execute(f'ALTER DATABASE "{db_name}" SET {name} = %s', (value,))
    finally:
        conn.close()


class ProllyToolSuite(DoltToolSuite):
    BACKEND_NAME = "prolly"
    SUPPORTS_MULTI_REF_EXEC = True

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            PROLLY_USER, PROLLY_PASSWORD, PROLLY_HOST, PROLLY_PORT, "postgres"
        )

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(
            PROLLY_USER, PROLLY_PASSWORD, PROLLY_HOST, PROLLY_PORT, db_name
        )

    def _storage_bytes(self) -> int:
        """0 once the database is dropped; read over a connection of its
        own when the session's is closed (after the run's cleanup)."""
        if not self.db_name:
            return 0
        query = ("SELECT COALESCE((SELECT pg_database_size(oid) FROM pg_database "
                 "WHERE datname = %s), 0);")
        if self.conn is not None and not self.conn.closed:
            return int(self._execute(query, (self.db_name,))[0][0])
        conn = psycopg2.connect(self.get_default_connection_uri())
        try:
            with conn.cursor() as cur:
                cur.execute(query, (self.db_name,))
                return int(cur.fetchone()[0])
        finally:
            conn.close()

    def _flush_impl(self) -> None:
        """ANALYZE the tables never analyzed (reltuples < 0) on the current
        ref. The runners flush once the seed is committed, so this is where
        the seed's tables get their first statistics, before the workload
        plans its first queries. Without them a join of CH-benCHmark's
        new_order, orders and customer estimates one row, and the nested
        loop it picks runs for many minutes (coffeegoddd/postgres#1055).
        The file_copy backend does the same before its clones. A later
        flush analyzes only tables made since."""
        rows = self._execute(
            "SELECT c.relname FROM pg_class c WHERE c.relkind = 'r' AND c.reltuples < 0 "
            "AND c.relnamespace = 'public'::regnamespace ORDER BY 1;") or []
        for (t,) in rows:
            self._execute(f'ANALYZE "{t}";')


    # -- rebase -------------------------------------------------------------

    def _execute(self, query: str, vars=None):
        """A rebase step that stops at a data conflict raises it, as
        Dolt's does: committing conflicts allowed, prolly's
        dolt_rebase('--continue') returns status 1 and leaves the rebase
        paused on them instead, which Dolt's rebase loop (resolve, then
        continue) would take for the end of the rebase."""
        rows = super()._execute(query, vars)
        if rows and query.startswith("SELECT dolt_rebase('--continue')"):
            cells = _cells(rows[0][0])
            if cells and str(cells[0]) == "1" and "conflict" in str(cells[-1]).lower():
                raise RuntimeError(f"rebase conflict: {cells[-1]}")
        return rows

    def _rebase_impl(self, ref, onto, on_conflict="ours") -> dict:
        """Dolt's, but a rebase that fails is aborted: Dolt aborts one
        stopped by a schema conflict itself, prolly leaves it under way
        (its dolt_rebase_<branch> branch), and the branch's next rebase
        would find it."""
        try:
            return super()._rebase_impl(ref, onto, on_conflict)
        except Exception:
            rows = self._execute("SELECT count(*) FROM dolt_branches WHERE name = %s;",
                                 (f"dolt_rebase_{ref.branch}",))
            if rows and int(rows[0][0]):
                self._abort_rebase()
            raise

    # -- conflicts ----------------------------------------------------------

    def _allow_conflicts(self) -> None:
        if not getattr(self, "_conflicts_allowed", False):
            self._execute("SET dolt.allow_commit_conflicts = on;")
            self._conflicts_allowed = True

    def _conflict_tables(self) -> list:
        """The tables with conflicts, named as Dolt names them: without
        the schema (prolly's names are qualified), the scenarios' SQL
        using the name as given."""
        rows = self._execute(
            "SELECT table_name, current_schema() FROM dolt_conflicts();")
        out = []
        for name, schema in rows or []:
            prefix = f"{schema}."
            out.append(name[len(prefix):] if name.startswith(prefix) else name)
        return out

    def _conflict_rows(self, table: str) -> list:
        """The table's conflict rows in Dolt's shape: each side's columns
        as base_<col>, our_<col> and their_<col> (NULL where the side has
        no row), and the two diff types."""
        rows = self._execute(
            "SELECT row_to_json(base_row), row_to_json(our_row), row_to_json(their_row), "
            f'our_diff_type, their_diff_type FROM dolt_conflicts(NULL::"{table}");')
        cols = [r[0] for r in self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND table_schema = current_schema() "
            "ORDER BY ordinal_position;", (table,))]
        out = []
        for base, ours, theirs, our_type, their_type in rows or []:
            row = {}
            for side, value in (("base", base), ("our", ours), ("their", theirs)):
                value = json.loads(value) if isinstance(value, str) else (value or {})
                for c in cols:
                    row[f"{side}_{c}"] = value.get(c)
            row["our_diff_type"] = our_type
            row["their_diff_type"] = their_type
            out.append(row)
        return out

    def _mark_resolved(self, session, table: str) -> None:
        # (--ours keeps the working set's rows: what the callable left)
        session.sql("SELECT dolt_conflicts_resolve('--ours', %s)", (table,))

    def _resolve_table(self, table: str, flag: str, info: dict) -> None:
        try:
            self._execute("SELECT dolt_conflicts_resolve(%s, %s);", (flag, table))
        except Exception as e:
            if "schema" not in str(e).lower():
                raise
            if flag == "--theirs":
                self._resolve_theirs_by_hand(table)
                info.setdefault("theirs_by_hand", []).append(table)
            else:
                info.setdefault("fallback_ours", []).append(table)
            self._execute("SELECT dolt_conflicts_resolve('--ours', %s);", (table,))

    def _resolve_theirs_by_hand(self, table: str) -> None:
        """Dolt's, reading the conflict rows through _conflict_rows()."""
        rows = self._conflict_rows(table)
        saved = self._rows_as_dicts
        self._rows_as_dicts = lambda query, vars=None: rows
        try:
            super()._resolve_theirs_by_hand(table)
        finally:
            self._rows_as_dicts = saved
