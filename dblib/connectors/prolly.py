from __future__ import annotations

from contextlib import closing
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.target import PreparedTarget, SuiteRequest
from dblib.connector import Connector, DatabaseHandle
from dblib import prolly
from dblib.dolt import commit_dolt_schema
from dblib.prolly import ProllyToolSuite
from util.import_db import load_sql_file


class Prolly(Connector):
    """PostgreSQL with the prolly engine (version control built into the
    server): backend prolly. Each database's public schema is made
    VERSIONED, with prolly its tables' default access method."""

    environment = ("PROLLY_",)
    needs_psql = True
    version_sql = "SELECT version()"

    def scope(self) -> dict:
        return {"host": prolly.PROLLY_HOST, "port": prolly.PROLLY_PORT, "user": prolly.PROLLY_USER}

    def _admin(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(ProllyToolSuite.get_default_connection_uri())
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def provision(self, db: str) -> DatabaseHandle:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db}")
        try:
            prolly.prepare_database(ProllyToolSuite.get_initial_connection_uri(db), db)
        except BaseException:
            self.delete(db)
            raise
        return DatabaseHandle(db, "main")

    def load(self, database: DatabaseHandle, dump: Path) -> None:
        uri = ProllyToolSuite.get_initial_connection_uri(database.name)
        load_sql_file(uri, dump)
        commit_dolt_schema(uri)

    def prepare_target(self, database: DatabaseHandle) -> PreparedTarget:
        def open_suite(request: SuiteRequest):
            return ProllyToolSuite.init_for_bench(
                request.result_collector,
                database.name,
                request.initial_ref,
                request.measure_storage,
            )

        return PreparedTarget(database.root_ref, ProllyToolSuite, open_suite)

    def delete(self, db: str) -> None:
        # (FORCE: a worker's session left open by a failed run would
        # otherwise keep the database)
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
        if self.exists(db):
            raise RuntimeError(f"prolly database '{db}' still exists after deletion")

    def exists(self, db: str) -> bool:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db,))
            return cur.fetchone() is not None

    def version(self) -> str:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(self.version_sql)
            return str(cur.fetchone()[0])


CONNECTOR = Prolly
