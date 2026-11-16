"""Official sync/async driver regression for BEGIN before any RUN.

uv run --no-project --with 'neo4j>=5,<7' python scripts/test_issue10_bolt.py <binary>
"""
import asyncio
import sys
from neo4j import GraphDatabase, AsyncGraphDatabase
from neo4j.exceptions import Neo4jError
from test_issue14_bolt import main


def probe(uri):
    report = {}
    for database in ["neo4j", None]:
        name = "explicit_database" if database else "default_database"
        try:
            with GraphDatabase.driver(uri, auth=None, max_transaction_retry_time=0) as driver:
                with driver.session(database=database) as session:
                    with session.begin_transaction() as tx:
                        row = tx.run("RETURN $value AS value", value=42).single()
                        tx.commit()
                    report[name] = {"pass": row["value"] == 42}
        except Neo4jError as error:
            report[name] = {"pass": False, "code": error.code}

    async def async_query():
        async with AsyncGraphDatabase.driver(uri, auth=None, max_transaction_retry_time=0) as driver:
            records, _, _ = await driver.execute_query("RETURN $value AS value", value=43, database_="neo4j")
            return records[0]["value"] == 43
    try:
        report["async_execute_query"] = {"pass": asyncio.run(async_query())}
    except Neo4jError as error:
        report["async_execute_query"] = {"pass": False, "code": error.code}
    return report


if __name__ == "__main__":
    sys.exit(main(probe_fn=probe))
