"""Black-box transactional integrity regressions against an isolated server.

Run with: uv run --no-project --with neo4j==4.4.11 python
scripts/test_issue14_bolt.py target/debug/ermya-graph-server
Or use --image ermya-graph-community:0.13.2 instead of a binary path.
"""

import concurrent.futures
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable


def probe(uri):
    def driver():
        return GraphDatabase.driver(uri, auth=None, max_transaction_retry_time=0)

    def query(text, **params):
        with driver() as d, d.session(database="neo4j") as s:
            return [r.data() for r in s.run(text, params)]

    def compete(label, updating):
        barrier = threading.Barrier(2)

        def worker(value):
            try:
                with driver() as d, d.session(database="neo4j") as s:
                    s.run("RETURN 1 AS selected").consume()
                    with s.begin_transaction() as tx:
                        if updating:
                            rows = list(tx.run(f"MATCH (n:{label}) RETURN n.version AS version"))
                            assert [r["version"] for r in rows] == [0], rows
                            barrier.wait(timeout=10)
                            tx.run(
                                f"MATCH (n:{label}) WHERE n.version = $expected "
                                "SET n.version = $updated RETURN n",
                                expected=0, updated=value,
                            ).consume()
                        else:
                            tx.run(f"CREATE (n:{label} {{key:'same'}}) RETURN n").consume()
                        barrier.wait(timeout=10)
                        tx.commit()
                return {"committed": True, "value": value}
            except Neo4jError as error:
                barrier.abort()
                return {"committed": False, "code": error.code, "message": error.message}
            except Exception:
                barrier.abort()
                raise

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(worker, [1, 2]))

    report = {}
    unique = "Issue14Unique" + uuid.uuid4().hex
    query(f"CREATE CONSTRAINT ON (n:{unique}) ASSERT n.key IS UNIQUE")
    clients = compete(unique, False)
    rows = query(f"MATCH (n:{unique}) RETURN n.key AS key")
    losers = [c for c in clients if not c["committed"]]
    report["unique"] = {
        "clients": clients, "rows": rows,
        "pass": len(rows) == 1 and len(losers) == 1
        and losers[0]["code"] == "Neo.ClientError.Schema.ConstraintValidationFailed",
    }

    cas = "Issue14Cas" + uuid.uuid4().hex
    query(f"CREATE (n:{cas} {{version:0}})")
    clients = compete(cas, True)
    rows = query(f"MATCH (n:{cas}) RETURN n.version AS version")
    winners = [c for c in clients if c["committed"]]
    losers = [c for c in clients if not c["committed"]]
    report["cas"] = {
        "clients": clients, "rows": rows,
        "pass": len(winners) == 1 and len(losers) == 1
        and losers[0]["code"].startswith("Neo.TransientError.Transaction.")
        and rows == [{"version": winners[0]["value"]}],
    }

    deletion = "Issue14Delete" + uuid.uuid4().hex
    with driver() as d, d.session(database="neo4j") as s:
        s.run("RETURN 1 AS selected").consume()
        with s.begin_transaction() as tx:
            tx.run(f"CREATE (n:{deletion} {{key:'delete-me'}})").consume()
            tx.commit()
    assert query(f"MATCH (n:{deletion}) RETURN n.key AS key") == [{"key": "delete-me"}]
    try:
        query(f"MATCH (n:{deletion}) DELETE n")
        rows = query(f"MATCH (n:{deletion}) RETURN n.key AS key")
        report["delete"] = {"pass": rows == [], "rows": rows}
    except Neo4jError as error:
        report["delete"] = {"pass": False, "code": error.code, "message": error.message}
    return report


def main():
    container_image = sys.argv[2] if sys.argv[1] == "--image" else None
    binary = None if container_image else str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="ermya-issue14-") as directory:
        root = Path(directory)
        cert, key = root / "cert.pem", root / "key.pem"
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(cert), "-days", "1",
            "-subj", "/CN=localhost",
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        env = {k: v for k, v in os.environ.items() if not k.startswith("ERMYA_")}
        env.update({
            "ERMYA_BIND": f"127.0.0.1:{port}",
            "ERMYA_DATA_DIR": str(root / "data"),
            "ERMYA_TLS_CERT": str(cert), "ERMYA_TLS_KEY": str(key),
            "ERMYA_NO_AUTH": "1", "ERMYA_PASSWORD": "isolated-test-only-14",
            "ERMYA_VACUUM_INTERVAL_SECONDS": "0", "ERMYA_QUERIES_MAX_PER_SECOND": "0",
        })
        container_name = "ermya-issue14-" + uuid.uuid4().hex if container_image else None
        command = [binary]
        if container_image:
            # Ephemeral self-signed test key, readable by the image's non-root user.
            root.chmod(0o755)
            cert.chmod(0o644)
            key.chmod(0o644)
            command = ["docker", "run", "--rm", "--name", container_name,
                       "-p", f"127.0.0.1:{port}:7687",
                       "-v", f"{root}:/test-tls:ro"]
            env.update({"ERMYA_BIND": "0.0.0.0:7687",
                        "ERMYA_DATA_DIR": "/var/lib/ermya",
                        "ERMYA_TLS_CERT": "/test-tls/cert.pem",
                        "ERMYA_TLS_KEY": "/test-tls/key.pem"})
            for name, value in env.items():
                if name.startswith("ERMYA_"):
                    command.extend(["-e", f"{name}={value}"])
            command.append(container_image)
        with (root / "server.log").open("w+") as log:
            server = subprocess.Popen(command, env=env, stdout=log, stderr=log)
            try:
                for _ in range(200):
                    if server.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(log.read())
                    try:
                        # Docker's port proxy can listen before the server is ready.
                        # Require a complete TLS/Bolt handshake before the test starts.
                        with GraphDatabase.driver(f"bolt+ssc://127.0.0.1:{port}", auth=None,
                                                  connection_timeout=0.2) as driver:
                            with driver.session(database="neo4j") as session:
                                session.run("RETURN 1 AS ready").consume()
                        break
                    except (OSError, ServiceUnavailable):
                        time.sleep(0.05)
                else:
                    raise TimeoutError("isolated server did not listen")
                report = probe(f"bolt+ssc://127.0.0.1:{port}")
                if container_image:
                    metadata = json.loads(subprocess.check_output(
                        ["docker", "image", "inspect", container_image]))[0]
                    version = metadata["Config"]["Labels"]["org.opencontainers.image.version"]
                    with GraphDatabase.driver(f"bolt+ssc://127.0.0.1:{port}", auth=None) as driver:
                        with driver.session(database="neo4j") as session:
                            agent = session.run("RETURN 1 AS ready").consume().server.agent
                    report["image"] = {"pass": bool(version) and agent == f"Neo4j/{version}",
                                       "version": version, "agent": agent,
                                       "image_id": metadata["Id"],
                                       "source_revision": metadata["Config"]["Labels"]["org.opencontainers.image.revision"]}
                print(json.dumps(report, indent=2))
                return 0 if all(case["pass"] for case in report.values()) else 1
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                if container_name:
                    subprocess.run(["docker", "stop", "--time", "5", container_name],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   timeout=15, check=False)
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


if __name__ == "__main__":
    sys.exit(main())
