"""Verify acknowledged Bolt writes across real server shutdown and process death."""
import json
import hashlib
import shutil
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


def main():
    container_image = sys.argv[2] if sys.argv[1] == "--image" else None
    binary = None if container_image else str(Path(sys.argv[1]).resolve())
    seed = sys.argv[3:] if container_image else sys.argv[2:]
    with tempfile.TemporaryDirectory(prefix="ermya-issue17-") as directory:
        root = Path(directory)
        if seed:
            target = root / "data" / "databases" / "neo4j"
            target.parent.mkdir(parents=True)
            shutil.copytree(seed[0], target)
        cert, key = root / "cert.pem", root / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", str(key), "-out", str(cert), "-days", "1",
                        "-subj", "/CN=localhost"], check=True, capture_output=True)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        uri = f"bolt+ssc://127.0.0.1:{port}"
        env = {k: v for k, v in os.environ.items() if not k.startswith("ERMYA_")}
        env.update(ERMYA_BIND=f"127.0.0.1:{port}", ERMYA_DATA_DIR=str(root / "data"),
                   ERMYA_TLS_CERT=str(cert), ERMYA_TLS_KEY=str(key), ERMYA_NO_AUTH="1",
                   ERMYA_PASSWORD="isolated-test-only-17", ERMYA_VACUUM_INTERVAL_SECONDS="0",
                   ERMYA_QUERIES_MAX_PER_SECOND="0")
        if container_image:
            root.chmod(0o755)
            cert.chmod(0o644)
            key.chmod(0o644)
            data = root / "data"
            data.mkdir(exist_ok=True)
            for entry in [data, *data.rglob("*")]:
                entry.chmod(0o777 if entry.is_dir() else 0o666)
            env.update(ERMYA_BIND="0.0.0.0:7687", ERMYA_DATA_DIR="/test/data",
                       ERMYA_TLS_CERT="/test/cert.pem", ERMYA_TLS_KEY="/test/key.pem")
        expected = []
        baseline = None
        report = []
        with (root / "server.log").open("w+") as log:
            for cycle, stop in enumerate(["terminate", "kill", "terminate", "terminate"]):
                name = "ermya-issue17-" + uuid.uuid4().hex if container_image else None
                command = [binary]
                if container_image:
                    command = ["docker", "run", "--name", name, "--user", f"{os.getuid()}:{os.getgid()}", "-p", f"127.0.0.1:{port}:7687",
                               "-v", f"{root}:/test"]
                    for key_name, value in env.items():
                        if key_name.startswith("ERMYA_"):
                            command.extend(["-e", f"{key_name}={value}"])
                    command.append(container_image)
                server = subprocess.Popen(command, env=env, stdout=log, stderr=log)
                try:
                    for _ in range(200):
                        if server.poll() is not None:
                            raise RuntimeError("server exited while reopening store")
                        try:
                            with GraphDatabase.driver(uri, auth=None, connection_timeout=.2) as driver:
                                with driver.session(database="neo4j") as session:
                                    session.run("RETURN 1").consume()
                            break
                        except (OSError, ServiceUnavailable):
                            time.sleep(.05)
                    else:
                        raise TimeoutError("Bolt readiness")
                    with GraphDatabase.driver(uri, auth=None, max_transaction_retry_time=0) as driver:
                        with driver.session(database="neo4j") as session:
                            existing = sorted([
                                {"id": row["n"].element_id, "labels": sorted(row["n"].labels), "properties": dict(row["n"])}
                                for row in session.run("MATCH (n) RETURN n") if "Issue17" not in row["n"].labels
                            ], key=lambda n: n["id"])
                            fingerprint = hashlib.sha256(json.dumps(existing, sort_keys=True, default=str).encode()).hexdigest()
                            if baseline is None:
                                baseline = fingerprint
                            assert fingerprint == baseline, "preexisting data changed"
                            rows = [r.data() for r in session.run("MATCH (n:Issue17) RETURN n.id AS id, n.payload AS payload ORDER BY n.id")]
                            assert rows == expected, f"content mismatch after restart {cycle}: actual={rows!r}, expected={expected!r}"
                            if cycle < 3:
                                payload = "payload-" * 40
                                with session.begin_transaction() as tx:
                                    tx.run("CREATE (n:Issue17 {id:$id, payload:$payload})", id=cycle, payload=payload).consume()
                                    tx.commit()
                                expected.append({"id": cycle, "payload": payload})
                    report.append({"cycle": cycle, "stop": stop, "verified_nodes": len(rows), "preexisting_nodes": len(existing), "preexisting_sha256": fingerprint, "pass": True})
                    if name:
                        subprocess.run(["docker", "kill", "--signal", "TERM" if stop == "terminate" else "KILL", name],
                                       check=True, capture_output=True, timeout=10)
                    else:
                        getattr(server, stop)()
                    code = server.wait(timeout=20)
                    if stop == "terminate":
                        assert code == 0, f"shutdown exit code {code}"
                except Exception:
                    log.flush()
                    log.seek(0)
                    print(log.read(), file=sys.stderr)
                    raise
                finally:
                    if name:
                        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)
                        # Rootless Docker maps container IDs to subordinate host IDs.
                        # Restore deletable permissions only on this disposable fixture.
                        if cycle == 3 or sys.exc_info()[0] is not None:
                            subprocess.run(["docker", "run", "--rm", "--user", "0", "--entrypoint", "chmod",
                                            "-v", f"{root}:/test", container_image, "-R", "a+rwX", "/test/data"],
                                           check=True, capture_output=True, timeout=15)
                    if server.poll() is None:
                        server.kill()
                        server.wait(timeout=10)
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
