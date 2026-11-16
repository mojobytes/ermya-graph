"""Read-only store inventory; prepare a separate, explicitly unapproved recovery copy."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import struct
import tempfile
import zlib

SIZE, HEADER, SLOT = 4096, 16, 128


def digest(data):
    return hashlib.sha256(data).hexdigest()


def recovery_plan(current, historical):
    plan = {"status": "ready", "replacements": [],
            "quarantined": current["orphans"], "unresolved": list(current.get("issues", []))}
    if current["wal_bytes"]:
        plan["unresolved"].append("WAL requires engine recovery and reconciliation")
    for record in current["live"]:
        if record["sha256"] is not None:
            continue
        choices = [r for r in historical if (r["kind"], r["id"]) == (record["kind"], record["id"])
                   and r["sha256"] is not None and r.get("identity") == record.get("identity")]
        if len({r["sha256"] for r in choices}) != 1:
            plan["unresolved"].append({"kind": record["kind"], "id": record["id"],
                                       "reason": "no unique historical candidate"})
        else:
            plan["replacements"].append(choices[0])
    if plan["unresolved"]:
        plan["status"] = "blocked"
    elif plan["replacements"] or plan["quarantined"]:
        plan["status"] = "needs_decision"
    return plan


def checked_page(data, page_id):
    page = data[page_id * SIZE:(page_id + 1) * SIZE]
    if len(page) != SIZE or zlib.crc32(page[HEADER:]) != struct.unpack_from("<I", page, 8)[0]:
        raise ValueError(f"missing page or invalid checksum: {page_id}")
    return page


def slab_entries(page):
    count = struct.unpack_from("<H", page, HEADER)[0]
    if 2 + count * 16 > SIZE - HEADER:
        raise ValueError("invalid slab directory size")
    entries = []
    for i in range(count):
        entity, kind, _, offset, length = struct.unpack_from("<QBBHI", page, HEADER + 2 + i * 16)
        if kind == 255:
            continue
        if kind not in (0, 1) or offset < 2 + count * 16 or offset + length > SIZE - HEADER:
            raise ValueError("invalid slab entry")
        entries.append((entity, kind, page[HEADER + offset:HEADER + offset + length]))
    return entries


def properties(data, count):
    """Validate the complete property stream without printing its values."""
    pos = 0
    for _ in range(count):
        if pos >= len(data):
            raise ValueError("missing property key")
        key_len = data[pos]
        pos += 1
        if pos + key_len >= len(data):
            raise ValueError("truncated property key")
        data[pos:pos + key_len].decode("utf-8")
        pos += key_len
        tag = data[pos]
        pos += 1
        if tag in (1, 5):
            if pos + 4 > len(data):
                raise ValueError("truncated property length")
            length = struct.unpack_from("<I", data, pos)[0]
            pos += 4
        elif tag in (2, 3):
            length = 8
        elif tag == 4:
            length = 1
        else:
            raise ValueError("unknown property tag")
        if pos + length > len(data):
            raise ValueError("truncated property value")
        if tag == 1:
            data[pos:pos + length].decode("utf-8")
        pos += length
    if pos != len(data):
        raise ValueError("property count and bytes disagree")
    return data


def read_blob(overflow, first, entity, kind):
    page = checked_page(overflow, first)
    if struct.unpack_from("<H", page, 6)[0] == 8:
        matches = [blob for key, entry_kind, blob in slab_entries(page) if (key, entry_kind) == (entity, kind)]
        if len(matches) != 1:
            raise ValueError("missing or ambiguous property blob")
        return matches[0]
    if struct.unpack_from("<H", page, 6)[0] != 5:
        raise ValueError("property reference has wrong page type")
    total = struct.unpack_from("<I", page, HEADER)[0]
    result = bytearray(page[20:20 + min(total, 4072)])
    nxt = struct.unpack_from("<I", page, 4092)[0]
    seen = {first}
    while len(result) < total:
        if nxt == 0xffffffff or nxt in seen:
            raise ValueError("truncated or cyclic property chain")
        seen.add(nxt)
        page = checked_page(overflow, nxt)
        result.extend(page[20:20 + min(total - len(result), 4076)])
        nxt = struct.unpack_from("<I", page, HEADER)[0]
    return bytes(result)


def inventory(source):
    source = Path(source).resolve()
    files = {p.name: p.read_bytes() for p in source.iterdir() if p.is_file() and not p.is_symlink()}
    if any(p.is_symlink() for p in source.iterdir()):
        raise ValueError("symlink in source")
    meta = checked_page(files["graph.meta"], 0)
    if meta[:4] != b"TGMD" or struct.unpack_from("<H", meta, 4)[0] != 4:
        raise ValueError("unsupported metadata format")
    result = {"source": str(source), "files": {name: digest(data) for name, data in files.items()},
              "wal_bytes": len(files.get("wal.log", b"")), "live": [], "orphans": [], "issues": []}
    overflow = files["overflow.db"]
    if len(overflow) % SIZE:
        raise ValueError("partial overflow page")
    referenced = set()
    blobs = {}
    for kind, name in enumerate(("nodes.db", "edges.db")):
        data = files[name]
        if len(data) % SIZE:
            raise ValueError("partial record page")
        for pid in range(len(data) // SIZE):
            page = checked_page(data, pid)
            if struct.unpack_from("<H", page, 6)[0] == 7:
                continue
            for index in range(31):
                slot = page[HEADER + index * SLOT:HEADER + (index + 1) * SLOT]
                if slot[0] != 1:
                    continue
                entity = struct.unpack_from("<Q", slot, 1)[0]
                # Edge offsets differ from node offsets; unsupported recovery must fail closed.
                if kind == 1:
                    result["issues"].append("edge recovery requires engine reconciliation")
                    continue
                count, inline = struct.unpack_from("<HH", slot, 65)
                record = {"kind": "node", "id": entity, "source": str(source), "sha256": None,
                          "identity": digest(slot[1:65]), "page": pid, "slot": index, "prop_count": count}
                try:
                    if inline > 38:
                        raise ValueError("invalid inline length")
                    if count and not inline:
                        pointer = struct.unpack_from("<I", slot, 107)[0]
                        referenced.add((pointer, entity, kind))
                        blob = read_blob(overflow, pointer, entity, kind)
                    else:
                        blob = slot[69:69 + inline]
                    properties(blob, count)
                    record["sha256"] = digest(blob)
                    blobs[("node", entity)] = blob
                except ValueError as error:
                    record["error"] = str(error)
                result["live"].append(record)
    for pid in range(len(overflow) // SIZE):
        page = checked_page(overflow, pid)
        if struct.unpack_from("<H", page, 6)[0] != 8:
            continue
        for entity, kind, blob in slab_entries(page):
            if (pid, entity, kind) not in referenced:
                result["orphans"].append({"kind": "node" if kind == 0 else "edge", "id": entity,
                                          "page": pid, "sha256": digest(blob), "source": str(source)})
    expected_nodes, expected_edges = struct.unpack_from("<QQ", meta, 32)
    if expected_nodes != len(result["live"]) or expected_edges:
        result["issues"].append("metadata counts do not match supported live records")
    return result, blobs


def prepare_candidate(source, previous, destination):
    source, previous, destination = map(lambda p: Path(p).resolve(), (source, previous, destination))
    if destination.exists() or destination.is_relative_to(source) or destination.is_relative_to(previous):
        raise ValueError("destination must be new and outside the evidence sources")
    current, _ = inventory(source)
    historical, old_blobs = inventory(previous)
    plan = recovery_plan(current, historical["live"])
    if plan["status"] == "blocked":
        raise ValueError("unresolved recovery evidence")
    # Build all replacement bytes before creating any output.
    nodes = bytearray((source / "nodes.db").read_bytes())
    overflow = bytearray((source / "overflow.db").read_bytes())
    meta = bytearray((source / "graph.meta").read_bytes())
    for replacement in plan["replacements"]:
        blob = old_blobs[(replacement["kind"], replacement["id"])]
        if len(blob) > 4072:
            raise ValueError("historical candidate exceeds bounded recovery format")
        current_record = next(r for r in current["live"] if r["id"] == replacement["id"] and r["kind"] == replacement["kind"])
        page_id = len(overflow) // SIZE
        page = bytearray(SIZE)
        struct.pack_into("<4sHHIHH", page, 0, b"TGOD", 1, 5, 0, 0, 0)
        struct.pack_into("<I", page, HEADER, len(blob))
        page[20:20 + len(blob)] = blob
        struct.pack_into("<I", page, 4092, 0xffffffff)
        struct.pack_into("<I", page, 8, zlib.crc32(page[HEADER:]))
        overflow.extend(page)
        offset = current_record["page"] * SIZE + HEADER + current_record["slot"] * SLOT
        struct.pack_into("<HH", nodes, offset + 65, replacement["prop_count"], 0)
        struct.pack_into("<I", nodes, offset + 107, page_id)
        start = current_record["page"] * SIZE
        struct.pack_into("<I", nodes, start + 8, zlib.crc32(nodes[start + HEADER:start + SIZE]))
    struct.pack_into("<I", meta, 64, len(overflow) // SIZE)
    struct.pack_into("<I", meta, 8, zlib.crc32(meta[HEADER:]))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="issue17-candidate-", dir=destination.parent) as temporary:
        stage = Path(temporary) / "graph"
        shutil.copytree(source, stage)
        (stage / "nodes.db").write_bytes(nodes)
        (stage / "overflow.db").write_bytes(overflow)
        (stage / "graph.meta").write_bytes(meta)
        (stage / "index.bin").unlink(missing_ok=True)
        report = {"status": "UNAPPROVED_CANDIDATE", "source_hashes": current["files"],
                  "historical_hashes": historical["files"], "plan": plan,
                  "limitations": "Historical values may be stale. Unreferenced blobs do not prove committed records. No claim of complete recovery."}
        (stage / "RECOVERY_REVIEW.json").write_text(json.dumps(report, indent=2) + "\n")
        verified, _ = inventory(stage)
        if verified["issues"] or any(r["sha256"] is None for r in verified["live"]):
            raise ValueError("candidate failed structural verification")
        # Evidence must remain unchanged throughout preparation.
        for path, expected in [(source, current["files"]), (previous, historical["files"])]:
            if inventory(path)[0]["files"] != expected:
                raise ValueError("evidence changed during recovery preparation")
        if destination.exists():
            raise ValueError("destination appeared during preparation")
        stage.rename(destination)
    return report


def validate_report_destination(output, sources):
    output = Path(output).resolve()
    if output.exists() or any(output.is_relative_to(Path(source).resolve()) for source in sources):
        raise ValueError("report destination must be new and outside evidence sources")
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--previous", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--candidate", type=Path)
    args = parser.parse_args()
    output = validate_report_destination(args.output, [args.source, args.previous])
    current, _ = inventory(args.source)
    historical, _ = inventory(args.previous)
    report = {"current": current, "historical": historical,
              "plan": recovery_plan(current, historical["live"])}
    if args.candidate:
        report["candidate"] = prepare_candidate(args.source, args.previous, args.candidate)
    with output.open("x") as handle:
        handle.write(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["plan"]["status"], "live": len(current["live"]),
                      "quarantined": len(current["orphans"]), "replacements": len(report["plan"]["replacements"]),
                      "unresolved": report["plan"]["unresolved"]}))


if __name__ == "__main__":
    main()
