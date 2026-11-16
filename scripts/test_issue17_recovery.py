import unittest
from issue17_recovery import recovery_plan


def record(entity, digest, source="current"):
    return {"kind": "node", "id": entity, "sha256": digest, "source": source}


class RecoveryPlanTests(unittest.TestCase):
    def test_missing_reference_requires_explicit_historical_decision(self):
        current = {"live": [record(10, None)], "orphans": [], "wal_bytes": 0}
        plan = recovery_plan(current, [record(10, "old", "backup")])
        self.assertEqual(plan["status"], "needs_decision")
        self.assertEqual(plan["replacements"][0]["sha256"], "old")
        self.assertEqual(plan["replacements"][0]["source"], "backup")

    def test_orphans_never_become_live_records(self):
        current = {"live": [record(2, "valid")], "orphans": [record(99, "unknown")], "wal_bytes": 0}
        plan = recovery_plan(current, [])
        self.assertEqual(plan["status"], "needs_decision")
        self.assertEqual(plan["quarantined"], current["orphans"])
        self.assertEqual(plan["replacements"], [])

    def test_current_value_wins_over_historical_value(self):
        current = {"live": [record(2, "new")], "orphans": [], "wal_bytes": 0}
        self.assertEqual(recovery_plan(current, [record(2, "old", "backup")])["replacements"], [])

    def test_conflicting_historical_candidates_are_not_chosen(self):
        current = {"live": [record(10, None)], "orphans": [], "wal_bytes": 0}
        plan = recovery_plan(current, [record(10, "a", "backup1"), record(10, "b", "backup2")])
        self.assertEqual(plan["status"], "blocked")
        self.assertEqual(plan["replacements"], [])
        self.assertEqual(len(plan["unresolved"]), 1)

    def test_missing_history_is_not_invented(self):
        current = {"live": [record(10, None)], "orphans": [], "wal_bytes": 0}
        self.assertEqual(recovery_plan(current, [])["status"], "blocked")

    def test_wal_must_be_reconciled_before_offline_repair(self):
        current = {"live": [record(2, "valid")], "orphans": [], "wal_bytes": 42}
        self.assertEqual(recovery_plan(current, [])["status"], "blocked")


class RecoveryFileTests(unittest.TestCase):
    def test_report_cannot_overwrite_evidence(self):
        import tempfile
        from pathlib import Path
        from issue17_recovery import validate_report_destination
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            source.mkdir()
            evidence = source / 'graph.meta'
            evidence.write_bytes(b'original')
            for output in (evidence, source / 'new-report.json'):
                with self.assertRaises(ValueError):
                    validate_report_destination(output, [source])
            self.assertEqual(evidence.read_bytes(), b'original')

    def test_corrupt_page_is_rejected(self):
        from issue17_recovery import checked_page
        with self.assertRaises(ValueError):
            checked_page(bytes(4096), 0)
        with self.assertRaises(ValueError):
            checked_page(bytes(10), 0)

    def test_candidate_cannot_replace_or_write_inside_source(self):
        import tempfile
        from pathlib import Path
        from issue17_recovery import prepare_candidate
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            source.mkdir()
            for destination in (source, source / 'candidate'):
                with self.assertRaisesRegex(ValueError, 'destination must be new'):
                    prepare_candidate(source, source, destination)
            self.assertEqual(list(source.iterdir()), [])

    def test_candidate_recovers_only_missing_blob_and_preserves_inputs(self):
        import tempfile
        import struct
        import zlib
        from pathlib import Path
        from issue17_recovery import prepare_candidate, inventory, SIZE

        def page(magic, kind):
            data = bytearray(SIZE)
            struct.pack_into('<4sHHIHH', data, 0, magic, 4 if kind == 0 else 1, kind, 0, 1, 0)
            return data

        def finish(data):
            struct.pack_into('<I', data, 8, zlib.crc32(data[16:]))
            return data

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, valid in [('current', False), ('previous', True)]:
                target = root / name
                target.mkdir()
                meta = page(b'TGMD', 0)
                struct.pack_into('<QQ', meta, 32, 1, 0)
                node = page(b'TGND', 1)
                node[16] = 1
                struct.pack_into('<Q', node, 17, 10)
                struct.pack_into('<HH', node, 81, 1, 0)
                struct.pack_into('<I', node, 123, 0)
                slab = page(b'TGOD', 8)
                if valid:
                    blob = b'\x01k\x01' + struct.pack('<I', 3) + b'old'
                    struct.pack_into('<H', slab, 16, 1)
                    struct.pack_into('<QBBHI', slab, 18, 10, 0, 0, 100, len(blob))
                    slab[116:116 + len(blob)] = blob
                for filename, data in [('graph.meta', meta), ('nodes.db', node), ('overflow.db', slab)]:
                    (target / filename).write_bytes(finish(data))
                (target / 'edges.db').write_bytes(b'')
                (target / 'wal.log').write_bytes(b'')
            before = [inventory(root / name)[0]['files'] for name in ('current', 'previous')]
            report = prepare_candidate(root / 'current', root / 'previous', root / 'candidate')
            self.assertEqual(report['status'], 'UNAPPROVED_CANDIDATE')
            self.assertEqual(report['plan']['status'], 'needs_decision')
            recovered, blobs = inventory(root / 'candidate')
            self.assertEqual(len(recovered['live']), 1)
            self.assertEqual(blobs[('node', 10)], blob)
            self.assertEqual(before, [inventory(root / name)[0]['files'] for name in ('current', 'previous')])

if __name__ == "__main__":
    unittest.main()
