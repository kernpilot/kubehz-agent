#!/usr/bin/env python3
"""Tests of dep_scan.py. Run: python3 .github/scripts/dep_scan_test.py"""

import contextlib
import datetime
import io
import json
import os
import tempfile
import unittest

import dep_scan

TODAY = datetime.date(2026, 1, 15)
CONFIG = {"config": {"go_version": "go1.26.1", "scan_level": "symbol", "scan_mode": "source"}}
SBOM = {"SBOM": {"modules": [{"path": "example.com/app"}], "roots": ["example.com/app"]}}
OSV = {"osv": {"id": "GO-2026-0001", "aliases": ["CVE-2026-1"], "summary": "a hole"}}


def finding(function="", module="example.com/lib", fixed="v1.2.0"):
    top = {"module": module, "version": "v1.0.0"}
    if function:
        top["function"] = function
    main = {"module": "example.com/app", "package": "example.com/app", "function": "main",
            "position": {"filename": "main.go", "line": 7}}
    return {"finding": {"osv": "GO-2026-0001", "fixed_version": fixed, "trace": [top, main] if function else [top]}}


def entry(**changes):
    return {"id": "GO-2026-0001", "package": "example.com/lib", "reason": "not exposed", "expires": "2026-01-31", **changes}


class VerdictTest(unittest.TestCase):
    def run_verdict(self, messages, ignore=(), raw_ignore=None, today=TODAY):
        with tempfile.TemporaryDirectory() as tmp:
            report, ignore_file = os.path.join(tmp, "report.json"), os.path.join(tmp, "ignore.json")
            with open(report, "w", encoding="utf-8") as f:
                # As govulncheck writes it: indented objects, one after another.
                f.write("\n".join(json.dumps(m, indent=2) for m in messages))
            with open(ignore_file, "w", encoding="utf-8") as f:
                f.write(json.dumps({"ignore": list(ignore)}) if raw_ignore is None else raw_ignore)
            out = io.StringIO()
            return dep_scan.verdict(ignore_file, [report], today, out), out.getvalue()

    def test_clean_scan_is_green(self):
        self.assertEqual(self.run_verdict([CONFIG, SBOM])[0], 0)

    def test_called_vulnerability_is_red_and_names_the_caller(self):
        rc, out = self.run_verdict([CONFIG, SBOM, OSV, finding(), finding("Parse")])
        self.assertEqual(rc, 1)
        self.assertIn("::error::GO-2026-0001 example.com/lib v1.0.0 (fixed in v1.2.0): a hole", out)
        self.assertIn("example.com/app.main (main.go:7)", out)

    def test_called_without_a_fix_is_red(self):
        rc, out = self.run_verdict([CONFIG, SBOM, OSV, finding("Parse", fixed="")])
        self.assertEqual(rc, 1)
        self.assertIn("no fixed version", out)

    def test_not_called_is_listed_and_green(self):
        rc, out = self.run_verdict([CONFIG, SBOM, OSV, finding()])
        self.assertEqual(rc, 0)
        self.assertIn("not called  GO-2026-0001", out)

    def test_a_summary_stays_on_one_line(self):
        osv = {"osv": {"id": "GO-2026-0001", "summary": "a\n::notice::hole"}}
        _, out = self.run_verdict([CONFIG, SBOM, osv, finding("Parse")])
        self.assertIn("a ::notice::hole", out)
        self.assertNotIn("\n::notice::", out)

    def test_order_of_the_findings_does_not_matter(self):
        self.assertEqual(self.run_verdict([CONFIG, SBOM, finding("Parse"), finding(), OSV])[0], 1)

    def test_valid_entry_ignores_by_id_or_alias(self):
        for name in ("GO-2026-0001", "CVE-2026-1"):
            rc, out = self.run_verdict([CONFIG, SBOM, OSV, finding("Parse")], [entry(id=name)])
            self.assertEqual(rc, 0, out)
            self.assertIn("not exposed", out)

    def test_entry_of_another_package_does_not_ignore(self):
        rc, _ = self.run_verdict([CONFIG, SBOM, OSV, finding("Parse")], [entry(package="stdlib")])
        self.assertEqual(rc, 1)

    def test_entry_is_valid_on_its_last_day_and_red_after_it(self):
        last = datetime.date(2026, 1, 31)
        messages = [CONFIG, SBOM, OSV, finding("Parse")]
        self.assertEqual(self.run_verdict(messages, [entry()], today=last)[0], 0)
        rc, out = self.run_verdict(messages, [entry()], today=last + datetime.timedelta(days=1))
        self.assertEqual(rc, 1)
        self.assertIn("expired on 2026-01-31", out)
        self.assertIn("called from", out)

    def test_expired_entry_is_red_when_its_finding_is_gone(self):
        self.assertEqual(self.run_verdict([CONFIG, SBOM], [entry(expires="2026-01-14")])[0], 1)

    def test_unused_entry_warns_and_is_green(self):
        rc, out = self.run_verdict([CONFIG, SBOM], [entry()])
        self.assertEqual(rc, 0)
        self.assertIn("::warning::", out)

    def test_malformed_ignore_files_are_refused(self):
        no_reason = {k: v for k, v in entry().items() if k != "reason"}
        for bad in ("", "[]", '{"ignore": {}}',
                    json.dumps({"ignore": [no_reason]}),
                    json.dumps({"ignore": [entry(reason=" ")]}),
                    json.dumps({"ignore": [entry(expires="soon")]}),
                    json.dumps({"ignore": [entry(severity="LOW")]}),
                    json.dumps({"ignore": [entry(), entry(reason="again")]})):
            with self.assertRaises(dep_scan.InputError, msg=bad):
                self.run_verdict([CONFIG, SBOM], raw_ignore=bad)

    def test_report_without_a_scan_is_refused(self):
        module_scan = {"config": {"scan_level": "module", "scan_mode": "source"}}
        binary_scan = {"config": {"scan_level": "symbol", "scan_mode": "binary"}}
        no_roots = {"SBOM": {"modules": [{"path": "example.com/app"}]}}
        no_modules = {"SBOM": {"roots": ["example.com/app"]}}
        for messages in ([], [CONFIG], [SBOM], [module_scan, SBOM], [binary_scan, SBOM],
                         [CONFIG, no_roots], [CONFIG, no_modules]):
            with self.assertRaises(dep_scan.InputError, msg=messages):
                self.run_verdict(messages)

    def test_main_gives_2_for_a_truncated_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            report, ignore_file = os.path.join(tmp, "report.json"), os.path.join(tmp, "ignore.json")
            with open(report, "w", encoding="utf-8") as f:
                f.write(json.dumps(CONFIG) + '{"SBOM": {')
            with open(ignore_file, "w", encoding="utf-8") as f:
                f.write('{"ignore": []}')
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(dep_scan.main(["--ignore", ignore_file, report]), 2)
            self.assertIn("not a JSON stream", out.getvalue())


if __name__ == "__main__":
    unittest.main()
