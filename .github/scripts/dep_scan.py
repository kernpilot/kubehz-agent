#!/usr/bin/env python3
"""The verdict of the daily dependency scan.

  dep_scan.py --ignore FILE REPORT...

Each REPORT is the output of `govulncheck -format json ./...` for one module.

The verdict is red when
  - the code calls a vulnerable symbol and no valid ignore entry names it, or
  - an ignore entry is past its expiry date (also when its finding is gone).
A vulnerable module that the code does not call is listed and is not red.

Exit status: 0 green, 1 red, 2 the input is not usable. A report without a
scan in it gives 2: a scan that did not run is not a clean scan.
"""

import argparse
import datetime
import json
import sys


class InputError(Exception):
    """A file that the verdict cannot rest on."""


def load_ignore(path, today):
    """Return (valid, expired) entries of the ignore file."""
    try:
        with open(path, encoding="utf-8") as f:
            entries = json.load(f)["ignore"]
    except (OSError, ValueError, KeyError, TypeError) as err:
        raise InputError(f"{path}: {err!r}") from err
    if not isinstance(entries, list):
        raise InputError(f"{path}: 'ignore' is not a list")
    valid, expired, seen = [], [], set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"id", "package", "reason", "expires"}:
            raise InputError(f"{path}: an entry needs exactly id, package, reason and expires: {entry}")
        if not all(isinstance(v, str) and v.strip() for v in entry.values()):
            raise InputError(f"{path}: an empty or non-text value: {entry}")
        try:
            expires = datetime.date.fromisoformat(entry["expires"])
        except ValueError as err:
            raise InputError(f"{path}: {entry['id']}: {err}") from err
        key = (entry["id"], entry["package"])
        if key in seen:
            raise InputError(f"{path}: {entry['id']} for {entry['package']} is listed twice")
        seen.add(key)
        (expired if expires < today else valid).append(entry)
    return valid, expired


def find_ignore(valid, ids, package):
    """The entry that names one of the ids of a finding and its package."""
    return next((e for e in valid if e["package"] == package and e["id"] in ids), None)


def one_line(text):
    """Advisory text is foreign: keep it on one line, so that it cannot start
    a workflow command in the log."""
    return " ".join(str(text).split())


def read_stream(path):
    """The messages of a govulncheck JSON stream (objects, one after another)."""
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except (OSError, ValueError) as err:
        raise InputError(f"{path}: {err!r}") from err
    decoder, pos, messages = json.JSONDecoder(), 0, []
    while True:
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos == len(text):
            return messages
        try:
            message, pos = decoder.raw_decode(text, pos)
        except ValueError as err:
            raise InputError(f"{path}: not a JSON stream: {err}") from err
        messages.append(message)


def read_report(path):
    """Return (config, sbom, findings) of one report."""
    config, sbom, osv, findings = None, None, {}, {}
    for message in read_stream(path):
        if "config" in message:
            config = message["config"]
        elif "SBOM" in message:
            sbom = message["SBOM"]
        elif "osv" in message:
            osv[message["osv"]["id"]] = message["osv"]
        elif "finding" in message:
            found = message["finding"]
            top = found["trace"][0]
            item = findings.setdefault(
                (found["osv"], top["module"]),
                {"version": top.get("version", ""), "fixed": "", "called": False, "caller": ""},
            )
            item["fixed"] = found.get("fixed_version") or item["fixed"]
            # Only a frame with a function is a call. govulncheck also reports
            # a module that is required and a package that is imported.
            if top.get("function") and not item["called"]:
                last = found["trace"][-1]
                where = last.get("position") or {}
                item["called"] = True
                item["caller"] = f"{last.get('package', '')}.{last.get('function', '')}"
                if where.get("filename"):
                    item["caller"] += f" ({where['filename']}:{where.get('line', 0)})"
    if not config or config.get("scan_level") != "symbol" or config.get("scan_mode") != "source":
        raise InputError(f"{path}: not a source scan at symbol level")
    if not sbom or not sbom.get("modules") or not sbom.get("roots"):
        raise InputError(f"{path}: the scan read no module or no package")
    for (vuln, _), item in findings.items():
        item["ids"] = {vuln, *osv.get(vuln, {}).get("aliases", [])}
        item["summary"] = one_line(osv.get(vuln, {}).get("summary", ""))
    return config, sbom, findings


def verdict(ignore_path, reports, today, out):
    """Write the verdict to `out` and return the exit status."""
    valid, expired = load_ignore(ignore_path, today)
    red, used = False, []
    for path in reports:
        config, sbom, findings = read_report(path)
        print(
            f"{path}: {len(sbom['modules'])} modules, {len(sbom['roots'])} packages,"
            f" {config.get('go_version')}, {config.get('scanner_version')},"
            f" database of {config.get('db_last_modified')}",
            file=out,
        )
        for (vuln, module), item in sorted(findings.items()):
            fix = f"fixed in {item['fixed']}" if item["fixed"] else "no fixed version"
            line = f"{vuln} {module} {item['version']} ({fix}): {item['summary']}"
            entry = find_ignore(valid, item["ids"], module)
            if not item["called"]:
                print(f"  not called  {line}", file=out)
            elif entry:
                used.append(entry)
                print(f"  ignored     {line}; until {entry['expires']}: {entry['reason']}", file=out)
            else:
                red = True
                print(f"::error::{line}; called from {item['caller']}", file=out)
    for entry in expired:
        red = True
        print(f"::error::the ignore entry {entry['id']} ({entry['package']}) expired on {entry['expires']}", file=out)
    for entry in valid:
        if entry not in used:
            print(f"::warning::the ignore entry {entry['id']} ({entry['package']}) matches no finding: remove it", file=out)
    print("verdict: red" if red else "verdict: green", file=out)
    return 1 if red else 0


def main(argv):
    parser = argparse.ArgumentParser(description="The verdict of the daily dependency scan.")
    parser.add_argument("--ignore", required=True)
    parser.add_argument("report", nargs="+")
    args = parser.parse_args(argv)
    today = datetime.datetime.now(datetime.timezone.utc).date()
    try:
        return verdict(args.ignore, args.report, today, sys.stdout)
    except InputError as err:
        print(f"::error::{err}")
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
