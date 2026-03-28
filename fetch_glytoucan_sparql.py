#!/usr/bin/env python3
"""Fetch bulk data from GlyTouCan SPARQL endpoints.

This script is intentionally stdlib-only so it can run in constrained
environments without extra dependencies.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List


DEFAULT_ENDPOINT = "https://ts.glytoucan.org/sparql"
DEFAULT_TIMEOUT = 180
DEFAULT_PAGE_SIZE = 5000
DEFAULT_RETRIES = 4
USER_AGENT = "glytoucan-bulk-fetcher/0.1"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_query(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sanitize_filename(value: str) -> str:
    value = re.sub(r"^https?://", "", value)
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return value.strip("._-") or "graph"


def _build_headers() -> Dict[str, str]:
    return {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "User-Agent": USER_AGENT,
    }


def _post_sparql(endpoint: str, query: str, timeout: int) -> Dict:
    payload = urllib.parse.urlencode({"query": query}).encode("utf-8")
    req = urllib.request.Request(endpoint, data=payload, headers=_build_headers(), method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw)


def _get_sparql(endpoint: str, query: str, timeout: int) -> Dict:
    url = f"{endpoint}?{urllib.parse.urlencode({'query': query})}"
    req = urllib.request.Request(url, headers=_build_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw)


def sparql_request(endpoint: str, query: str, timeout: int, retries: int) -> Dict:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return _post_sparql(endpoint=endpoint, query=query, timeout=timeout)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, socket.timeout, json.JSONDecodeError) as exc:
            last_error = exc

        try:
            return _get_sparql(endpoint=endpoint, query=query, timeout=timeout)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, socket.timeout, json.JSONDecodeError) as exc:
            last_error = exc

        if attempt == retries:
            break

        sleep_seconds = min(2 ** (attempt - 1), 20)
        print(
            f"[retry] endpoint={endpoint} attempt={attempt}/{retries} waiting={sleep_seconds}s error={last_error}",
            file=sys.stderr,
        )
        time.sleep(sleep_seconds)

    raise RuntimeError(f"SPARQL request failed after {retries} attempts: {last_error}") from last_error


def paginated_query(base_query: str, limit: int, offset: int) -> str:
    return f"{base_query}\nLIMIT {limit}\nOFFSET {offset}\n"


def limited_query(base_query: str, limit: int) -> str:
    return f"{base_query}\nLIMIT {limit}\n"


def bindings_to_rows(bindings: Iterable[Dict], variables: List[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for binding in bindings:
        row = {}
        for var in variables:
            row[var] = binding.get(var, {}).get("value", "")
        rows.append(row)
    return rows


def write_json(path: Path, payload: Dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def write_csv(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def raw_page_files(raw_dir: Path) -> List[Path]:
    return sorted(raw_dir.glob("*.json"))


def load_rows_from_page(page_path: Path) -> tuple[List[str], List[Dict[str, str]]]:
    payload = json.loads(page_path.read_text(encoding="utf-8"))
    variables = payload.get("head", {}).get("vars", [])
    bindings = payload.get("results", {}).get("bindings", [])
    rows = bindings_to_rows(bindings, variables)
    return variables, rows


def assemble_outputs(output_dir: Path, query_name: str) -> Dict:
    raw_dir = output_dir / "raw_pages"
    all_rows: List[Dict[str, str]] = []
    variables: List[str] = []
    page_files = raw_page_files(raw_dir)

    for page_path in page_files:
        page_vars, rows = load_rows_from_page(page_path)
        if not variables:
            variables = page_vars
        all_rows.extend(rows)

    jsonl_count = write_jsonl(output_dir / f"{query_name}.jsonl", all_rows)
    write_csv(output_dir / f"{query_name}.csv", all_rows, variables)
    return {
        "row_count": jsonl_count,
        "page_count": len(page_files),
        "variables": variables,
    }


def last_row_from_raw_pages(raw_dir: Path) -> Dict[str, str] | None:
    page_files = raw_page_files(raw_dir)
    if not page_files:
        return None
    _, rows = load_rows_from_page(page_files[-1])
    return rows[-1] if rows else None


def fetch_paged_rows(
    endpoint: str,
    query_name: str,
    query_text: str,
    output_dir: Path,
    page_size: int,
    timeout: int,
    retries: int,
    max_pages: int | None = None,
) -> Dict:
    raw_dir = output_dir / "raw_pages"
    ensure_dir(raw_dir)

    page_files = raw_page_files(raw_dir)
    existing_pages = len(page_files)
    offset = existing_pages * page_size
    page_index = existing_pages
    fetched_pages = 0

    if page_files:
        _, last_rows = load_rows_from_page(page_files[-1])
        if len(last_rows) < page_size:
            assembled = assemble_outputs(output_dir, query_name)
            manifest = {
                "query_name": query_name,
                "endpoint": endpoint,
                "page_size": page_size,
                "row_count": assembled["row_count"],
                "page_count": assembled["page_count"],
                "variables": assembled["variables"],
                "generated_at_utc": utc_now(),
                "complete": True,
                "resumed_from_existing_pages": existing_pages,
            }
            write_json(output_dir / "manifest.json", manifest)
            return manifest

    while True:
        if max_pages is not None and fetched_pages >= max_pages:
            break

        query = paginated_query(query_text, page_size, offset)
        payload = sparql_request(endpoint=endpoint, query=query, timeout=timeout, retries=retries)
        page_path = raw_dir / f"{page_index:06d}.json"
        write_json(page_path, payload)

        bindings = payload.get("results", {}).get("bindings", [])
        rows = bindings_to_rows(bindings, payload.get("head", {}).get("vars", []))

        print(
            f"[page] query={query_name} page={page_index} offset={offset} rows={len(rows)}",
            file=sys.stderr,
        )

        fetched_pages += 1
        if len(rows) < page_size:
            break

        offset += page_size
        page_index += 1

    assembled = assemble_outputs(output_dir, query_name)
    complete = False
    page_files = raw_page_files(raw_dir)
    if page_files:
        _, last_rows = load_rows_from_page(page_files[-1])
        complete = len(last_rows) < page_size
    manifest = {
        "query_name": query_name,
        "endpoint": endpoint,
        "page_size": page_size,
        "row_count": assembled["row_count"],
        "page_count": assembled["page_count"],
        "variables": assembled["variables"],
        "generated_at_utc": utc_now(),
        "complete": complete,
        "resumed_from_existing_pages": existing_pages,
        "fetched_pages_this_run": fetched_pages,
    }
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def fetch_cursor_paged_rows(
    endpoint: str,
    query_name: str,
    query_text: str,
    output_dir: Path,
    page_size: int,
    timeout: int,
    retries: int,
    max_pages: int | None = None,
) -> Dict:
    placeholder = "__AFTER_ACCNUM_FILTER__"
    if placeholder not in query_text:
        raise ValueError(f"Cursor query must contain placeholder: {placeholder}")

    raw_dir = output_dir / "raw_pages"
    ensure_dir(raw_dir)

    page_files = raw_page_files(raw_dir)
    existing_pages = len(page_files)
    page_index = existing_pages
    fetched_pages = 0

    if page_files:
        _, last_rows = load_rows_from_page(page_files[-1])
        if len(last_rows) < page_size:
            assembled = assemble_outputs(output_dir, query_name)
            manifest = {
                "query_name": query_name,
                "endpoint": endpoint,
                "page_size": page_size,
                "row_count": assembled["row_count"],
                "page_count": assembled["page_count"],
                "variables": assembled["variables"],
                "generated_at_utc": utc_now(),
                "complete": True,
                "resumed_from_existing_pages": existing_pages,
                "pagination": "cursor_accnum",
            }
            write_json(output_dir / "manifest.json", manifest)
            return manifest

    while True:
        if max_pages is not None and fetched_pages >= max_pages:
            break

        last_row = last_row_from_raw_pages(raw_dir)
        after_filter = ""
        if last_row and last_row.get("AccNum"):
            after_value = last_row["AccNum"].replace("\\", "\\\\").replace('"', '\\"')
            after_filter = f'FILTER(?AccNum > "{after_value}")'
        query = limited_query(query_text.replace(placeholder, after_filter), page_size)
        payload = sparql_request(endpoint=endpoint, query=query, timeout=timeout, retries=retries)
        page_path = raw_dir / f"{page_index:06d}.json"
        write_json(page_path, payload)

        bindings = payload.get("results", {}).get("bindings", [])
        rows = bindings_to_rows(bindings, payload.get("head", {}).get("vars", []))
        cursor_value = rows[-1].get("AccNum", "") if rows else ""
        print(
            f"[page] query={query_name} page={page_index} rows={len(rows)} last_accnum={cursor_value}",
            file=sys.stderr,
        )

        fetched_pages += 1
        page_index += 1
        if len(rows) < page_size:
            break

    assembled = assemble_outputs(output_dir, query_name)
    complete = False
    page_files = raw_page_files(raw_dir)
    if page_files:
        _, last_rows = load_rows_from_page(page_files[-1])
        complete = len(last_rows) < page_size
    manifest = {
        "query_name": query_name,
        "endpoint": endpoint,
        "page_size": page_size,
        "row_count": assembled["row_count"],
        "page_count": assembled["page_count"],
        "variables": assembled["variables"],
        "generated_at_utc": utc_now(),
        "complete": complete,
        "resumed_from_existing_pages": existing_pages,
        "fetched_pages_this_run": fetched_pages,
        "pagination": "cursor_accnum",
    }
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def fetch_main_dataset(args: argparse.Namespace, run_dir: Path) -> Dict:
    query_path = Path(args.query_dir) / "main_glycan_dump.rq"
    query = load_query(query_path)
    output_dir = run_dir / "main_dataset"
    ensure_dir(output_dir)
    return fetch_cursor_paged_rows(
        endpoint=args.endpoint,
        query_name="main_glycan_dump",
        query_text=query,
        output_dir=output_dir,
        page_size=args.page_size,
        timeout=args.timeout,
        retries=args.retries,
        max_pages=args.max_pages,
    )


def fetch_partner_graphs(args: argparse.Namespace, run_dir: Path) -> List[str]:
    query_path = Path(args.query_dir) / "partner_graphs.rq"
    query = load_query(query_path)
    output_dir = run_dir / "partner_graphs"
    ensure_dir(output_dir)

    payload = sparql_request(
        endpoint=args.endpoint,
        query=query,
        timeout=args.timeout,
        retries=args.retries,
    )
    write_json(output_dir / "partner_graphs_raw.json", payload)

    variables = payload.get("head", {}).get("vars", [])
    bindings = payload.get("results", {}).get("bindings", [])
    rows = bindings_to_rows(bindings, variables)
    write_jsonl(output_dir / "partner_graphs.jsonl", rows)
    write_csv(output_dir / "partner_graphs.csv", rows, variables)

    graphs = [row["g"] for row in rows if row.get("g")]
    manifest = {
        "endpoint": args.endpoint,
        "graph_count": len(graphs),
        "graphs": graphs,
        "generated_at_utc": utc_now(),
    }
    write_json(output_dir / "manifest.json", manifest)
    return graphs


def fetch_partner_graph_dump(
    args: argparse.Namespace,
    run_dir: Path,
    graph_uri: str,
) -> Dict:
    template_path = Path(args.query_dir) / "partner_graph_triples.rq"
    query_template = load_query(template_path)
    safe_graph = sanitize_filename(graph_uri)
    output_dir = run_dir / "partner_graph_dumps" / safe_graph
    ensure_dir(output_dir)

    graph_escaped = graph_uri.replace("\\", "\\\\").replace('"', '\\"')
    query = query_template.replace("__GRAPH_URI__", graph_escaped)
    manifest = fetch_paged_rows(
        endpoint=args.endpoint,
        query_name="triples",
        query_text=query,
        output_dir=output_dir,
        page_size=args.page_size,
        timeout=args.timeout,
        retries=args.retries,
        max_pages=args.max_pages,
    )
    manifest["graph_uri"] = graph_uri
    manifest["safe_graph"] = safe_graph
    write_json(output_dir / "manifest.json", manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bulk fetch GlyTouCan SPARQL data.")
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"SPARQL endpoint. Default: {DEFAULT_ENDPOINT}",
    )
    parser.add_argument(
        "--query-dir",
        default="queries",
        help="Directory containing .rq query templates.",
    )
    parser.add_argument(
        "--output-root",
        default="data",
        help="Directory where fetched data will be written.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Rows per SPARQL page. Default: {DEFAULT_PAGE_SIZE}",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT}",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help=f"Retry count per request. Default: {DEFAULT_RETRIES}",
    )
    parser.add_argument(
        "--mode",
        choices=("all", "main", "partner-graphs", "partner-dumps"),
        default="all",
        help="Which fetch workflow to run.",
    )
    parser.add_argument(
        "--graph-uri",
        action="append",
        default=[],
        help="Optional specific partner graph URI. Repeat to fetch multiple graphs.",
    )
    parser.add_argument(
        "--run-dir",
        default="",
        help="Optional existing run directory for resume. If omitted, a new run directory is created.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional limit on how many pages to fetch in this execution.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir) if args.run_dir else Path(args.output_root) / "runs" / utc_now()
    ensure_dir(run_dir)

    summary: Dict[str, object] = {
        "mode": args.mode,
        "endpoint": args.endpoint,
        "run_dir": str(run_dir.resolve()),
        "generated_at_utc": utc_now(),
    }

    if args.mode in {"all", "main"}:
        summary["main_dataset"] = fetch_main_dataset(args, run_dir)

    graphs: List[str] = []
    if args.mode in {"all", "partner-graphs"}:
        graphs = fetch_partner_graphs(args, run_dir)
        summary["partner_graphs"] = graphs

    if args.mode == "partner-dumps":
        graphs = args.graph_uri
    elif args.mode == "all" and not graphs:
        graphs = []

    if args.mode in {"all", "partner-dumps"}:
        if not graphs:
            partner_manifest = run_dir / "partner_graphs" / "manifest.json"
            if partner_manifest.exists():
                graphs = json.loads(partner_manifest.read_text(encoding="utf-8")).get("graphs", [])
        dumps = [fetch_partner_graph_dump(args, run_dir, graph_uri) for graph_uri in graphs]
        summary["partner_graph_dumps"] = dumps

    write_json(run_dir / "run_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
