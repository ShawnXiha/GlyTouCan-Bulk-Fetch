# GlyTouCan Bulk Fetch

This directory now contains a standalone bulk fetcher for GlyTouCan SPARQL data.

## What it does

- Fetches the main GlyTouCan structure dataset from `https://ts.glytoucan.org/sparql`
- Enumerates partner graphs under `http://rdf.glytoucan.org/partner/*`
- Dumps each partner graph as paginated SPARQL results
- Saves both raw page responses and merged `jsonl/csv`

## Files

- `scripts/fetch_glytoucan_sparql.py`: bulk fetch entrypoint
- `queries/main_glycan_dump.rq`: main glycan query
- `queries/partner_graphs.rq`: partner graph enumeration
- `queries/partner_graph_triples.rq`: generic partner graph triple dump

## Usage

Run the full workflow:

```powershell
python scripts/fetch_glytoucan_sparql.py --mode all
```

Only fetch the main dataset:

```powershell
python scripts/fetch_glytoucan_sparql.py --mode main
```

Only enumerate partner graphs:

```powershell
python scripts/fetch_glytoucan_sparql.py --mode partner-graphs
```

Dump specific partner graphs:

```powershell
python scripts/fetch_glytoucan_sparql.py --mode partner-dumps `
  --graph-uri http://rdf.glytoucan.org/partner/glycomedb `
  --graph-uri http://rdf.glytoucan.org/partner/pubchem
```

Tune paging if the endpoint is unstable:

```powershell
python scripts/fetch_glytoucan_sparql.py --mode all --page-size 1000 --retries 6
```

## Output layout

Each run writes into:

```text
data/runs/<UTC timestamp>/
```

Typical contents:

- `main_dataset/raw_pages/*.json`
- `main_dataset/main_glycan_dump.jsonl`
- `main_dataset/main_glycan_dump.csv`
- `partner_graphs/partner_graphs.csv`
- `partner_graph_dumps/<graph>/raw_pages/*.json`
- `partner_graph_dumps/<graph>/triples.jsonl`
- `partner_graph_dumps/<graph>/triples.csv`
- `run_summary.json`

## Notes

- The main query excludes the `archive` graph.
- Partner graph dumps are generic triple exports. They preserve all returned triples first; semantic normalization can be added later.
- The script uses only Python standard library modules.
