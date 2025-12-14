# FamilyBook CLI

Generate a Markdown genealogical book from a FamilySearch person ID. It pulls immediate family, ancestry and descendancy, portraits, life events, and (optionally) local historical context via Wikidata.

## Features
- Fetches ancestors and descendants from FamilySearch given a root person ID.
- Builds per-person sections with portrait URL, basic bio, life events, and immediate family.
- Optional historical context lookup by place and life span using Wikidata SPARQL.
- Outputs clean Markdown ready for conversion to PDF/Word.

## Requirements
- Python 3.10+
- `requests` (`pip install requests`)
- A valid FamilySearch OAuth access token with Tree API scope.

## Setup
1) Install dependencies:
   ```bash
   python3 -m pip install requests
   ```
2) Export your token (replace with your real token):
   ```bash
   export FS_ACCESS_TOKEN="your_familysearch_access_token"
   ```
   Optional: point to sandbox or a custom host:
   ```bash
   export FS_BASE_URL="https://api.familysearch.org"  # default
   ```

## Usage
```bash
python3 familybook.py --person-id <PERSON_ID> --generations 4 --context --output book.md
```
Flags:
- `--person-id` (required): FamilySearch ID for the root person.
- `--generations`: Generations to explore for both ancestry and descendancy (default 4).
- `--context`: Include historical context events from Wikidata (best-effort; failures are skipped).
- `--output`: Output Markdown path (default `family_book.md`).

After generation, convert Markdown if desired:
```bash
pandoc book.md -o book.pdf
```

## Notes on authentication
FamilySearch requires OAuth. Do not embed username/password. Obtain a token via the official OAuth flow for your registered app or developer sandbox, then set `FS_ACCESS_TOKEN`.

## Next steps / enhancements
- Cache or download portrait images to `assets/` for offline/PDF export.
- Add HTML/PDF export with styling (e.g., via WeasyPrint or wkhtmltopdf).
- Include more facts (occupations, memories/notes) and compute age at death.
- Add rate-limit handling and retries for FamilySearch and Wikidata calls.
- Add a small test suite for relationship parsing and event ordering.
