# PERM DOL Dashboard Backend

Dedicated backend for the PERM Premium Dashboard.
Scrapes flag.dol.gov/processingtimes + DOL XLSX disclosure data.

## Deploy on Railway

1. Push this folder to GitHub
2. railway.app → New Project → Deploy from GitHub
3. Select this repo → Deploy
4. Settings → Generate Domain

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Health check + endpoint list |
| `GET /api/dol/queue` | PERM queue dates (Analyst/Audit/Recon) |
| `GET /api/dol/pending` | PERM pending cases by receipt month |
| `GET /api/dol/pwd` | Prevailing wage determination dates |
| `GET /api/dol/avg-days` | Average processing days |
| `GET /api/dol/schedule` | DOL update schedule |
| `GET /api/cases/stats` | Yesterday certified/processed counts |
| `GET /api/cases/chart?days=30&type=certified` | Chart data |
| `GET /api/scraper/run?type=all` | Trigger manual scrape |
| `GET /api/scraper/run?type=dol` | Scrape DOL dates only |
| `GET /api/scraper/run?type=xlsx` | Scrape XLSX only |
| `GET /api/scraper/logs` | Scraper logs |

## Data Sources

1. **flag.dol.gov/processingtimes** → Queue dates, PWD dates, pending by month
2. **DOL XLSX** → PERM_Disclosure_Data_FY2026_Q1.xlsx → Real case counts

## Scrape Schedule

- On startup: immediate scrape
- Then: every 12 hours automatically

## Dashboard Integration

In your dashboard JS, set:
```js
const API = 'https://your-new-railway-url.railway.app';
```

Then call:
- `/api/dol/queue` → Analyst/Audit/Recon dates
- `/api/dol/pending` → Pending by month cards
- `/api/dol/pwd` → PWD table data
- `/api/cases/chart?days=30` → Certified vs Processed charts
- `/api/cases/stats` → Yesterday's numbers
