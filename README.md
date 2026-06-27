# Weather Database Backup

Off-site backup of the Colorado Springs weather database (`weather.db`), pushed
nightly from **wxbox** — the live source of truth.

- **Last backup:** 20260627_041501 (wxbox local time)
- **Compressed size:** 27 MB (gzip)
- **Restore:** `gunzip weather.db.gz`

Force-pushed with no history (latest snapshot only). Plain gzip, not Git LFS.
