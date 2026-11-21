# Releasing remake

The steps for a release, in order. Written down for 0.9.0 (2026-11-21);
earlier releases followed the same steps informally.

What a tag does and doesn't do:

- **Pushing a `v*` tag** runs `.github/workflows/release.yml`: it builds the
  sdist and wheel, checks the tag matches `remake.__version__`, and
  publishes to **PyPI** (Trusted Publishing; no tokens). It does **not**
  create a GitHub Release.
- **Publishing a GitHub Release** is a separate step (below). Zenodo's
  GitHub integration (a repo webhook) archives the release and mints a
  **DOI** for it. No GitHub Release means no DOI.
- A manual run of the workflow (`workflow_dispatch`) publishes to TestPyPI
  only: the dry run.

## Before tagging

1. The release's review gates are done: see `releases/vX.Y.Z.md` (for a
   minor release, the pre-tag review in [roadmap.md](roadmap.md)). Merge
   into `main` with `git merge --no-ff --no-commit <branch>`, then commit
   (the review hook applies).
2. Bump `src/remake/version.py` to the release version. The workflow fails
   if the tag and the version differ (v0.9.0a1 was tagged with the version
   still at 0.8.5, and its release run failed).
3. In `CHANGELOG.md`, rename `[Unreleased]` to `[X.Y.Z] — YYYY-MM-DD`
   (keep an empty `[Unreleased]` above it), add a one-paragraph summary,
   and add the `[X.Y.Z]:` link at the bottom.
4. Full suite: `uv run pytest -q`.
5. Smoke test of the built wheel in a clean venv: `uv build`, install the
   wheel with `[examples]`, run `make_example_data.py` and a few examples
   (ex1, ex2, ex2 under `-E multiproc`).
6. Commit as `release: X.Y.Z.` (body: what was checked), push `main`, and
   wait for CI to go green on every Python version.

## Tagging and publishing

7. `git tag -a vX.Y.Z -m "remake X.Y.Z"` and `git push origin vX.Y.Z`.
8. Watch the release workflow (`gh run watch`) and check that the version
   is on PyPI: `https://pypi.org/project/remake/X.Y.Z/`.
9. Create the GitHub Release, with the changelog section as its notes:
   `gh release create vX.Y.Z --title vX.Y.Z --notes-file <section>`
   (add `--prerelease` for an `aN`/`rcN`; Zenodo archives those too).

## After publishing

10. **Check that Zenodo minted a DOI for this version.** Zenodo usually
    takes a few minutes after the GitHub Release is published. Query:

    ```sh
    curl -s 'https://zenodo.org/api/records?q=%22markmuetz%2Fremake%22&sort=mostrecent&size=3' \
      | python3 -c "import json,sys; [print(h['metadata'].get('version'), h['doi']) for h in json.load(sys.stdin)['hits']['hits']]"
    ```

    The newest record must have version `vX.Y.Z` and its own DOI. Every
    version shares the concept DOI 10.5281/zenodo.10058368. If no record
    appears within ~30 min, check the webhook's recent deliveries
    (repo Settings → Webhooks → zenodo.org) and the GitHub account's
    page on Zenodo (zenodo.org/account/settings/github), and fix it before
    calling the release done.
11. Record the version DOI in the release: add it to the GitHub Release
    notes (`gh release edit vX.Y.Z --notes-file ...`) and in
    `releases/vX.Y.Z.md`.
12. Freeze `releases/vX.Y.Z.md` as a record (design_docs/README.md), and
    delete the merged release branch.
