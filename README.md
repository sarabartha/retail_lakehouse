# retail_lakehouse

A practice data product on Databricks: a medallion-style retail lakehouse built as a
Lakeflow Declarative Pipeline, defined as code with Databricks Asset Bundles, and
tested and released through GitHub Actions.

It uses the public TPC-H sample data (`samples.tpch.orders` and `samples.tpch.customer`),
so no real or sensitive data is involved.

## What it does

```
samples.tpch.orders ──► CSV files in a landing volume ──► bronze ──► silver ──► gold
samples.tpch.customer ─────────────────────────────────► bronze ──► silver ──┘
```

- **Landing:** a notebook drops one month of orders as CSV files into a Unity Catalog volume,
  playing the role of a source system.
- **Bronze:** raw data loaded incrementally. Orders arrive through Auto Loader (streaming table),
  with `source_file` and `ingested_at` for traceability.
- **Silver:** validated orders and customers. Data quality is enforced with expectations:
  warn (`has_comment`), drop (`positive_price`, `known_segment`) and fail (`has_order_key`).
- **Gold:** `gold_revenue_by_segment`, monthly orders, urgent orders and revenue per
  market segment.
- **Check:** after each refresh, `check_gold` validates the gold table as a whole
  (not empty, all five segments present, no non-positive revenue, urgent orders never
  above total orders) and fails the job if any rule breaks.

Everything runs as one job, `retail_job`: land files → refresh pipeline → check gold.

## Repository structure

```
retail_lakehouse/
├── .github/workflows/ci.yml        # PR checks and manual releases
├── databricks.yml                  # bundle settings, variables and targets
├── resources/
│   ├── schema.yml                  # the schema, created by the bundle
│   ├── volume.yml                  # landing volume for source files
│   ├── retail_pipeline.pipeline.yml
│   ├── retail_job.job.yml
│   └── land_orders_job.job.yml
├── src/
│   ├── pipeline/                   # bronze, silver and gold definitions
│   ├── checks/                     # check_gold notebook and gold_rules module
│   └── tools/                      # land_orders notebook (fake source system)
├── tests/                          # unit tests (pytest)
├── pyproject.toml, uv.lock         # Python tooling, managed by uv
└── .python-version                 # Python 3.12
```

## Environments

| Target    | Purpose                                   | Data loaded  | Deployed by                     |
|-----------|-------------------------------------------|--------------|---------------------------------|
| `dev`     | Personal development, prefixed with your name | From 1998 | You, from your laptop           |
| `ci`      | Integration test of every pull request    | From 1998    | GitHub Actions, on pull request |
| `staging` | Release candidate check                   | From 1995    | GitHub Actions, manual, from `main` |
| `prod`    | Production                                | From 1992    | GitHub Actions, manual, from a tag, after approval |

Dev, ci and staging load less history to keep runs fast and cheap. The amount is set by
the `min_order_date` variable per target.

## Getting started

Prerequisites: Python 3.12, [uv](https://docs.astral.sh/uv/), the Databricks CLI, and
access to the workspace.

```bash
# Install the development tools from the lock file
uv sync --locked

# Log in to the workspace (opens a browser)
databricks auth login --host <workspace-url>

# Deploy and run your own dev copy
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev retail_job
```

## Development workflow

This repo follows trunk-based development:

1. Start from the latest `main`: `git checkout main` and `git pull`.
2. Create a short-lived branch, lowercase with hyphens, for example `add-urgent-orders`.
3. Test your change in `dev` before opening a pull request.
4. Open a pull request with a clear title and a description covering **what**, **why**
   and **how it was tested**.
5. When the checks are green and the PR is approved, merge with **Squash and merge**.
   The PR title and description become the commit on `main`.

Commit messages use a capitalized summary of about 50 characters in imperative mood
("Add …", not "Added …"), a blank line, then a body explaining what and why, wrapped at
72 characters.

## Quality checks

Run these before pushing:

```bash
uv run ruff check .          # lint: errors, naming, Google-style docstrings
uv run ruff format .         # format code consistently
uv run pytest                # unit tests (needs Java 17 for local PySpark)
```

## CI/CD

The workflow in `.github/workflows/ci.yml` has two parts.

**On every pull request into `main`:**

1. `quality`: Ruff lint and format check, and unit tests.
2. `test-in-ci`: deploys the bundle to the `ci` target and runs `retail_job` on real
   sample data, including the gold checks.

Both must pass before a pull request can be merged.

**Releases (manual, through workflow dispatch):**

1. **Staging:** Actions → *Retail CI/CD* → *Run workflow*, use workflow from `main`,
   target `staging`.
2. **Tag the release** once staging looks right, with the product short name and a
   semantic version:

   ```bash
   git tag -a RTL-v1.0.0 -m "Describe the release"
   git push origin RTL-v1.0.0
   ```

3. **Prod:** *Run workflow* from the tag, target `prod`. The run waits for approval
   in the `prod` environment before deploying.

The workflow refuses staging releases from anything but `main`, and prod releases from
anything but an `RTL-v*` tag.

## Conventions

- Python 3.12, matching serverless environment version 4 and DBR 17.3 LTS
- lower snake_case for files, folders, tables, jobs and variables
- Google-style docstrings and type hints for Python functions
- No secrets in the repository: locally the Databricks CLI login is used, and in CI the
  credentials are GitHub secrets
