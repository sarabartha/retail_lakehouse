# retail_lakehouse

Practice data products on Databricks, built as Lakeflow Declarative Pipelines, defined as
code with Databricks Asset Bundles, and tested and released through GitHub Actions.

The repository is a monorepo: each data product lives in its own folder under its domain
(`sales/`) and is deployed as its own bundle.

All products use the public TPC-H sample data (`samples.tpch.orders` and
`samples.tpch.customer`), so no real or sensitive data is involved.

## Data products

| Product | Folder | Short name | Bundle | Main job |
|---------|--------|------------|--------|----------|
| Retail lakehouse | `sales/retail_lakehouse_rtl` | `RTL` | `retail_lakehouse` | `retail_job` |
| Orders | `sales/orders_ord` | `ORD` | `orders` | `orders_job` |

The short name at the end of the folder name is used for release tags (see [CI/CD](#cicd)).

### Retail lakehouse (`RTL`)

A medallion-style lakehouse with revenue per market segment as its result.

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

### Orders (`ORD`)

A core data product for orders, plus a star schema data set built on top of it.

```
samples.tpch.orders ──► JSON files in a landing volume ──► raw ──► processed ──► serve ──► orders_star
```

- **Landing:** `land_orders_day` drops one day of orders as JSON lines into the `landing`
  volume.
- **Raw** (`sales_raw` schema): each landed line loaded untouched, with load metadata.
- **Processed** (`sales_processed` schema): the JSON parsed, keeping only the most recently
  loaded copy of each order.
- **Serve** (`sales` schema): typed, readable, source-aligned columns. Expectations drop
  non-positive prices (`positive_price`) and fail on missing order keys (`has_order_key`).
- **Star schema** (`orders_star` schema, in the data sets catalog): `fact_orders`,
  `dim_customer` and `dim_date`, built from the serve layer.

The core data product lives in the core data products catalog (`cdp_catalog`), and the star
schema in the data sets catalog (`datasets_catalog`).

`orders_job` runs land one day → refresh `orders_cdp` → refresh `orders_star`.

## Repository structure

```
retail_lakehouse/
├── .github/workflows/deploy.yml        # PR checks and manual releases, for all products
├── sales/
│   ├── retail_lakehouse_rtl/
│   │   ├── databricks.yml              # bundle settings, variables and targets
│   │   ├── resources/                  # schema, volume, pipeline and jobs
│   │   └── src/
│   │       ├── pipeline/               # bronze, silver and gold definitions
│   │       ├── checks/                 # check_gold notebook and gold_rules module
│   │       └── tools/                  # land_orders notebook (fake source system)
│   └── orders_ord/
│       ├── databricks.yml
│       ├── resources/                  # schemas, volume, pipelines and jobs
│       └── src/
│           ├── orders_cdp/             # landing notebook and raw/processed/serve pipeline
│           └── datasets/               # orders star schema pipeline
├── tests/                              # unit tests (pytest)
├── pyproject.toml, uv.lock             # Python tooling for the whole repo, managed by uv
└── .python-version                     # Python 3.12
```

## Environments

Each product has the same four targets in its `databricks.yml`:

| Target    | Purpose                                   | Data loaded  | Deployed by                     |
|-----------|-------------------------------------------|--------------|---------------------------------|
| `dev`     | Personal development, prefixed with your name | From 1998 | You, from your laptop           |
| `ci`      | Integration test of every pull request    | From 1998    | GitHub Actions, on pull request |
| `staging` | Release candidate check                   | From 1995    | GitHub Actions, manual, from `main` |
| `prod`    | Production                                | From 1992    | GitHub Actions, manual, from a tag, after approval |

Dev, ci and staging load less history to keep runs fast and cheap. The amount is set by
the `min_order_date` variable per target.

| Target    | Core data products catalog   | Data sets catalog   |
|-----------|------------------------------|---------------------|
| `dev`/`ci`| `dev_core_data_products`     | `dev_datasets`      |
| `staging` | `staging_core_data_products` | `staging_datasets`  |
| `prod`    | `core_data_products`         | `datasets`          |

## Getting started

Prerequisites: Python 3.12, [uv](https://docs.astral.sh/uv/), Java 17 (for local PySpark
tests), the Databricks CLI, and access to the workspace.

```bash
# Install the development tools from the lock file (from the repo root)
uv sync --locked

# Log in to the workspace (opens a browser)
databricks auth login --host <workspace-url>

# Deploy and run your own dev copy of a product, from its folder
cd sales/retail_lakehouse_rtl        # or sales/orders_ord
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev retail_job   # or orders_job
```

Bundle commands only work from inside a product folder, where its `databricks.yml` is.

## Development workflow

This repo follows trunk-based development:

1. Start from the latest `main`: `git checkout main` and `git pull`.
2. Create a short-lived branch, lowercase with hyphens, for example `add-urgent-orders`.
3. Test your change in `dev` before opening a pull request.
4. Open a pull request with a clear title and a description covering **what**, **why**
   and **how it was tested**.
5. When the checks are green and the PR is approved, merge with **Squash and merge**.
   The PR title and description become the commit on `main`.

`main` is protected: changes reach it only through pull requests.

Commit messages use a capitalized summary of about 50 characters in imperative mood
("Add …", not "Added …"), a blank line, then a body explaining what and why, wrapped at
72 characters.

## Quality checks

Run these from the repo root before pushing:

```bash
uv run ruff check .          # lint: errors, naming, Google-style docstrings
uv run ruff format .         # format code consistently
uv run pytest                # unit tests (needs Java 17 for local PySpark)
```

## CI/CD

The workflow in `.github/workflows/deploy.yml` (*Deploy Databricks Asset Bundle*) has two
parts.

**On every pull request into `main`:**

1. `quality`: Ruff lint and format check, and unit tests, once for the whole repo.
2. `test-in-ci`: for each product, one at a time, deploys the bundle to the `ci` target
   and runs its main job on real sample data.

Both must pass before a pull request can be merged.

**Releases (manual, through workflow dispatch), one product at a time:**

1. **Staging:** Actions → *Deploy Databricks Asset Bundle* → *Run workflow*, use workflow
   from `main`, choose the product and target `staging`.
2. **Tag the release** once staging looks right, with the product short name and a
   semantic version:

   ```bash
   git tag -a RTL-v1.0.0 -m "Describe the release"    # or ORD-v1.0.0
   git push origin RTL-v1.0.0
   ```

3. **Prod:** *Run workflow* from the tag, choose the same product and target `prod`.
   The run waits for approval in the `prod` environment before deploying.

The workflow refuses staging releases from anything but `main`, and prod releases from
anything but a tag matching the product, such as `RTL-v*` for the retail lakehouse or
`ORD-v*` for orders.

## Conventions

- One folder per data product, named `<product>_<short name>`, under its domain folder
- Python 3.12, matching serverless environment version 4 and DBR 17.3 LTS
- lower snake_case for files, folders, tables, jobs and variables
- Google-style docstrings and type hints for Python functions
- No secrets in the repository: locally the Databricks CLI login is used, and in CI the
  credentials are GitHub secrets
