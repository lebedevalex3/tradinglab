# TradingLab — Run Contract

**Run Contract Version:** 1.0  
**Status:** Active  
**Applies to:** All backtest, research, and execution runs

---

## 1. Purpose

This document defines the **run folder contract** — the mandatory structure and contents of a single strategy run.

The run contract ensures:

- full **reproducibility** of experiments,
- clear **traceability** (what was run, when, and how),
- separation between **code** and **results**,
- long-term maintainability of research outputs.

The contract is **strategy-agnostic** and applies to all current and future strategies.

Any change to this contract:

- requires updating this document,
- requires updating validation tests,
- must bump the contract version.

---

## 2. Core Principle

> **One run = one folder = one self-contained unit of analysis**

A run folder must allow a third party (or future you) to understand and reproduce the experiment **without**:

- inspecting source code history,
- relying on memory,
- searching for missing configuration files.

---

## 3. Run Folder Location

All runs must be stored under:
artifacts/runs/<run_id>/

Where:

- `<run_id>` is a unique identifier, typically:
  - UTC timestamp (`YYYY-MM-DD_HH-MM-SS`), or
  - timestamp + short hash.

---

## 4. Mandatory Run Folder Structure

artifacts/runs/<run_id>/
meta.json
summary.json
report.md
trades.parquet
config.yml

All files listed above are **required**.  
A run folder missing any of these files is considered **invalid**.

---

## 5. File Specifications

### 5.1. `meta.json` — Run Passport (Machine-Readable)

**Purpose:**  
Provides immutable metadata required for traceability and audit.

**Required fields:**

```json
{
  "run_id": "2026-01-11_10-32-05",
  "created_at_utc": "2026-01-11T10:32:05Z",
  "git_commit": "a1b2c3d",
  "strategy_version": "mr-envelope-v0.1.0",
  "data_path": "data/sample/btcusdt_10m.parquet",
  "params": {}
}
```

### 5.2. config.yml — Configuration Snapshot

**Purpose:**
Allows exact reproduction of the run.

**Rules:**

This file is a verbatim copy of the configuration used to start the run.

The file must not be edited after run creation.

Used as the canonical input for reruns.

### 5.3. summary.json — Run Summary (Machine-Readable)

**Purpose:**
Provides a compact overview suitable for dashboards and batch analysis.

Minimum structure:

```json
{
  "rows_in_data": 2000,
  "trades": 0,
  "status": "ok",
  "note": "Milestone 0 smoke run"
}
```

Notes:

- Additional metrics (PnL, drawdown, PF, etc.) may be added in later milestones.

- Existing keys must remain backward-compatible.

### 5.4. `trades.parquet` — Trade Log

**Purpose:**  
Stores detailed trade-level data for analysis.

**Rules:**

- The file must always exist, even if there are zero trades.
- Format must be columnar (`Parquet`).

**Minimum schema:**

| Column    | Type     | Description                       |
| --------- | -------- | --------------------------------- |
| timestamp | datetime | Trade timestamp (UTC)             |
| side      | string   | `buy` / `sell` / `long` / `short` |
| price     | float    | Execution price                   |
| qty       | float    | Executed quantity                 |

### 5.5. report.md — Human-Readable Report

**Purpose:**
Provides a quick, readable overview without requiring code execution.

**Minimum content:**

- run identifier
- strategy version
- git commit
  -dataset path and size
  -trade count
  -execution status

Example:

```markdown
# TradingLab — Run Report

Run ID: 2026-01-11_10-32-05  
Strategy: mr-envelope-v0.1.0  
Git commit: a1b2c3d

Data:

- Path: data/sample/btcusdt_10m.parquet
- Rows: 2000

Trades: 0

Status: OK
```
