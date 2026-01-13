# src/tradinglab/experiments/exp001_adx_bins.py
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from tradinglab.experiments.base import ExperimentResult, RunContext
from tradinglab.features.adx import add_dmi_adx
from tradinglab.features.returns import add_forward_returns


def _get_params(ctx: RunContext) -> dict[str, Any]:
    cfg = ctx.config if isinstance(ctx.config, dict) else {}
    exp_cfg = cfg.get("experiment", {}) if isinstance(cfg.get("experiment", {}), dict) else {}
    params = exp_cfg.get("params", {}) if isinstance(exp_cfg.get("params", {}), dict) else {}
    return params if isinstance(params, dict) else {}


def _safe_qcut_bins(series: pd.Series, q: int) -> pd.Series:
    """
    Quantile bins with duplicates drop.
    Returns labels like q1/q2/...; if qcut fails, returns NA series.
    """
    if q <= 1:
        return pd.Series(["q1"] * len(series), index=series.index)

    s_non_na = series.dropna()
    if s_non_na.empty:
        return pd.Series([pd.NA] * len(series), index=series.index)

    try:
        cat = pd.qcut(series, q=q, duplicates="drop")
    except Exception:
        return pd.Series([pd.NA] * len(series), index=series.index)

    # Map interval categories to stable labels (q1..qK)
    try:
        categories = list(cat.dtype.categories)  # type: ignore[attr-defined]
        labels = [f"q{i + 1}" for i in range(len(categories))]
        mapping = {categories[i]: labels[i] for i in range(len(categories))}
        return cat.map(mapping)
    except Exception:
        # Fallback: string representation
        return cat.astype("string")


def run(df: pd.DataFrame, ctx: RunContext) -> ExperimentResult:
    """
    Exp001: ADX regime via conditional forward returns.

    - Computes DMI/ADX (Wilder)
    - Computes forward returns at multiple horizons
    - Groups by (ADX bin, ADX slope)
    - Reports both:
        (a) raw forward returns (long-only)
        (b) direction-adjusted returns using DI sign:
            dir=+1 if DI+ > DI-, else -1
            fwd_ret_dir = dir * fwd_ret

    Output (tidy table) columns:
      adx_bin | adx_slope | horizon | n |
      mean | median | hit_rate | mean_abs |
      mean_dir | median_dir | hit_rate_dir
    """
    p = _get_params(ctx)

    dmi_length = int(p.get("dmi_length", 14))
    adx_smoothing = int(p.get("adx_smoothing", 14))
    horizons: Sequence[int] = p.get("horizons", [1, 3, 6, 12, 24])
    horizons = [int(h) for h in horizons]

    slope_lookback = int(p.get("slope_lookback", 3))
    adx_bins = int(p.get("adx_bins", 3))
    binning = str(p.get("binning", "quantile")).lower()

    # ---- Features
    df1 = add_dmi_adx(df, length=dmi_length, adx_smoothing=adx_smoothing)
    df2 = add_forward_returns(df1, horizons=horizons)

    # Direction proxy from DI: +1 if DI+ > DI-, else -1 (NA if missing)
    df2["di_dir"] = pd.NA
    mask_dir = df2["pdi"].notna() & df2["mdi"].notna()
    df2.loc[mask_dir, "di_dir"] = (df2.loc[mask_dir, "pdi"] > df2.loc[mask_dir, "mdi"]).map(
        {True: 1, False: -1}
    )

    # ADX slope: rising/falling based on lookback
    df2["adx_slope_val"] = df2["adx"] - df2["adx"].shift(slope_lookback)
    df2["adx_slope"] = df2["adx_slope_val"].apply(
        lambda x: "rising" if pd.notna(x) and x > 0 else ("falling" if pd.notna(x) else pd.NA)
    )

    # ADX bins: quantile (default) or fixed thresholds
    if binning == "quantile":
        df2["adx_bin"] = _safe_qcut_bins(df2["adx"], q=adx_bins)
    else:
        th_low = float(p.get("adx_th_low", 20))
        th_high = float(p.get("adx_th_high", 25))

        def _bin(x: float) -> str:
            if pd.isna(x):
                return pd.NA  # type: ignore[return-value]
            if x < th_low:
                return "low"
            if x < th_high:
                return "mid"
            return "high"

        df2["adx_bin"] = df2["adx"].apply(_bin)

    # ---- Valid rows: require ADX, DI direction, and returns up to max horizon
    max_h = max(horizons)
    valid = df2["adx"].notna() & df2["di_dir"].notna() & df2[f"fwd_ret_{max_h}"].notna()
    dfv = df2.loc[valid].copy()

    # ---- Aggregation
    rows: list[dict[str, Any]] = []
    group_cols = ["adx_bin", "adx_slope"]

    for (adx_bin, adx_slope), g in dfv.groupby(group_cols, dropna=True):
        for h in horizons:
            col = f"fwd_ret_{h}"
            tmp = g[[col, "di_dir"]].dropna()
            if tmp.empty:
                continue

            r = tmp[col]
            di_dir_num = pd.to_numeric(tmp["di_dir"], errors="coerce")
            r_dir = r * di_dir_num

            rows.append(
                {
                    "adx_bin": str(adx_bin),
                    "adx_slope": str(adx_slope),
                    "horizon": int(h),
                    "n": int(r.shape[0]),
                    # raw long returns
                    "mean": float(r.mean()),
                    "median": float(r.median()),
                    "hit_rate": float((r > 0).mean()),
                    "mean_abs": float(r.abs().mean()),
                    # direction-adjusted (DI sign)
                    "mean_dir": float(r_dir.mean()),
                    "median_dir": float(r_dir.median()),
                    "hit_rate_dir": float((r_dir > 0).mean()),
                }
            )

    results_df = pd.DataFrame(rows)
    if not results_df.empty:
        results_df = results_df.sort_values(["horizon", "adx_bin", "adx_slope"]).reset_index(
            drop=True
        )

    if results_df.empty:
        n_groups = 0
    else:
        n_groups = int(results_df[["adx_bin", "adx_slope"]].drop_duplicates().shape[0])

    metrics: dict[str, Any] = {
        "experiment": "exp001_adx_bins",
        "run_id": ctx.run_id,
        "dmi_length": dmi_length,
        "adx_smoothing": adx_smoothing,
        "horizons": list(horizons),
        "slope_lookback": slope_lookback,
        "adx_bins": adx_bins,
        "binning": binning,
        "n_rows_input": int(len(df)),
        "n_rows_valid": int(len(dfv)),
        "n_groups": n_groups,
    }

    # ---- Markdown report
    report_lines: list[str] = [
        "# Experiment: exp001_adx_bins",
        "",
        "Purpose: test whether ADX regime (level + slope) changes forward return distribution.",
        "Also includes direction-adjusted returns using DI sign: dir=+1 if DI+>DI-, else -1.",
        "",
        "## Params",
        f"- dmi_length: **{dmi_length}**",
        f"- adx_smoothing: **{adx_smoothing}**",
        f"- horizons: **{list(horizons)}**",
        f"- slope_lookback: **{slope_lookback}**",
        f"- binning: **{binning}** (bins={adx_bins})",
        "",
        "## Data",
        f"- input rows: **{metrics['n_rows_input']}**",
        f"- valid rows: **{metrics['n_rows_valid']}**",
        "",
    ]

    if results_df.empty:
        report_lines += ["No results produced (empty results_df). Check data/params."]
    else:
        h_show = max(horizons)
        sub = results_df[results_df["horizon"] == h_show].copy()

        if not sub.empty:
            sub = sub[
                [
                    "adx_bin",
                    "adx_slope",
                    "n",
                    "mean",
                    "hit_rate",
                    "mean_dir",
                    "hit_rate_dir",
                    "mean_abs",
                ]
            ]

            report_lines += [
                f"## Snapshot (horizon={h_show})",
                "",
                "```",
                sub.to_string(index=False),
                "```",
                "",
                "Notes:",
                "- `mean/hit_rate` are raw forward returns (long-only).",
                "- `mean_dir/hit_rate_dir` are direction-adjusted using DI sign.",
                "",
                "Full tidy table is saved in results.parquet/results.csv.",
            ]
        else:
            report_lines += ["Results saved as tidy table in results.parquet/results.csv."]

    report_md = "\n".join(report_lines) + "\n"

    try:
        ctx.logger.info(
            "exp001_adx_bins: valid_rows=%s groups=%s",
            metrics["n_rows_valid"],
            metrics["n_groups"],
        )
    except Exception:
        pass

    return ExperimentResult(metrics=metrics, results_df=results_df, report_md=report_md)
