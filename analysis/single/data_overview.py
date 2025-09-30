"""Generate descriptive statistics and distribution plots for the master data set.

This script reads the ``data/data_master.xlsx`` file, applies project specific
filters, computes overall and per-school descriptive statistics, and exports the
results as Excel workbooks and figures under ``analysis_result/single``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configure basic logging for the script."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def load_data(data_path: Path) -> pd.DataFrame:
    """Load the master Excel file.

    Parameters
    ----------
    data_path: Path
        Location of the ``data_master.xlsx`` file.

    Returns
    -------
    pd.DataFrame
        Loaded dataset.
    """
    LOGGER.info("Loading data from %s", data_path)
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")
    return pd.read_excel(data_path)


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply project specific data filters.

    Only records where ``school_id == 1`` will be restricted to measurement wave
    1, while records for other schools remain unchanged.
    """
    if "school_id" not in df.columns or "measurement_wave" not in df.columns:
        missing = {col for col in ("school_id", "measurement_wave") if col not in df.columns}
        raise KeyError(f"Missing required columns: {missing}")

    LOGGER.info("Applying school-specific filters")
    mask = ~((df["school_id"] == 1) & (df["measurement_wave"] != 1))
    filtered_df = df.loc[mask].reset_index(drop=True)
    LOGGER.info("Filtered dataset shape: %s", filtered_df.shape)
    return filtered_df


def compute_numeric_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Compute descriptive statistics for numeric columns."""
    numeric_df = df.select_dtypes(include=["number"])
    if numeric_df.empty:
        LOGGER.warning("No numeric columns found for summary statistics.")
        return pd.DataFrame()

    summary = numeric_df.describe().T
    summary["missing_count"] = df.shape[0] - numeric_df.count()
    summary["missing_ratio"] = summary["missing_count"] / df.shape[0]
    return summary


def compute_categorical_summary(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Compute frequency counts for categorical columns."""
    categorical_df = df.select_dtypes(exclude=["number", "datetime", "timedelta"])
    summaries: dict[str, pd.Series] = {}
    for column in categorical_df.columns:
        summaries[column] = categorical_df[column].value_counts(dropna=False)
    return summaries


def compute_school_summaries(df: pd.DataFrame, school_column: str = "school_id") -> dict[int, dict[str, pd.DataFrame]]:
    """Compute per-school summaries for numeric and categorical columns."""
    if school_column not in df.columns:
        raise KeyError(f"Missing school identifier column: {school_column}")

    results: dict[int, dict[str, pd.DataFrame]] = {}
    for school_id, school_df in df.groupby(school_column):
        LOGGER.info("Summarising data for school %s", school_id)
        results[school_id] = {
            "numeric": compute_numeric_summary(school_df),
            "categorical": {col: series for col, series in compute_categorical_summary(school_df).items()},
        }
    return results


def save_excel_reports(
    overall_numeric: pd.DataFrame,
    overall_categorical: dict[str, pd.Series],
    school_summaries: dict[int, dict[str, pd.DataFrame]],
    output_path: Path,
) -> None:
    """Save overall and per-school summaries to an Excel workbook."""
    LOGGER.info("Writing Excel report to %s", output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        if not overall_numeric.empty:
            overall_numeric.to_excel(writer, sheet_name="overall_numeric")
        if overall_categorical:
            for column, counts in overall_categorical.items():
                counts.to_frame(name="count").to_excel(writer, sheet_name=f"overall_cat_{column[:25]}")

        for school_id, summaries in school_summaries.items():
            numeric = summaries.get("numeric")
            categorical = summaries.get("categorical")

            if numeric is not None and not numeric.empty:
                numeric.to_excel(writer, sheet_name=f"school_{school_id}_numeric")
            if categorical:
                for column, counts in categorical.items():
                    counts.to_frame(name="count").to_excel(
                        writer, sheet_name=f"school_{school_id}_cat_{str(column)[:20]}"
                    )


def generate_distribution_plots(df: pd.DataFrame, figure_dir: Path, hue: str = "school_id") -> None:
    """Generate distribution plots for numeric features."""
    figure_dir.mkdir(parents=True, exist_ok=True)
    numeric_columns: Iterable[str] = df.select_dtypes(include=["number"]).columns

    sns.set_theme(style="whitegrid")

    for column in numeric_columns:
        plt.figure(figsize=(8, 5))
        sns.histplot(data=df, x=column, hue=hue if hue in df.columns else None, kde=True, element="step")
        plt.title(f"Distribution of {column}")
        plt.tight_layout()
        hist_path = figure_dir / f"{column}_hist.png"
        plt.savefig(hist_path, dpi=300)
        plt.close()

        plt.figure(figsize=(6, 5))
        if hue in df.columns:
            sns.boxplot(data=df, x=hue, y=column)
            plt.title(f"{column} by {hue}")
        else:
            sns.boxplot(y=df[column])
            plt.title(f"{column} distribution")
        plt.tight_layout()
        box_path = figure_dir / f"{column}_box.png"
        plt.savefig(box_path, dpi=300)
        plt.close()


def main() -> None:
    configure_logging()

    root_dir = Path(__file__).resolve().parents[2]
    data_path = root_dir / "data" / "data_master.xlsx"
    result_dir = root_dir / "analysis_result" / "single"
    figure_dir = result_dir / "figure"
    excel_output_path = result_dir / "data_overview.xlsx"

    df = load_data(data_path)
    df = apply_filters(df)

    overall_numeric = compute_numeric_summary(df)
    overall_categorical = compute_categorical_summary(df)
    school_summaries = compute_school_summaries(df)

    save_excel_reports(overall_numeric, overall_categorical, school_summaries, excel_output_path)
    generate_distribution_plots(df, figure_dir)

    LOGGER.info("Analysis completed successfully.")


if __name__ == "__main__":
    main()
