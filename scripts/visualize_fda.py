"""Visualize FDA recall data from DuckDB."""

import logging
import sys
from pathlib import Path
from typing import Optional

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Add parent directory to path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config.LOGS_DIR / "visualize_fda.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)


def get_data_from_db() -> Optional[pd.DataFrame]:
    """
    Fetch all recall data from DuckDB.

    Returns:
        DataFrame with recall data or None if error
    """
    if not config.DB_PATH.exists():
        logger.error(f"Database not found at {config.DB_PATH}")
        return None

    try:
        conn = duckdb.connect(str(config.DB_PATH), read_only=True)
        df = conn.execute("SELECT * FROM recalls").fetchdf()
        conn.close()
        logger.info(f"Loaded {len(df)} records from database")
        return df
    except Exception as e:
        logger.error(f"Error loading data from database: {e}")
        return None


def plot_recalls_by_type(df: pd.DataFrame, output_path: Optional[Path] = None) -> None:
    """
    Create bar plot of recalls by type.

    Args:
        df: DataFrame with recall data
        output_path: Optional path to save the plot
    """
    logger.info("Creating recalls by type plot")
    plt.figure(figsize=(10, 6))

    type_counts = df["recall_type"].value_counts()
    type_counts.plot(kind="bar", color=["#FF6B6B", "#4ECDC4", "#45B7D1"])

    plt.title("FDA Recalls by Type", fontsize=16, fontweight="bold")
    plt.xlabel("Recall Type", fontsize=12)
    plt.ylabel("Number of Recalls", fontsize=12)
    plt.xticks(rotation=0)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        logger.info(f"Saved plot to {output_path}")

    plt.show()


def plot_recalls_by_classification(
    df: pd.DataFrame, output_path: Optional[Path] = None
) -> None:
    """
    Create bar plot of recalls by classification.

    Args:
        df: DataFrame with recall data
        output_path: Optional path to save the plot
    """
    logger.info("Creating recalls by classification plot")
    plt.figure(figsize=(10, 6))

    classification_counts = df["classification"].value_counts()
    classification_counts.plot(kind="bar", color=sns.color_palette("viridis", len(classification_counts)))

    plt.title("FDA Recalls by Classification", fontsize=16, fontweight="bold")
    plt.xlabel("Classification", fontsize=12)
    plt.ylabel("Number of Recalls", fontsize=12)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        logger.info(f"Saved plot to {output_path}")

    plt.show()


def plot_recalls_by_state(
    df: pd.DataFrame, top_n: int = 10, output_path: Optional[Path] = None
) -> None:
    """
    Create bar plot of top states by number of recalls.

    Args:
        df: DataFrame with recall data
        top_n: Number of top states to show
        output_path: Optional path to save the plot
    """
    logger.info(f"Creating top {top_n} states by recalls plot")
    plt.figure(figsize=(12, 6))

    state_counts = df["state"].value_counts().head(top_n)
    state_counts.plot(kind="barh", color=sns.color_palette("rocket", len(state_counts)))

    plt.title(f"Top {top_n} States by Number of Recalls", fontsize=16, fontweight="bold")
    plt.xlabel("Number of Recalls", fontsize=12)
    plt.ylabel("State", fontsize=12)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        logger.info(f"Saved plot to {output_path}")

    plt.show()


def plot_recalls_over_time(
    df: pd.DataFrame, output_path: Optional[Path] = None
) -> None:
    """
    Create line plot of recalls over time.

    Args:
        df: DataFrame with recall data
        output_path: Optional path to save the plot
    """
    logger.info("Creating recalls over time plot")

    # Convert date columns to datetime
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")

    # Filter out null dates and group by month
    df_with_dates = df[df["report_date"].notna()].copy()
    df_with_dates["year_month"] = df_with_dates["report_date"].dt.to_period("M")

    plt.figure(figsize=(14, 6))
    recalls_by_month = df_with_dates.groupby(["year_month", "recall_type"]).size().unstack(fill_value=0)

    recalls_by_month.plot(kind="line", marker="o", linewidth=2)

    plt.title("FDA Recalls Over Time by Type", fontsize=16, fontweight="bold")
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Number of Recalls", fontsize=12)
    plt.legend(title="Recall Type", loc="best")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        logger.info(f"Saved plot to {output_path}")

    plt.show()


def generate_all_visualizations(save_plots: bool = False) -> None:
    """
    Generate all visualizations.

    Args:
        save_plots: Whether to save plots to files
    """
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting visualization generation")

    df = get_data_from_db()
    if df is None or len(df) == 0:
        logger.error("No data available for visualization")
        return

    output_dir = config.DATA_DIR / "visualizations" if save_plots else None
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)

    plot_recalls_by_type(df, output_dir / "recalls_by_type.png" if output_dir else None)
    plot_recalls_by_classification(df, output_dir / "recalls_by_classification.png" if output_dir else None)
    plot_recalls_by_state(df, top_n=10, output_path=output_dir / "recalls_by_state.png" if output_dir else None)
    plot_recalls_over_time(df, output_dir / "recalls_over_time.png" if output_dir else None)

    logger.info("Visualization generation complete")


if __name__ == "__main__":
    generate_all_visualizations(save_plots=True)
