import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from config import DUCKDB_PATH

# Directory where all plots will be stored
PLOT_DIR = "plots"
os.makedirs(PLOT_DIR, exist_ok=True)

def fetch_df(sql: str) -> pd.DataFrame:
    """
    Helper that runs a SQL query against DuckDB and returns a DataFrame.
    Used by all plotting functions.
    """
    with duckdb.connect(DUCKDB_PATH) as conn:
        return conn.execute(sql).df()

def plot_yearly_trend():
    """Line chart showing count of recalls per year."""
    df = fetch_df("SELECT * FROM v_yearly_counts")
    if df.empty:
        return
    plt.figure()
    plt.plot(df["year"], df["recalls"], marker="o")
    plt.title("FDA Drug Recalls by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Recalls")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "yearly_recalls.png"))
    plt.close()

def plot_top_firms():
    """Horizontal bar chart of the top 20 recalling firms."""
    df = fetch_df("SELECT * FROM v_top_firms")
    if df.empty:
        return
    plt.figure(figsize=(9,6))
    df = df.sort_values("recalls")
    plt.barh(df["recalling_firm"], df["recalls"])
    plt.title("Top Firms by Number of Recalls")
    plt.xlabel("Recalls")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "top_firms.png"))
    plt.close()

def plot_reasons():
    """Horizontal bar chart for most common recall reasons."""
    df = fetch_df("SELECT * FROM v_reasons")
    if df.empty:
        return
    plt.figure(figsize=(10,7))
    df = df.sort_values("recalls")
    # Truncate text labels to avoid overcrowding
    plt.barh(df["reason_for_recall"].str.slice(0,60), df["recalls"])
    plt.title("Most Common Reasons for Recall (truncated labels)")
    plt.xlabel("Recalls")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "reasons.png"))
    plt.close()

def plot_class_distribution():
    """Bar chart showing distribution of recall classes (e.g., Class I, II, III)."""
    df = fetch_df("SELECT * FROM v_class_distribution")
    if df.empty:
        return
    plt.figure()
    plt.bar(df["classification"], df["recalls"])
    plt.title("Recall Class Distribution")
    plt.xlabel("Class")
    plt.ylabel("Recalls")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "class_distribution.png"))
    plt.close()

def plot_drug_food_comparison():
    """
    Line chart comparing recalls by source (drug vs food)
    aggregated by year using enforcement_raw table.
    """
    sql = (
        "SELECT source, SUBSTR(report_date, 1, 4) AS year, COUNT(*) AS recalls "
        "FROM enforcement_raw "
        "WHERE report_date IS NOT NULL AND source IS NOT NULL "
        "GROUP BY source, year "
        "ORDER BY year"
    )
    df = fetch_df(sql)
    if df.empty:
        return

    # Pivot so each recall source forms a separate line
    df_pivot = df.pivot(index="year", columns="source", values="recalls").fillna(0)
    df_pivot.index = df_pivot.index.astype(int)
    df_pivot = df_pivot.sort_index()

    plt.figure(figsize=(10,6))
    for col in df_pivot.columns:
        plt.plot(df_pivot.index, df_pivot[col], marker="o", label=str(col))

    plt.title("Drug vs Food Recalls by Year")
    plt.xlabel("Year")
    plt.ylabel("Number of Recalls")
    plt.legend(title="Source")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "drug_vs_food_recalls.png"))
    plt.close()

def plot_drug_food_enhanced():
    """
    Three enhanced visuals for deeper trend insight:
    1. Stacked area (volume over time)
    2. Normalized share (percentage by year)
    3. Cumulative totals (long-term divergence)
    """
    sql = (
        "SELECT source, SUBSTR(report_date, 1, 4) AS year, COUNT(*) AS recalls "
        "FROM enforcement_raw "
        "WHERE report_date IS NOT NULL AND source IS NOT NULL "
        "GROUP BY source, year "
        "ORDER BY year"
    )
    df = fetch_df(sql)
    if df.empty:
        return

    df_pivot = df.pivot(index="year", columns="source", values="recalls").fillna(0)
    df_pivot.index = df_pivot.index.astype(int)
    df_pivot = df_pivot.sort_index()

    # 1) Stacked Area Plot
    plt.figure(figsize=(10,6))
    df_pivot.plot(kind='area', stacked=True, alpha=0.6)
    plt.title('Stacked Area: Recalls by Source over Years')
    plt.xlabel('Year')
    plt.ylabel('Recalls')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_stacked_area.png'))
    plt.close()

    # 2) Normalized yearly share
    df_norm = df_pivot.div(df_pivot.sum(axis=1).replace(0, 1), axis=0)
    plt.figure(figsize=(10,6))
    df_norm.plot(kind='line', marker='o')
    plt.title('Normalized Share of Recalls by Source (per year)')
    plt.xlabel('Year')
    plt.ylabel('Share of Recalls')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_normalized_share.png'))
    plt.close()

    # 3) Cumulative recalls
    df_cum = df_pivot.cumsum()
    plt.figure(figsize=(10,6))
    df_cum.plot(marker='o')
    plt.title('Cumulative Recalls by Source')
    plt.xlabel('Year')
    plt.ylabel('Cumulative Recalls')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_cumulative.png'))
    plt.close()

def plot_drug_food_monthly():
    """
    Monthly granularity (last ~5 years only)
    Helps detect short-term spikes or seasonality patterns.
    """
    sql = (
        "SELECT source, SUBSTR(report_date,1,6) AS yearmonth, COUNT(*) AS recalls "
        "FROM enforcement_raw "
        "WHERE report_date IS NOT NULL AND source IS NOT NULL "
        "GROUP BY source, yearmonth "
        "ORDER BY yearmonth"
    )
    df = fetch_df(sql)
    if df.empty:
        return

    df_pivot = df.pivot(index='yearmonth', columns='source', values='recalls').fillna(0)

    # Restrict to most recent 60 months for readability
    if len(df_pivot) > 60:
        df_pivot = df_pivot.tail(60)

    plt.figure(figsize=(12,6))
    for col in df_pivot.columns:
        plt.plot(df_pivot.index, df_pivot[col], marker='o', label=str(col))

    plt.title('Monthly Recalls (last ~5 years) by Source')
    plt.xlabel('YearMonth')
    plt.ylabel('Recalls')
    plt.xticks(rotation=45)
    plt.legend(title='Source')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_monthly_recent.png'))
    plt.close()

if __name__ == "__main__":
    # Run all plots when this script is executed standalone
    plot_yearly_trend()
    plot_top_firms()
    plot_reasons()
    plot_class_distribution()
    plot_drug_food_comparison()
    plot_drug_food_enhanced()
    plot_drug_food_monthly()
