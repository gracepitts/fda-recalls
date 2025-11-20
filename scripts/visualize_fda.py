import os
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from config import DUCKDB_PATH

PLOT_DIR = "plots"
os.makedirs(PLOT_DIR, exist_ok=True)

def fetch_df(sql: str) -> pd.DataFrame:
    with duckdb.connect(DUCKDB_PATH) as conn:
        return conn.execute(sql).df()

def plot_yearly_trend():
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
    df = fetch_df("SELECT * FROM v_reasons")
    if df.empty:
        return
    plt.figure(figsize=(10,7))
    df = df.sort_values("recalls")
    plt.barh(df["reason_for_recall"].str.slice(0,60), df["recalls"])
    plt.title("Most Common Reasons for Recall (truncated labels)")
    plt.xlabel("Recalls")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, "reasons.png"))
    plt.close()

def plot_class_distribution():
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
    """Compare counts of recalls for drug vs food over time (by year)."""
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

    # Pivot so each source is a column
    df_pivot = df.pivot(index="year", columns="source", values="recalls").fillna(0)
    # Sort years numerically
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
    """Create enhanced comparison plots: stacked area, normalized per-year, and cumulative counts."""
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

    # Stacked area plot
    plt.figure(figsize=(10,6))
    df_pivot.plot(kind='area', stacked=True, alpha=0.6)
    plt.title('Stacked Area: Recalls by Source over Years')
    plt.xlabel('Year')
    plt.ylabel('Recalls')
    plt.legend(title='Source')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_stacked_area.png'))
    plt.close()

    # Normalized (percent) per-year
    df_norm = df_pivot.div(df_pivot.sum(axis=1).replace(0, 1), axis=0)
    plt.figure(figsize=(10,6))
    df_norm.plot(kind='line', marker='o')
    plt.title('Normalized Share of Recalls by Source (per year)')
    plt.xlabel('Year')
    plt.ylabel('Share of Recalls')
    plt.legend(title='Source')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_normalized_share.png'))
    plt.close()

    # Cumulative counts
    df_cum = df_pivot.cumsum()
    plt.figure(figsize=(10,6))
    df_cum.plot(marker='o')
    plt.title('Cumulative Recalls by Source')
    plt.xlabel('Year')
    plt.ylabel('Cumulative Recalls')
    plt.legend(title='Source')
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_cumulative.png'))
    plt.close()

def plot_drug_food_monthly():
    """Monthly trend (last 5 years) for finer-grained comparison."""
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
    # focus on last 60 months if present
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
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOT_DIR, 'drug_food_monthly_recent.png'))
    plt.close()

if __name__ == "__main__":
    plot_yearly_trend()
    plot_top_firms()
    plot_reasons()
    plot_class_distribution()
    plot_drug_food_comparison()
    plot_drug_food_enhanced()
    plot_drug_food_monthly()

