# dashboard/draw_charts.py — Plotly chart helpers (kept same function names)
# What it does:
# - bar_top(): tidy -> Top-N bar with Display on X and Value on Y
# - line_trend(): tidy -> multi-line time series by Area

import pandas as pd
import plotly.express as px

def bar_top(df: pd.DataFrame, title: str, top_n: int = 20):
    """
    Expect a tidy frame with columns:
      - Display: label for each bar (e.g., 'Total population')
      - Value: numeric estimate
    We aggregate by Display (in case multiple rows share the same label),
    take the Top-N, and draw a Plotly bar.
    """
    if df.empty:
        return px.scatter(title="No data")

    agg = df.groupby("Display", as_index=False)["Value"].sum()
    top = agg.sort_values("Value", ascending=False).head(top_n)
    fig = px.bar(top, x="Display", y="Value", title=title)
    fig.update_layout(xaxis_title="", yaxis_title="Estimate", bargap=0.2)
    return fig

def line_trend(df: pd.DataFrame, title: str):
    """
    Expect a tidy frame with columns:
      - Year: int
      - Value: numeric estimate
      - Area: legend label (e.g., 'City', 'US', 'ZIP 90813')
    Draw a Plotly line chart with markers.
    """
    if df.empty:
        return px.scatter(title="No data")
    fig = px.line(df, x="Year", y="Value", color="Area", markers=True, title=title)
    fig.update_layout(yaxis_title="Estimate")
    return fig
