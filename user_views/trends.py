from shiny import ui, render
from shinywidgets import output_widget, render_plotly
import pandas as pd

from dashboard.draw_charts import line_trend
from fetch_census_data import (
    YEARS,
    call_api_ttl_many,
    CITY_GEO,
    US_GEO,
    zcta_geo,
)

TREND_CODE = {
    "Total Population": "B01003_001E",
    "Median Age": "B01002_001E",
    "Sex by Age (full)": "B01001_001E",
    "Race (full)": "B02001_001E",
    "Hispanic/Latino Origin (full)": "B03002_001E",
}

YEARS_FAST = [y for y in YEARS if y >= max(YEARS) - 2]

def panel():
    return ui.page_fluid(
        ui.row(
            ui.column(3, ui.input_checkbox("more_years", "Show all years (slower)", value=False)),
            ui.column(3, ui.input_checkbox("all_zips", "All ZIPs (slower)", value=False)),
        ),
        output_widget("plot_trends"),
        ui.output_data_frame("tbl_trends"),
    )

def server_bind(output, input):
    def _area_name():
        g = input.geo_kind()
        return {"City": "City", "US": "US"}.get(g, f"ZIP {input.zip()}")

    def _geo():
        g = input.geo_kind()
        if g == "City":
            return CITY_GEO
        if g == "US":
            return US_GEO
        return zcta_geo(input.zip())

    def _years():
        return YEARS if input.more_years() else YEARS_FAST

    def _series(area_name, geo):
        _ = input.refresh()  # react to button
        code = TREND_CODE.get(input.topic(), "B01003_001E")
        frames = call_api_ttl_many(_years(), code, geo)

        rows = []
        for df_y in frames:
            if df_y.empty:
                continue
            y = int(df_y["Year"].iloc[0])
            val = pd.to_numeric(df_y.get(code), errors="coerce").sum() if code in df_y.columns else None
            rows.append({"Year": y, "Area": area_name, "Value": val})

        out = pd.DataFrame(rows)
        return out.sort_values(["Area", "Year"], ignore_index=True)

    @render_plotly
    def plot_trends():
        area = _area_name()
        df = _series(area, _geo())
        yr_min, yr_max = min(_years()), max(_years())
        return line_trend(df, f"Trends — {input.topic()} ({yr_min}–{yr_max}, {area})")

    @render.data_frame
    def tbl_trends():
        return _series(_area_name(), _geo())
