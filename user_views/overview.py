from shiny import ui, render
from shinywidgets import output_widget, render_plotly
import pandas as pd

from dashboard.draw_charts import bar_top
from fetch_census_data import (
    topic_to_vars_min,
    call_api_vars_ttl,
    CITY_GEO,
    US_GEO,
    zcta_geo,
    tidy_long,
)

def panel():
    return ui.page_fluid(
        ui.row(
            ui.column(
                3,
                ui.input_checkbox(
                    "adv_full", "Advanced: pull full table (slower)", value=False
                ),
            ),
        ),
        # Plotly widget output
        output_widget("plot_overview"),
        ui.output_data_frame("tbl_overview"),
    )

def server_bind(output, input):
    def _geo():
        g = input.geo_kind()
        if g == "City":
            return CITY_GEO
        if g == "US":
            return US_GEO
        return zcta_geo(input.zip())

    def _codes():
        topic = input.topic()
        codes = topic_to_vars_min(topic)  # small, explicit list

        # Optional “Advanced” toggles (explicit, but slower)
        if input.adv_full():
            if topic == "Race (full)":
                return [f"B02001_{i:03d}E" for i in range(1, 11)]
            if topic == "Sex by Age (full)":
                return [f"B01001_{i:03d}E" for i in range(1, 50)]
            if topic == "Hispanic/Latino Origin (full)":
                return [f"B03002_{i:03d}E" for i in range(1, 10)]

        return codes

    cache = {"k": None, "df": None}

    def _fetch():
        k = (input.topic(), int(input.year()), input.geo_kind(), input.zip(), tuple(_codes()))
        if cache["k"] == k and cache["df"] is not None:
            return cache["df"]
        df = call_api_vars_ttl(int(input.year()), _codes(), _geo())
        out = tidy_long(df)
        cache["k"], cache["df"] = k, out
        return out

    @render_plotly
    def plot_overview():
        df = _fetch()
        area = input.geo_kind()
        area_disp = area if area != "ZIP" else f"ZIP {input.zip()}"
        title = f"{area_disp} — {input.topic()} ({input.year()})"
        return bar_top(df, title)

    @render.data_frame
    def tbl_overview():
        return _fetch()
