# app.py — wires the UI and tabs together (kept same name & layout)

import threading
from shiny import App, ui, reactive
from fetch_census_data import YEARS, TOPICS, LB_ZCTAS
from user_views import overview, trends  # keep your module names

# Warm-up: fire-and-forget calls to prefill cache (non-blocking)
from fetch_census_data import call_api_vars_ttl, CITY_GEO, US_GEO
def _warm_async():
    combos = [
        (2023, ["DP05_0001E","DP05_0018E"], CITY_GEO),
        (2023, ["B01003_001E"], US_GEO),
        (2022, ["DP05_0001E"], CITY_GEO),
    ]
    for y, codes, geo in combos:
        t = threading.Thread(target=call_api_vars_ttl, args=(y, codes, geo), daemon=True)
        t.start()

# kick off without blocking startup
_warm_async()

app_ui = ui.page_fluid(
    ui.panel_title("Long Beach Demographics — LIVE (ACS 5-year)"),
    ui.layout_sidebar(
        ui.sidebar(
            # Topic list comes from fetch_census_data.TOPICS keys
            ui.input_select("topic", "Topic", list(TOPICS.keys()), selected="Total Population"),

            # Choose a single ACS 5-year release (2018–2023)
            ui.input_slider("year", "Year", min(YEARS), max(YEARS), value=max(YEARS), step=1),

            # Geography selector (City / US / ZIP)
            ui.input_radio_buttons("geo_kind", "Geography", ["City", "US", "ZIP"], selected="City"),

            # ZIP (ZCTA) selector — only show when Geography == ZIP
            ui.panel_conditional(
                "input.geo_kind === 'ZIP'",
                ui.input_select("zip", "ZIP (ZCTA)", LB_ZCTAS, selected=LB_ZCTAS[0]),
            ),

            # Manual refresh trigger (used by trends.py; available for future use in overview.py)
            ui.input_action_button("refresh", "Refresh from Census"),
            width=320,
        ),

        # Two tabs: Overview (bar) and Trends (line)
        ui.navset_tab(
            ui.nav_panel("Overview", *overview.panel().children),
            ui.nav_panel("Trends",   *trends.panel().children),
        ),
    ),
)

def server(input, output, session):
    # Show a small toast whenever Refresh is clicked (non-blocking)
    @reactive.Effect
    def _notify_refresh():
        input.refresh()  # track the button
        ui.notification_show(
            "Fetching latest data from Census…",
            type="message",
            duration=2
        )

    # Bind each tab's server code
    overview.server_bind(output, input)
    trends.server_bind(output, input)

app = App(app_ui, server)
