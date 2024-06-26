import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

## Navigation bar

navbar_style = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "6rem",  
    "padding": "1rem 0.5rem",
    "background-color": "#F14C5C", 
}

navbar = html.Div(
    [
        html.Div(
            html.H1("Bt", className="logo"),
            className="mb-4 text-center"
        ),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink(
                    [
                        html.Img(src="/assets/home.png", style={"width": "24px", "height": "24px"}, className="d-block mx-auto mb-1"),
                        html.Span("Home", className="d-block text-center"),
                    ],
                    href="/",
                    active="exact",
                ),
                dbc.NavLink(
                    [
                        html.Img(src="/assets/news.png", style={"width": "24px", "height": "24px"}, className="d-block mx-auto mb-1"),
                        html.Span("News", className="d-block text-center"),
                    ],
                    href="/news",
                    active="exact",
                ),
            ],
            vertical=True,
            pills=True,
            className="flex-column",
        ),
    ],
    style = navbar_style
)

app.layout = html.Div([
    dcc.Location(id="url"),
    navbar,
    dash.page_container
])

if __name__ == '__main__':
    app.run_server(debug=True, port=8552)