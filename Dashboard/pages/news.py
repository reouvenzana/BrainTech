import dash
from dash import Dash, dcc, html, Input, Output, callback
import polars as pl

dash.register_page(__name__, external_stylesheets='../assets/styles.css')

@callback(Output('tabs-content-inline', 'children'),
              Input('tabs-styled-with-inline', 'value'))
def render_content(tab):
    if tab == 'tab-1':
        return html.Div([
            html.H3('Tab content 1')
        ])
    elif tab == 'tab-2':
        return hot_topics_tab
    elif tab == 'tab-3':
        return html.Div([
            html.H3('Tab content 3')
        ])
    
df = (pl
        .read_parquet("./Data/News/news_with_topics.parquet")
        .to_pandas()
    )

current_news = (
    pl.DataFrame(df, schema_overrides={"date_published":pl.Date})
    .filter(pl.col("companies")=="company")
    .with_columns(
        text=pl.col("text").str.replace_all(r"\n",r"\n\n")
    )
)[0]

# Table news
df_news = (
    pl.DataFrame(df, schema_overrides={"date_published":pl.Date})
    .filter(
        pl.col("companies") == "company"
    )
    .select(
        ["title","date_published", "topics_representation", "companies", "topics", "topic_probability_distribution"]
    )
)

df_topics = (
    pl.DataFrame(df)
    .filter(
        pl.col("companies") == "company"
    )
    .select(["topics","topics_custom_name","topics_count"])
    .explode(["topics","topics_custom_name", "topics_count"])
    .unique(["topics","topics_custom_name", "topics_count"])
    .drop_nulls()
    .sort("topics_count", descending=True)
    .with_columns(
        topics = pl.col("topics").cast(pl.Int8).cast(pl.Utf8))
)
dict_topics = [{"label":row[1], "value":row[0]} for row in df_topics.rows()]

## Series of unique companies
s_unique_companies = (
    pl.DataFrame(df)
    .select("companies")
    .unique()
)
dict_unique_companies = [{"label":row[0], "value":row[0]} for row in s_unique_companies.rows()]

text = current_news[0,'text'].replace(r"\n","\n")

hot_topics_tab = html.Div([
    html.Div(className="selectors-panel"),
    html.P("Company", className="selector-title"),
    dcc.Dropdown(options=dict_unique_companies, 
                multi=False,
                placeholder="Choose a customer",
                style = {
                    "width":"14rem"
                }
    )],
    style={
    "margin-top": "-8rem",
    "margin-left" : "7rem"
    }
)

layout = [
    html.Div('News', className="header"),
    html.Div([
    dcc.Tabs(id="tabs-styled-with-inline", value='tab-1', children=[
        dcc.Tab(label='General View', value='tab-1', className="custom-tab", selected_className="custom-tab--selected"),
        dcc.Tab(label='Hot Topics', value='tab-2', className="custom-tab", selected_className="custom-tab--selected"),
        dcc.Tab(label='Sentiment Analysis', value='tab-3', className="custom-tab", selected_className="custom-tab--selected")
    ],
    className="custom-tabs-container"),
    html.Div(id='tabs-content-inline')
    ])
]


