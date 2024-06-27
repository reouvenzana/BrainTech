import dash
from dash import Dash, dcc, html, Input, Output, callback
import polars as pl
#add BrainTech directory to PYTHONPATH env variable if import error
from dashboard.utils import news_preprocessing as news 

dash.register_page(__name__, external_stylesheets='../assets/styles.css', suppress_callback_exceptions=True)

df = news.load_news_data()
filtered_df = news.filter_news_by_company(df, "Walmart")
dict_companies = news.get_company_dict(df)
dict_topics = news.get_topic_dict(filtered_df)
news_title, news_date_published, news_text, news_link = news.get_news_elements(filtered_df, 0)
header = f"# {news_title}\n"
abstract = "> Abstract summary"
date_published = f"*{news_date_published}*\n\n"
text = f"{news_text}\n\n"
link = f"{news_link}\n"
final_string = header + abstract + date_published + text + link

hot_topics_panel = html.Div([
    html.Div(className="selectors-panel"),
    html.P("Company", className="selector-title"),
    dcc.Dropdown(id="company-dropdown",
                options=dict_companies, 
                multi=False,
                placeholder="Select a company",
                style = {
                    "width":"14rem"
                }
    ),
    html.P("Topics", className="selector-title"),
    dcc.Dropdown(id="topics-dropdown",
                options=dict_topics, 
                multi=False,
                placeholder="Select a topic",
                style = {
                    "width":"14rem"
                },
                optionHeight=150,
                maxHeight=500
    )],
    style={
    "margin-top": "-8rem",
    "margin-left" : "7rem"
    }
)

hot_topics_news_card = html.Div(
    dcc.Markdown(final_string),
    className="markdown-card",
    style = {
        "margin-top": "0rem",
        "margin-left" : "25rem",
        "width" : "50rem",
        "height" : "35rem",
    }
)


hot_topics_tab = [hot_topics_panel, hot_topics_news_card]


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

@callback(
    Output('topics-dropdown', 'options'),
    Input('company-dropdown', 'value')
)
def update_topics(company):
    if company is None:
        return []
    return news.get_topic_dict(news.filter_news_by_company(df, company))
