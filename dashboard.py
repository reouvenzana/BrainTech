#%%
import vizro.plotly.express as px
from vizro.tables import dash_ag_grid
from dash_ag_grid import AgGrid
from vizro import Vizro
import vizro.models as vm
import polars as pl
from vizro.models.types import capture
import vizro.plotly.express as px
from dash import dcc

#%%
df = (pl
        .read_parquet("./Data/News/news_with_topics.parquet")
        .to_pandas()
    )
#%%

company = "Walmart"
current_news = (
    pl.DataFrame(df, schema_overrides={"date_published":pl.Date})
    .filter(pl.col("companies")==company)
    .with_columns(
        text=pl.col("text").str.replace_all(r"\n",r"\n\n")
    )
)[0]

# Table news

df_news = (
    pl.DataFrame(df, schema_overrides={"date_published":pl.Date})
    .filter(
        pl.col("companies") == company
    )
    .select(
        ["title","date_published", "topics_representation", "companies", "topics", "topic_probability_distribution"]
    )
)

# Creating the topics list

# df_topics = (
#     pl.DataFrame(df)
#     .filter(
#         pl.col("companies") == company
#     )
#     .select(["topics","topics_representation"])
#     .explode(["topics","topics_representation"])
#     .unique(["topics","topics_representation"])
#     .drop_nulls()
#     .with_columns(
#         topics_representation = pl.col("topics_representation").list.join(" "),
#         topics = pl.col("topics").cast(pl.Int8).cast(pl.Utf8))
# )
# dict_topics = [{"label":row[1], "value":row[0]} for row in df_topics.rows()]


df_topics = (
    pl.DataFrame(df)
    .filter(
        pl.col("companies") == company
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

#%%
s_unique_companies = (
    pl.DataFrame(df)
    .select("companies")
    .unique()
)
dict_unique_companies = [{"label":row[0], "value":row[0]} for row in s_unique_companies.rows()]


text = current_news[0,'text'].replace(r"\n","\n")

#%%
@capture("ag_grid")
def my_custom_aggrid(data_frame, company, topics):
    """Custom ag_grid."""

    columnDefs = [
        {"field": "title"},
        {'field': 'topic_probability_distribution',
         'initialSort': 'desc',
         'headerName':"Topic Probability"}
    ]

    data_frame = (
            pl.DataFrame(data_frame)
            .filter(
                pl.col("companies") == company,
                pl.col("topics").list.contains(int(topics))
            )
            .select(
                ["title","date_published", "topics_representation", "companies", "topics", "topic_probability_distribution"]
            )
            .with_columns(
                topic_probability_distribution = (
                    pl.col("topic_probability_distribution")
                    .list.get(
                        pl.col("topics")
                        .list.eval(pl.element()==int(topics))
                        .list.arg_max()
                    )
                )
                .cast(pl.Float64)
                .round(2)
            )
            .to_pandas()
    )

    defaults = {
        "className": "ag-theme-quartz-dark ag-theme-vizro",
        "defaultColDef": {
            "resizable": True,
            "sortable": True,
            "filter": True,
            "filterParams": {
                "buttons": ["apply", "reset"],
                "closeOnApply": True,
            },
            "flex": 1,
            "minWidth": 70,
        },
        "dashGridOptions": {
            "pagination": True,
            "paginationAutoPageSize": True,
            "paginationPageSizeSelector": False
        },
        "style": {"height": "100%"},
    }
    
    return AgGrid(
        columnDefs=columnDefs,
        rowData=data_frame.to_dict("records"),
        **defaults
    )

@capture("action")
def my_custom_action(t: int):
    """Custom action."""
    sleep(t)


#%%
page = vm.Page(
    layout=vm.Layout(grid=[
        [0, 0, 1, 1],
        [0, 0, 1, 1]
        ]
    ),
    title="News",
    components=[
        vm.Card(
            text = f"# {current_news[0,'title']} \n\n  **{current_news[0,'date_published']}**  \n\n  {text} \n\n {current_news[0,'link']}"
        ),
        vm.AgGrid(id = "custom_ag_grid", title="Articles", figure=my_custom_aggrid(data_frame=df, company="Walmart", topics="2")),
    ],
     controls=[
        vm.Parameter(
            targets=["custom_ag_grid.company"],
            selector=vm.Dropdown(title="Company", options=dict_unique_companies),
        ),
        vm.Parameter(
            targets=["custom_ag_grid.topics"],
            selector=vm.Dropdown(title="Hot topics", options=dict_topics, multi=False)
        )
    ],
)

dashboard = vm.Dashboard(pages=[page])

Vizro().build(dashboard).run()
# %%
