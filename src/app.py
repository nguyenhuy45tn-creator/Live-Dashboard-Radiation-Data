import plotly.express as px
from dash import Dash, dcc, html, callback, Output, Input, State
import pandas as pd
from sqlalchemy import create_engine




app = Dash()
engine = create_engine("postgresql+psycopg2://postgres:example@localhost:5432/postgres", echo=True)

app.layout = html.Div([
    html.Div(children="World map"),
    dcc.Graph(id="live_world_map", figure=None),
    dcc.Interval(id="timekeeper", interval=3*1000) # in miliseconds
])



@callback(
    [Output(component_id='live_world_map', component_property='figure'),
     Output(component_id='live_world_map', component_property='relayoutData')],
    Input(component_id='timekeeper', component_property='n_intervals'),
    State(component_id='live_world_map', component_property="relayoutData"),
    prevent_initial_call=True)
def update_table(interval, relayout):
    with engine.connect() as conn:
        query = """
        SELECT *
        FROM geo_data;"""
        df = pd.read_sql_query(query, con = conn)
        print("query dataframe-----", len(df))
    fig = px.scatter_geo(df,
                    lat="latitude",
                    lon="longtitude",
                    hover_name="measurement",
                    projection="natural earth")
    return fig, fig

if __name__=='__main__':
    ###################
    app.run(debug=True)  
    ###################