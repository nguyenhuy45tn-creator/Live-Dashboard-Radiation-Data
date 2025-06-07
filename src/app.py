import plotly.express as px
from dash import Dash, dcc, html
import pandas as pd


df = pd.read_csv("./output/temp_result/result.csv") #read from database
df = df.drop_duplicates(["longtitude","latitude"])
print(len(df))
fig = px.scatter_geo(df,
                     lat="latitude",
                     lon="longtitude",
                     hover_name="measurement",
                     size="measurement",
                     projection="natural earth")

app = Dash()
app.layout = html.Div([
    dcc.Graph(figure=fig)
])

app.run(debug=True, use_reloader=False)  # Turn off reloader if inside Jupyter