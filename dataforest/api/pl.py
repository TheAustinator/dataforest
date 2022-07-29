from dataforest.utils.analysis.pairwise import heat_sns

import dash

from jupyter_dash import JupyterDash
import pandas as pd
import plotly.express as px


def dash_flex(df: pd.DataFrame):
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = JupyterDash(__name__, external_stylesheets=external_stylesheets)
    df = pd.read_csv('https://plotly.github.io/datasets/country_indicators.csv')
    print(df.head())
    available_indicators = df['Indicator Name'].unique()
    app.layout = dash.html.Div([
        dash.html.Div([

            dash.html.Div([
                dash.dcc.Dropdown(
                    id='crossfilter-xaxis-column',
                    options=[{'label': i, 'value': i} for i in available_indicators],
                    value='Fertility rate, total (births per woman)'
                ),
                dash.dcc.RadioItems(
                    id='crossfilter-xaxis-type',
                    options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
                    value='Linear',
                    labelStyle={'display': 'inline-block', 'marginTop': '5px'}
                )
            ],
                style={'width': '49%', 'display': 'inline-block'}),

            dash.html.Div([
                dash.dcc.Dropdown(
                    id='crossfilter-yaxis-column',
                    options=[{'label': i, 'value': i} for i in available_indicators],
                    value='Life expectancy at birth, total (years)'
                ),
                dash.dcc.RadioItems(
                    id='crossfilter-yaxis-type',
                    options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
                    value='Linear',
                    labelStyle={'display': 'inline-block', 'marginTop': '5px'}
                )
            ], style={'width': '49%', 'float': 'right', 'display': 'inline-block'})
        ], style={
            'padding': '10px 5px'
        }),

        dash.html.Div([
            dash.dcc.Graph(
                id='crossfilter-indicator-scatter',
                hoverData={'points': [{'customdata': 'Japan'}]}
            )
        ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
        dash.html.Div([
            dash.dcc.Graph(id='x-time-series'),
            dash.dcc.Graph(id='y-time-series'),
        ], style={'display': 'inline-block', 'width': '49%'}),

        dash.html.Div(dash.dcc.Slider(
            id='crossfilter-year--slider',
            min=df['Year'].min(),
            max=df['Year'].max(),
            value=df['Year'].max(),
            marks={str(year): str(year) for year in df['Year'].unique()},
            step=None
        ), style={'width': '49%', 'padding': '0px 20px 20px 20px'})
    ])


    @app.callback(
        dash.dependencies.Output('crossfilter-indicator-scatter', 'figure'),
        [dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
         dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
         dash.dependencies.Input('crossfilter-xaxis-type', 'value'),
         dash.dependencies.Input('crossfilter-yaxis-type', 'value'),
         dash.dependencies.Input('crossfilter-year--slider', 'value')])
    def update_graph(xaxis_column_name, yaxis_column_name,
                     xaxis_type, yaxis_type,
                     year_value):
        dff = df[df['Year'] == year_value]

        fig = px.scatter(x=dff[dff['Indicator Name'] == xaxis_column_name]['Value'],
                         y=dff[dff['Indicator Name'] == yaxis_column_name]['Value'],
                         hover_name=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name']
                         )
        fig.update_traces(customdata=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name'])
        fig.update_xaxes(title=xaxis_column_name, type='linear' if xaxis_type == 'Linear' else 'log')
        fig.update_yaxes(title=yaxis_column_name, type='linear' if yaxis_type == 'Linear' else 'log')
        fig.update_layout(margin={'l': 40, 'b': 40, 't': 10, 'r': 0}, hovermode='closest')
        return fig

    def create_time_series(dff, axis_type, title):
        fig = px.scatter(dff, x='Year', y='Value')
        fig.update_traces(mode='lines+markers')
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(type='linear' if axis_type == 'Linear' else 'log')
        fig.add_annotation(x=0, y=0.85, xanchor='left', yanchor='bottom',
                           xref='paper', yref='paper', showarrow=False, align='left',
                           text=title)
        fig.update_layout(height=225, margin={'l': 20, 'b': 30, 'r': 10, 't': 10})
        return fig


    @app.callback(
        dash.dependencies.Output('x-time-series', 'figure'),
        [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
         dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
         dash.dependencies.Input('crossfilter-xaxis-type', 'value')])
    def update_y_timeseries(hoverData, xaxis_column_name, axis_type):
        country_name = hoverData['points'][0]['customdata']
        dff = df[df['Country Name'] == country_name]
        dff = dff[dff['Indicator Name'] == xaxis_column_name]
        title = '<b>{}</b><br>{}'.format(country_name, xaxis_column_name)
        return create_time_series(dff, axis_type, title)


    @app.callback(
        dash.dependencies.Output('y-time-series', 'figure'),
        [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
         dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
         dash.dependencies.Input('crossfilter-yaxis-type', 'value')])
    def update_x_timeseries(hoverData, yaxis_column_name, axis_type):
        dff = df[df['Country Name'] == hoverData['points'][0]['customdata']]
        dff = dff[dff['Indicator Name'] == yaxis_column_name]
        return create_time_series(dff, axis_type, yaxis_column_name)

    app.run_server(host="0.0.0.0", mode="jupyterlab", debug=True)
    return app


def value_counts_pivot(df, cols, heatmap=False, cluster=False, fillna=0, norm=None, **kwargs):
    piv = df.value_counts(cols).reset_index().pivot(*cols)
    piv.columns = piv.columns.droplevel()
    if norm == 0:
        piv /= piv.sum(axis=0)
    elif norm == 1:
        piv = (piv.T / piv.sum(axis=1)).T
    if heatmap:
        import seaborn as sns
        from matplotlib import pyplot as plt
        plot = sns.clustermap if cluster else sns.heatmap
        piv = piv.fillna(fillna) if cluster else piv
        plot(piv, **kwargs)
        plt.show()
    return piv
