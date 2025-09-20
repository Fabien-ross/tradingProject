import pandas as pd

import dash
from typing import List, Dict, Optional, Any
from dash import dcc, html, Input, Output, State
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import numpy as np

from src.core.utils.dates.date_format import interval_map
from src.core.logging.loggers import logger_spo
from src.core.exceptions.exceptions import *

from src.models.items_models.assets_models import BaseAsset


class DisplayExecutor:

    def __init__(self) -> None:

        self.graph_height = 600
        self.graph_width = 1350
        self.graph_margins = 25

        self.tfs = list(interval_map.keys())
        self.displaying_dict: Dict[str, Dict[str, pd.DataFrame]] = {}
        self.asset_ids: List[str] = []
        self.indic_dict: Dict[str, Dict[str,Any]] = {
            "prices" : {
                "type":1,
                "yaxis":"y1",
                "curves": ["close"]
                },
            "bollinger": {
                "type": 1,
                "yaxis": "y1",
                "curves": [
                    "boll_low2",
                    "boll_low1",
                    "boll_mid",
                    "boll_up1",
                    "boll_up2"
                ]
            },
            "msd": {
                "type": 2,
                "yaxis": "y3",
                "curves": ["msd"]
            },
            "return": {
                "type": 2,
                "yaxis": "y3",
                "curves": [
                    "simple_return",
                    #"log_return"
                ]
            },
            "obv": {
                "type": 2,
                "yaxis": "y2",
                "curves": ["obv"]
            },
            "vwap": {
                "type": 1,
                "yaxis": "y1",
                "curves": ["vwap"]
            },
            "volatility": {
                "type": 2,
                "yaxis": "y3",
                "curves": ["volatility"]
            },
            "rsi": {
                "type": 2,
                "yaxis": "y3",
                "curves": [
                    "rsi",
                    #"stoch_rsi"
                ]
            },
            "ema": {
                "type": 1,
                "yaxis": "y1",
                "curves": [
                    "ema_short",
                    "ema_mid",
                    "ema_long"
                ]
            },
            "macd": {
                "type": 2,
                "yaxis": "y3",
                "curves": ["macd"]
            },
            "score": {
                "type": 2,
                "yaxis": "y3",
                "curves": ["score"]
            }    
        }
        self.indic: List[str] = list(self.indic_dict.keys())
        self.cached_graph = {}


    def get_labelled(self) -> Dict[str,List]:
        dic_labl = {}
        dic_labl["tfs"] = [{'label': k, 'value': k} for k in self.tfs]
        dic_labl["asset_ids"] = [{'label': k, 'value': k} for k in self.asset_ids]
        dic_labl["indic"] = [{'label': k, 'value': k} for k in self.indic]
        return dic_labl
        

    def make_app(self) -> dash.Dash:

        def create_fig(
            asset_id: str, 
            time_frame: str,
            df: pd.DataFrame,
            visible_indic: List[str]
        ) -> go.Figure:
            
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.6, 0.4],
                specs=[[{}], [{"secondary_y": True}]]
            )
            fig.add_trace(
                go.Candlestick(
                    x=df["open_time"],
                    open=df["open"],
                    high=df["high"],
                    low=df["low"],
                    close=df["close"],
                    name=f"candle-{asset_id}-{time_frame}",
                    visible=('prices' not in visible_indic),
                    showlegend=False
                ),
                row=1, col=1
            )

            fig.add_trace(
                go.Bar(
                    x=df['open_time'],
                    y=df['volume'],
                    name="Volume",
                    marker_color='grey'
                ),
                row=2, col=1
            )

            for indic_grp, indic_list in self.indic_dict.items():
                for indic in indic_list.get("curves",[]):
                    fig.add_trace(
                        go.Scatter(
                            x=df["open_time"],
                            y=df[indic],
                            mode="lines",
                            name=indic,                 
                            legendgroup=indic_grp,     
                            visible=(indic_grp in visible_indic or indic in visible_indic),
                            showlegend=False,
                            yaxis=indic_list.get("yaxis")
                        ),
                        row=indic_list.get("type",1), col=1,
                        secondary_y=(indic_list.get("yaxis")=='y3')
                    )

            fig.update_layout(
                uirevision='constant',
                width=self.graph_width,
                height=self.graph_height,
                xaxis=dict(rangeslider=dict(visible=False)),
                margin=dict(
                    l=self.graph_margins, 
                    r=self.graph_margins, 
                    t=self.graph_margins, 
                    b=self.graph_margins
                ),
                yaxis=dict(title="Prix"),        
                yaxis2=dict(title="Volume"),
                yaxis3=dict(title="Normalized", range=[-1.1,1.1]),
                showlegend=False
            )
            return fig

        def update_cached_fig(
            fig: go.Figure,
            visible_indic: List[str],
            asset_id: str,
            tf: str
        ):
            for trace in fig.data:
                if isinstance(trace, go.Scatter):
                    if trace.legendgroup in visible_indic:
                        trace.visible = True
                    else:
                        trace.visible = False
                    
            candle_name = f"candle-{asset_id}-{tf}"
            if "prices" in visible_indic:
                fig.update_traces(visible=False, selector=dict(name=candle_name))
            else:
                fig.update_traces(visible=True, selector=dict(name=candle_name))

        app = dash.Dash(__name__)
        dic_labl = self.get_labelled()
        app.layout = html.Div([
            html.Div([
                html.H2(id='dynamic-title', style={'margin': 0}),
                html.Div([
                    dcc.Dropdown(
                        id='group-selector',
                        options=dic_labl["asset_ids"],
                        value=self.asset_ids[0],
                        clearable=False,
                        style={'width': '200px', 'margin-right': '10px'}
                    ),
                    dcc.Dropdown(
                        id='range-selector',
                        options=dic_labl["tfs"],
                        value=self.tfs[0],
                        clearable=False,
                        style={'width': '100px'}
                    )
                ], style={'display': 'flex'})
            ], style={
                'display': 'flex',
                'justify-content': 'space-between',
                'align-items': 'center',
                'margin-bottom': '10px'
            }),

            html.Div([
                dcc.Graph(
                    id='graph',
                    style={'width': f'{self.graph_width}px', 'height': f'{self.graph_height}px', 'flex': 'none'},
                    config={'responsive': False}
                ),
                html.Div([
                    dcc.Checklist(
                        id='curve-toggle',
                        options=dic_labl["indic"],
                        value=[],  # Empty at start
                        labelStyle={'display': 'block', 'margin-bottom': '5px'}
                    )
                ], style={
                    'flex': 'none',
                    'width': '100px',
                    'padding-left': '0px',
                    'padding-top': '45px',
                    'height': f'{self.graph_height}px',
                    'overflowY': 'auto'
                })
            ], style={'display': 'flex', 'align-items': 'start'}),

            # Global storage of indicator checked
            dcc.Store(id='stored-checked-indic', data=[])
        ], style={'padding': '20px', 'font-family': 'Arial'})

        # Update global checkbox state when one is checked
        @app.callback(
            Output('stored-checked-indic', 'data'),
            Input('curve-toggle', 'value'),
            State('stored-checked-indic', 'data')
        )
        def update_storage(checked_values, stored):
            return checked_values

        # Graph update
        @app.callback(
            Output('graph', 'figure'),
            Output('dynamic-title', 'children'),
            Input('group-selector', 'value'),
            Input('range-selector', 'value'),
            Input('stored-checked-indic', 'data')
        )
        def update_graph(asset_id, time_frame, visible_indic):
            fig = self.cached_graph.get(asset_id,{}).get(time_frame)
            if fig:
                update_cached_fig(
                    fig=fig, 
                    visible_indic=visible_indic,
                    asset_id=asset_id,
                    tf=time_frame)
            else:
                df = self.displaying_dict.get(asset_id,{}).get(time_frame)
                if df is None or df.empty:
                    return fig, f"Error loading {asset_id} for {time_frame}"
                
                fig = create_fig(
                    asset_id=asset_id, 
                    time_frame=time_frame, 
                    df=df, 
                    visible_indic=visible_indic
                )
                if asset_id not in self.cached_graph.keys():
                    self.cached_graph[asset_id] = {}
                self.cached_graph[asset_id][time_frame] = fig
            title = f"{asset_id.upper()} {time_frame}"
            return fig, title
    
        return app
    

    def plot_klines_and_indicators(
            self,
            df_data : pd.DataFrame
        ):

            groups: Dict[str,pd.DataFrame] = {str(asset_id): subdf for asset_id, subdf in df_data.groupby("asset_id")}
            for asset_id, df_asset in groups.items():
                self.displaying_dict[asset_id] = {}
                tf_groups: Dict[str,pd.DataFrame] = {str(tf): subdf for tf, subdf in df_asset.groupby("time_frame")}
                for tf, df_tf in tf_groups.items():
                    self.displaying_dict[asset_id][tf] = df_tf.sort_values(by='open_time')

            self.asset_ids = list(self.displaying_dict.keys())

            app = self.make_app()
            app.run_server(debug=False, port=8050)


            
        













