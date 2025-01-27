import pandas as pd
import plotly.graph_objects as go

from m_elexon.utils import truncate_string


def get_fig(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    for bm_unit_id in df["BM Unit Id"].unique():
        bm_unit_data = df[df["BM Unit Id"] == bm_unit_id]

        fig.add_trace(
            go.Scatter(
                x=bm_unit_data["Settlement Datetime"],
                y=bm_unit_data["BM Unit Metered Volume"],
                mode="lines",
                name=truncate_string(bm_unit_id),
            )
        )

    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
        title="BM Unit Metered Volume vs Settlement Datetime",
        showlegend=True,
    )

    return fig
