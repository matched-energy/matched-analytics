import pandas as pd
import plotly.graph_objects as go

from ma.utils.misc import truncate_string


def get_fig(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    for bm_unit_id in df["bm_unit_id"].unique():
        bm_unit_data = df[df["bm_unit_id"] == bm_unit_id]

        fig.add_trace(
            go.Scatter(
                x=bm_unit_data["settlement_datetime"],
                y=bm_unit_data["bm_unit_metered_volume"],
                mode="lines",
                name=truncate_string(bm_unit_id),
            )
        )

    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False),
        title="bm_unit_metered_volume vs settlement_datetime",
        showlegend=True,
    )

    return fig
