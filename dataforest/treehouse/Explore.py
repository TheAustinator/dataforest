from typing import TYPE_CHECKING

from dash.dependencies import Input, Output, State
from dash_building_blocks import Block, Store
import dash_core_components as dcc
import dash_html_components as html

from dataforest.treehouse.const import DIMENSIONS

if TYPE_CHECKING:
    from dataforest.core.DataTree import DataTree


class TreeHouse(Block):
    TABS = {}

    def layout(self):
        tabs = [dcc.Tab(label=label, children=children) for label, children in self.TABS.items()]
        return html.Div(tabs)


class Explore:
    def callbacks(self, app):
        def _get_expr_df(rna, cell_ids, density=True):
            rna = rna[cell_ids]
            rna_avg = rna.sum(axis=0)
            rna_avg = np.squeeze(np.asarray(rna_avg))
            if density:
                rna_avg /= rna_avg.sum() / 100
            genes = branch.rna.genes
            df = pd.DataFrame({"gene": genes, "expr": rna_avg.flatten()})
            df.sort_values("expr", ascending=False, inplace=True)
            df.index = df["gene"]
            return df

        def _get_crude_markers(cells_1, cells_2):
            expr_1 = _get_expr_df(branch.rna, cells_1)
            expr_2 = _get_expr_df(branch.rna, cells_2)
            df = expr_1.copy()
            rna_root = branch.copy().goto_process("root").rna
            df["selected"] = _get_expr_df(rna_root, cells_1, density=False)["expr"]
            df["rest"] = _get_expr_df(rna_root, cells_2, density=False)["expr"]
            df["expr"] = (expr_1["expr"] - expr_2["expr"]) / expr_2["expr"]
            df["expr_abs"] = df["expr"].abs()
            df.sort_values("expr_abs", ascending=False, inplace=True)
            df.drop("expr_abs", axis=1, inplace=True)
            return df

        app.run_server(mode="jupyterlab", host="0.0.0.0", debug=True)
