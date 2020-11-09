from typing import List

from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

from dataforest.treehouse.components.core import Block, ModelBlock, Container, TabContent


class Scatter(ModelBlock):
    STYLE = dict(width="75%", height="50%", display="inline-block")

    def layout(self):
        return dcc.Graph(id=self.register("scatter"), style=self.STYLE)

    def callbacks(self, dropdowns: "DropDownColumn", sliders: "SliderColumn"):
        # TODO: PR to fix signature
        @self.callback(
            self.output("umap", "figure"),
            [drp.input("dropdown", "value") for drp in dropdowns]
            + [sliders.input(f"{k}--slider", "value") for k in self.model.MARKER_ARGS],
        )
        def make_figure(x, y, color, facet_col, facet_row, opacity, size):
            fig = px.scatter(
                self.model.df,
                x=x,
                y=y,
                color=color,
                facet_col=facet_col,
                facet_row=facet_row,
                height=700,
                custom_data=[self.model.df.index],
            )
            fig.update_traces(marker=dict(opacity=opacity, size=size))
            fig.update_layout(clickmode="event+select")
            return fig

    @staticmethod
    def get_selected_indices(selected_data):
        points = selected_data["points"]
        indices = [p["customdata"][0] for p in points]
        return indices


class DropDown(Block):
    def layout(self):
        return html.P(
            [self.data["title"] + ":", dcc.Dropdown(id=self.register("dropdown"), options=self.data["options"])]
        )


class Slider(Block):
    def layout(self):
        html.P([self.data["title"] + ":", dcc.Slider(id=self.register("slider"), **self.data["params"])])


class DropDownColumn(Block):
    """
    data:
        title_list: [str, str, ...]
        options: [{label: str, value: str}, ...]
    """

    @property
    def contents(self):
        return [
            DropDown(self.app, data={"title": title, "options": self.data["options"]})
            for title in self.data["title_list"]
        ]

    def layout(self):
        # TODO: layout is run before callbacks, right?
        return self.contents


class SliderColumn(ModelBlock):
    """
    data:
        slider_params: {str: {min: 0, max: 1, ...}}
    """

    @property
    def contents(self):
        return [Slider(data={"title": title, "params": params}) for title, params in self.data["slider_params"]]

    def layout(self):
        return self.contents


class DropDownOptionsStore(Block):
    def callbacks(self, master, subscribers: List[DropDown]):
        # TODO: don't know whether this will work -- may need to do by ID rather than object
        @self.callback(
            [sub.output("dropdown", "options") for sub in subscribers], [master.input("dropdown", "options")]
        )
        def propagate_new_col(dropdown_options):
            return tuple(len(subscribers) * [dropdown_options])

    def layout(self):
        raise NotImplementedError()


class ColumnAdder(Block):
    def layout(self):
        return [
            html.P(
                [
                    dcc.Markdown("**New Metadata Column**"),
                    "colname: ",
                    dcc.Input(id=self.register("colname"), value=""),
                    html.Button("Add Column", id=self.register("submit")),
                    html.Br(),
                ]
            )
        ]


class ScatterAnnotator(ModelBlock):
    @property
    def dropdown(self):
        # TODO: is this going to cause callback output persistence problems?
        return dcc.Dropdown(
            id=self.register("dropdown"), options=self.data.options, value=self.data.options[0]["value"]
        )

    def layout(self):
        return [
            html.P(
                [
                    dcc.Markdown("**Annotator**"),
                    "column:",
                    self.dropdown,
                    "label:",
                    dcc.Input(id=self.register("label"), value=""),
                    html.Button("add annotation", id=self.register("submit")),
                ]
            )
        ]

    def callbacks(self, scatter: "Scatter", col_adder: "ColumnAdder"):
        @self.callback(
            self.output("label", "value"),
            self.input("submit", "n_clicks"),
            [scatter.state("scatter", "selectedData"), self.state("dropdown", "value"), self.state("label", "value")],
        )
        def annotate(n_clicks, selected_data, col, label):
            if not n_clicks:
                return ""
            indices = scatter.get_selected_indices(selected_data)
            scatter.selected_indices = indices
            self.model.df.loc[indices, col] = label
            return ""

        @self.callback(
            [self.output("dropdown", "options"), col_adder.output("colname", "value")],
            [col_adder.input("submit", "n_clicks")],
            [col_adder.state("colname", "value"), self.state("dropdown", "options")],
        )
        def add_new_col(n_clicks, new_col, dropdown_options):
            if not n_clicks:
                return dropdown_options, ""
            if new_col not in {opt["value"] for opt in dropdown_options}:
                self.model.df[new_col] = ""
                dropdown_options.append({"label": new_col, "value": new_col})
            return dropdown_options, ""


# class ScatterPanel(Container):
#     STYLE = dict(width="25%", float="left")
#     COMPONENTS = [ScatterControlDropdowns, SliderColumn, ColumnAdder, ScatterAnnotator]
#     DIV_WRAP = True
#
#
# class ScatterTab(TabContent):
#     TITLE = html.H1("dataforest treehouse")
#     COMPONENTS = [ScatterPanel, Scatter, GeneData]
