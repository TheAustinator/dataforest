from typing import TYPE_CHECKING

import dash_core_components as dcc
import dash_html_components as html
from dash_building_blocks import Block
import dash_table as dt

if TYPE_CHECKING:
    from dataforest.treehouse.model import DataFrameModel


class Block(Block):
    def callbacks(self, *args):
        pass

    def output(self, *args, component_property="children"):
        component = self
        for subcomp_id in args:
            component = component[subcomp_id]
        return component.output(component_property)

    def layout(self):
        raise NotImplementedError()


class Container(Block):
    COMPONENTS = list()
    STYLE = dict()
    DIV_WRAP = False

    @property
    def components(self):
        """Modify in sublclasses for instance specific behavior"""
        return self.COMPONENTS

    @property
    def style(self):
        return self.STYLE

    def layout(self):
        components = []
        for cls in self.components:
            comp = cls().layout()
            if isinstance(comp, list):
                components += comp
            else:
                components.append(comp)
        if self.DIV_WRAP:
            components = html.Div(components, style=self.style)
        return components


class TabContent(Container):
    TITLE = ""
    COMPONENTS = []

    @property
    def components(self):
        title = html.H1(self.TITLE)
        return [title, *self.COMPONENTS]


class ModelBlock(Block):
    @property
    def model(self) -> "DataFrameModel":
        model = self.data.get("model", None)
        if not model:
            raise ValueError(f"{self.__class__} expects 'model' key in `data` arg to `__init__`")
        return model

    def layout(self):
        raise NotImplementedError()


class Table(ModelBlock):
    DF_ATTR = "df"

    def layout(self):
        df = getattr(self.model, self.DF_ATTR)
        table = dt.DataTable(columns=[{"name": i, "id": i} for i in df.columns], data=df.to_dict("records"),)
        return table
