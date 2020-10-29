from dataforest.core.PlotMethods import PlotMethods
from dataforest.plot.PlotWidget import PlotWidget


class PlotTreeMethods(PlotMethods):
    def __init__(self, tree):
        self._tree = tree
        for method_name in self.plot_method_lookup.keys():
            setattr(self, method_name, self._widget_wrap(method_name))

    def _widget_wrap(self, method_name):
        def wrap(**kwargs):
            plot_key = self.method_key_lookup[method_name]
            widget = PlotWidget(self._tree, plot_key, **kwargs)
            return widget.build_control(show=True, stop_on_error=True)

        wrap.__name__ = method_name
        return wrap
