# Consistent color scheme for all adapters across all plots
# Using standard data visualization colors for better clarity
from typing import Any
from matplotlib.pyplot import get_cmap

# Standardize dimensions for all plots
# At 100 DPI, 8x5 is 800x500 pixels.
# This fits well in reports while remaining legible.
PLOT_WIDTH = 8
PLOT_HEIGHT = 5
PLOT_DPI = 150

# Font sizes for consistency
FONT_SIZE_TITLE = 14
FONT_SIZE_LABEL = 12
FONT_SIZE_TICK = 10
FONT_SIZE_LEGEND = 10

adapters = [
    'umadb',
    'kurrentdb',
    'axonserver',
    'marten',
    'py-eventsourcing',
    '',
    'eventsourcingdb',
    'fact',
    'dummy',
]

cmap = get_cmap("Set1")

ADAPTER_COLORS = {
    adapter: cmap(i)
    for i, adapter in enumerate(adapters)
}


def get_adapter_color(adapter_name: str) -> Any:
    """Get consistent color for an adapter."""
    return ADAPTER_COLORS.get(adapter_name, '#cccccc')