# Consistent color scheme for all adapters across all plots
# Using standard data visualization colors for better clarity
from matplotlib.pyplot import get_cmap

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


def get_adapter_color(adapter_name):
    """Get consistent color for an adapter."""
    return ADAPTER_COLORS.get(adapter_name, '#cccccc')