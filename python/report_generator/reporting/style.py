# Consistent color scheme for all adapters across all plots
# Using standard data visualization colors for better clarity
ADAPTER_COLORS = {
    'umadb': '#d62728',        # Red
    'kurrentdb': '#1f77b4',    # Blue
    'axonserver': '#2ca02c',   # Green
    'eventsourcingdb': '#ff7f0e',  # Orange
    'dummy': '#888888',        # Grey
}


def get_adapter_color(adapter_name):
    """Get consistent color for an adapter."""
    return ADAPTER_COLORS.get(adapter_name, '#cccccc')