def pytest_collection_modifyitems(items):
    """Sort tests by the slow marker. Tests with the slow marker will be executed last."""

    def weight(item):
        return 1 if item.get_closest_marker("slow") else 0

    items.sort(key=weight)
