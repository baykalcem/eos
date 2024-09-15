from pathlib import Path

from eos.configuration.constants import COMMON_DIR, EXPERIMENTS_DIR, LABS_DIR, DEVICES_DIR, TASKS_DIR


class Package:
    """
    A collection of user-defined common files, experiments, labs, devices, and tasks.
    """

    def __init__(self, name: str, path: str):
        self.name = name
        self.path = Path(path)
        self.common_dir = self.path / COMMON_DIR
        self.experiments_dir = self.path / EXPERIMENTS_DIR
        self.labs_dir = self.path / LABS_DIR
        self.devices_dir = self.path / DEVICES_DIR
        self.tasks_dir = self.path / TASKS_DIR
