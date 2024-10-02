from pathlib import Path
from typing import ClassVar

from eos.configuration.constants import EXPERIMENTS_DIR, LABS_DIR, DEVICES_DIR, TASKS_DIR
from eos.configuration.packages.entities import EntityType


class Package:
    """
    A collection of user-defined experiments, labs, devices, tasks, and any code and data.
    """

    ENTITY_DIR_MAP: ClassVar = {
        EntityType.EXPERIMENT: EXPERIMENTS_DIR,
        EntityType.LAB: LABS_DIR,
        EntityType.DEVICE: DEVICES_DIR,
        EntityType.TASK: TASKS_DIR,
    }

    def __init__(self, name: str, path: str):
        self.name = name
        self.path = Path(path)
        self.entity_dirs = {entity_type: self.path / dir_name for entity_type, dir_name in self.ENTITY_DIR_MAP.items()}

    def get_entity_dir(self, entity_type: EntityType) -> Path:
        return self.entity_dirs[entity_type]
