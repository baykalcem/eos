from collections import defaultdict

from eos.configuration.exceptions import EosMissingConfigurationError
from eos.configuration.packages.entities import EntityType, EntityLocationInfo, ENTITY_INFO
from eos.configuration.packages.package import Package


class EntityIndex:
    """
    Indexes entities in packages for quick lookup.
    """

    def __init__(self):
        self._entity_indices: dict[EntityType, dict[str, EntityLocationInfo]] = defaultdict(dict)

    def build_indices(self, packages: dict[str, Package]) -> None:
        self._entity_indices.clear()
        for package in packages.values():
            self._index_package(package)

    def add_package(self, package: Package) -> None:
        self._index_package(package)

    def remove_package(self, package_name: str) -> None:
        for entity_type in self._entity_indices:
            self._entity_indices[entity_type] = {
                entity_name: location
                for entity_name, location in self._entity_indices[entity_type].items()
                if location.package_name != package_name
            }

    def _index_package(self, package: Package) -> None:
        for entity_type in EntityType:
            entity_dir = package.get_entity_dir(entity_type)
            config_file_name = ENTITY_INFO[entity_type].config_file_name

            for entity_path in entity_dir.rglob(config_file_name):
                relative_path = entity_path.relative_to(entity_dir).parent
                entity_name = relative_path.name
                self._entity_indices[entity_type][entity_name] = EntityLocationInfo(package.name, str(relative_path))

    def get_entity_location(self, entity_name: str, entity_type: EntityType) -> EntityLocationInfo:
        try:
            return self._entity_indices[entity_type][entity_name]
        except KeyError as e:
            raise EosMissingConfigurationError(f"{entity_type.name.capitalize()} '{entity_name}' not found") from e

    def get_entities_in_package(self, package_name: str, entity_type: EntityType) -> list[str]:
        return [
            entity_name
            for entity_name, location in self._entity_indices[entity_type].items()
            if location.package_name == package_name
        ]
