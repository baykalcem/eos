from tests.fixtures import *


@pytest.fixture
def container_manager(configuration_manager, setup_lab_experiment, db_manager, clean_db):
  return ContainerManager(configuration_manager, db_manager)


@pytest.mark.parametrize("setup_lab_experiment", [("small_lab", "water_purification")], indirect=True)
class TestContainerManager:
  def test_set_container_location(self, container_manager):
    container_id = "acf829f859e04fee80d54a1ee918555d"
    container_manager.set_location(container_id, "new_location")

    assert container_manager.get_container(container_id).location == "new_location"

  def test_set_container_lab(self, container_manager):
    container_id = "acf829f859e04fee80d54a1ee918555d"
    container_manager.set_lab(container_id, "new_lab")

    assert container_manager.get_container(container_id).lab == "new_lab"

  def test_set_container_metadata(self, container_manager):
    container_id = "acf829f859e04fee80d54a1ee918555d"
    container_manager.set_metadata(container_id, {"substance": "water"})
    container_manager.set_metadata(container_id, {"temperature": "cold"})

    assert container_manager.get_container(container_id).metadata == {"temperature": "cold"}

  def test_add_container_metadata(self, container_manager):
    container_id = "acf829f859e04fee80d54a1ee918555d"
    container_manager.add_metadata(container_id, {"substance": "water"})
    container_manager.add_metadata(container_id, {"temperature": "cold"})

    assert container_manager.get_container(container_id).metadata == {
      "capacity": 500,
      "substance": "water",
      "temperature": "cold",
    }

  def test_remove_container_metadata(self, container_manager):
    container_id = "acf829f859e04fee80d54a1ee918555d"
    container_manager.add_metadata(container_id, {"substance": "water", "temperature": "cold", "color": "blue"})
    container_manager.remove_metadata(container_id, ["color", "temperature"])

    assert container_manager.get_container(container_id).metadata == {"capacity": 500, "substance": "water"}
