import asyncio

from eos.campaigns.entities.campaign import CampaignStatus
from tests.fixtures import *

LAB_ID = "multiplication_lab"
CAMPAIGN_ID = "optimize_multiplication_campaign"
EXPERIMENT_TYPE = "optimize_multiplication"
MAX_EXPERIMENTS = 40
DO_OPTIMIZATION = True


@pytest.mark.parametrize(
    "setup_lab_experiment",
    [(LAB_ID, EXPERIMENT_TYPE)],
    indirect=True,
)
@pytest.mark.parametrize(
    "campaign_executor",
    [(CAMPAIGN_ID, EXPERIMENT_TYPE, MAX_EXPERIMENTS, DO_OPTIMIZATION)],
    indirect=True,
)
class TestCampaignExecutor:
    @pytest.mark.asyncio
    async def test_start_campaign(self, campaign_executor, campaign_manager):
        await campaign_executor.start_campaign()

        campaign = campaign_manager.get_campaign(CAMPAIGN_ID)
        assert campaign is not None
        assert campaign.id == CAMPAIGN_ID
        assert campaign.status == CampaignStatus.RUNNING

    @pytest.mark.asyncio
    async def test_progress_campaign(self, campaign_executor, campaign_manager, campaign_optimizer_manager):
        await campaign_executor.start_campaign()

        campaign_finished = False
        while not campaign_finished:
            campaign_finished = await campaign_executor.progress_campaign()
            await asyncio.sleep(0.1)

        solutions = await campaign_executor.optimizer.get_optimal_solutions.remote()
        assert not solutions.empty
        assert len(solutions) == 1
        assert solutions["compute_multiplication_objective.objective"].iloc[0] / 100 <= 80
