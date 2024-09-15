from typing import Any

import pandas as pd
import ray

from eos.optimization.abstract_sequential_optimizer import AbstractSequentialOptimizer


@ray.remote
class SequentialOptimizerActor(AbstractSequentialOptimizer):
    def __init__(self, constructor_args: dict[str, Any], optimizer_type: type[AbstractSequentialOptimizer]):
        self.optimizer = optimizer_type(**constructor_args)

    def sample(self, num_experiments: int = 1) -> pd.DataFrame:
        return self.optimizer.sample(num_experiments)

    def report(self, input_df: pd.DataFrame, output_df: pd.DataFrame) -> None:
        self.optimizer.report(input_df, output_df)

    def get_optimal_solutions(self) -> pd.DataFrame:
        return self.optimizer.get_optimal_solutions()

    def get_input_names(self) -> list[str]:
        return self.optimizer.get_input_names()

    def get_output_names(self) -> list[str]:
        return self.optimizer.get_output_names()
