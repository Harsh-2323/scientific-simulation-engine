"""
Pluggable simulation execution runner.

For this assignment, simulates work with configurable steps and durations
rather than running actual CFD/FEA solvers. Each step sleeps briefly,
accumulates simulated metrics, and produces a result dict on completion.

In a production system, this module would be replaced with adapters
that launch real simulation binaries (OpenFOAM, GROMACS, CalculiX, etc.).
"""

import asyncio
import random

from src.config import settings

import structlog

logger = structlog.get_logger(__name__)


class SimulationRunner:
    """
    Simulated workload runner.

    Each step represents a chunk of computation. The runner tracks
    accumulated state that can be serialized to a checkpoint and
    restored later for pause/resume support.
    """

    def __init__(self, job_type: str, params: dict) -> None:
        """
        Initialize the simulation runner.

        Args:
            job_type: The type of simulation (used for logging context).
            params: Simulation parameters dict from the job.
        """
        self.job_type = job_type
        self.params = params

        # Use params to configure step count, or fall back to config defaults
        self._total_steps = params.get("steps", settings.default_simulation_steps)
        self._step_duration = params.get(
            "step_duration", settings.default_step_duration_seconds
        )

    def total_steps(self) -> int:
        """Return the total number of steps this simulation will execute."""
        return self._total_steps

    async def step(self, step_number: int, state: dict) -> dict:
        """
        Execute a single simulation step.

        Simulates computation by sleeping for a configured duration,
        then updates the state with accumulated metrics.

        Args:
            step_number: The current step index (0-based).
            state: Mutable state dict carried between steps.

        Returns:
            The updated state dict after this step.
        """
        # Simulate computation time
        await asyncio.sleep(self._step_duration)

        # Accumulate simulated metrics
        if "accumulated_value" not in state:
            state["accumulated_value"] = 0.0
        state["accumulated_value"] += random.uniform(0.5, 1.5)

        if "step_results" not in state:
            state["step_results"] = []
        state["step_results"].append({
            "step": step_number,
            "value": state["accumulated_value"],
            "delta": random.uniform(-0.1, 0.1),
        })

        state["last_completed_step"] = step_number

        return state

    def serialize_state(self, state: dict) -> dict:
        """
        Serialize simulation state for checkpoint storage.

        Args:
            state: The current simulation state.

        Returns:
            A JSON-serializable dict suitable for storing in the checkpoints table.
        """
        return {
            "accumulated_value": state.get("accumulated_value", 0.0),
            "step_results": state.get("step_results", []),
            "last_completed_step": state.get("last_completed_step", -1),
        }

    def deserialize_state(self, data: dict) -> dict:
        """
        Restore simulation state from a checkpoint.

        Args:
            data: Checkpoint data previously returned by serialize_state().

        Returns:
            The restored state dict, ready for continued execution.
        """
        return {
            "accumulated_value": data.get("accumulated_value", 0.0),
            "step_results": data.get("step_results", []),
            "last_completed_step": data.get("last_completed_step", -1),
        }

    def build_result(self, state: dict) -> dict:
        """
        Build the final result dict from the completed simulation state.

        Called when all steps have finished successfully.

        Args:
            state: The final simulation state after all steps.

        Returns:
            Result dict stored in jobs.result on completion.
        """
        return {
            "job_type": self.job_type,
            "total_steps": self._total_steps,
            "final_value": state.get("accumulated_value", 0.0),
            "steps_completed": len(state.get("step_results", [])),
        }
