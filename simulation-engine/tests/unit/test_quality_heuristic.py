"""
Unit tests for LLM decomposition quality heuristics.

These tests verify that the quality evaluation catches suspicious
LLM output patterns without blocking valid decompositions.
"""

import pytest

from src.dag.llm_planner import evaluate_decomposition_quality
from src.schemas.dag import DagSubmitRequest, DagNodeSpec


class TestDecompositionQuality:
    """Test the quality heuristic warnings."""

    def test_reasonable_decomposition_no_warnings(self):
        """A well-structured DAG for a clear instruction should produce no warnings."""
        instruction = (
            "Run CFD simulations at 50k and 100k mesh resolutions, "
            "then compare the convergence results"
        )
        spec = DagSubmitRequest(
            name="convergence",
            nodes=[
                DagNodeSpec(node_id="mesh_50k", job_type="cfd_simulation", params={"resolution": 50000}),
                DagNodeSpec(node_id="mesh_100k", job_type="cfd_simulation", params={"resolution": 100000}),
                DagNodeSpec(node_id="compare", job_type="post_processing", params={}, depends_on=["mesh_50k", "mesh_100k"]),
            ],
        )
        warnings = evaluate_decomposition_quality(instruction, spec)
        assert len(warnings) == 0

    def test_over_granular_for_short_instruction(self):
        """Too many nodes for a short instruction should warn."""
        instruction = "Run a quick CFD test"
        nodes = [
            DagNodeSpec(node_id=f"step_{i}", job_type="simulation", params={})
            for i in range(10)
        ]
        spec = DagSubmitRequest(name="over_granular", nodes=nodes)
        warnings = evaluate_decomposition_quality(instruction, spec)
        assert any("node count" in w.lower() or "over-granular" in w.lower() for w in warnings)

    def test_all_same_job_type_warns(self):
        """All nodes having the same type is suspicious for a pipeline."""
        instruction = "Generate mesh, run simulation, and analyze results"
        spec = DagSubmitRequest(
            name="same_type",
            nodes=[
                DagNodeSpec(node_id="a", job_type="simulation", params={}),
                DagNodeSpec(node_id="b", job_type="simulation", params={}, depends_on=["a"]),
                DagNodeSpec(node_id="c", job_type="simulation", params={}, depends_on=["b"]),
            ],
        )
        warnings = evaluate_decomposition_quality(instruction, spec)
        assert any("same job type" in w.lower() for w in warnings)

    def test_missing_postprocessing_for_comparison(self):
        """Instruction implies comparison but no post-processing node."""
        instruction = "Compare drag coefficients at different mesh resolutions"
        spec = DagSubmitRequest(
            name="no_postproc",
            nodes=[
                DagNodeSpec(node_id="sim_50k", job_type="cfd_simulation", params={}),
                DagNodeSpec(node_id="sim_100k", job_type="cfd_simulation", params={}),
            ],
        )
        warnings = evaluate_decomposition_quality(instruction, spec)
        assert any("post-processing" in w.lower() for w in warnings)

    def test_linear_chain_with_parallelizable_types(self):
        """Sequential chain where nodes could run in parallel should warn."""
        instruction = "Run three independent thermal simulations"
        spec = DagSubmitRequest(
            name="linear_chain",
            nodes=[
                DagNodeSpec(node_id="t1", job_type="thermal_simulation", params={}),
                DagNodeSpec(node_id="t2", job_type="thermal_simulation", params={}, depends_on=["t1"]),
                DagNodeSpec(node_id="t3", job_type="thermal_simulation", params={}, depends_on=["t2"]),
                DagNodeSpec(node_id="t4", job_type="thermal_simulation", params={}, depends_on=["t3"]),
            ],
        )
        warnings = evaluate_decomposition_quality(instruction, spec)
        assert any("linear chain" in w.lower() or "parallel" in w.lower() for w in warnings)

    def test_two_nodes_same_type_no_false_positive(self):
        """Two nodes of the same type is fine — don't over-warn."""
        instruction = "Run simulation and post-process"
        spec = DagSubmitRequest(
            name="small",
            nodes=[
                DagNodeSpec(node_id="sim", job_type="simulation", params={}),
                DagNodeSpec(node_id="pp", job_type="post_processing", params={}, depends_on=["sim"]),
            ],
        )
        warnings = evaluate_decomposition_quality(instruction, spec)
        assert len(warnings) == 0

    def test_deep_dag_warns(self):
        """A very deep chain should warn about latency."""
        instruction = "Run a multi-stage pipeline with 8 dependent steps"
        nodes = [DagNodeSpec(node_id="s0", job_type="simulation", params={})]
        for i in range(1, 8):
            nodes.append(DagNodeSpec(
                node_id=f"s{i}", job_type="analysis" if i % 2 else "simulation",
                params={}, depends_on=[f"s{i-1}"]
            ))
        spec = DagSubmitRequest(name="deep", nodes=nodes)
        warnings = evaluate_decomposition_quality(instruction, spec)
        assert any("depth" in w.lower() for w in warnings)
