"""
Unit tests for DAG validation — structure, references, cycles, and limits.
"""

import pytest

from src.dag.validator import validate_dag, topological_sort, get_root_nodes
from src.schemas.dag import DagSubmitRequest, DagNodeSpec


class TestDagValidation:
    """Test the full validation pipeline."""

    def test_valid_linear_dag(self):
        """A simple linear DAG (A -> B -> C) should pass validation."""
        spec = DagSubmitRequest(
            name="linear",
            nodes=[
                DagNodeSpec(node_id="a", job_type="sim", params={}),
                DagNodeSpec(node_id="b", job_type="sim", params={}, depends_on=["a"]),
                DagNodeSpec(node_id="c", job_type="sim", params={}, depends_on=["b"]),
            ],
        )
        result = validate_dag(spec)
        assert result.valid is True
        assert result.errors == []

    def test_valid_diamond_dag(self):
        """A diamond DAG (A -> B, A -> C, B -> D, C -> D) should pass."""
        spec = DagSubmitRequest(
            name="diamond",
            nodes=[
                DagNodeSpec(node_id="a", job_type="sim", params={}),
                DagNodeSpec(node_id="b", job_type="sim", params={}, depends_on=["a"]),
                DagNodeSpec(node_id="c", job_type="sim", params={}, depends_on=["a"]),
                DagNodeSpec(node_id="d", job_type="sim", params={}, depends_on=["b", "c"]),
            ],
        )
        result = validate_dag(spec)
        assert result.valid is True

    def test_cycle_detected(self):
        """A cycle (A -> B -> C -> A) should be rejected."""
        spec = DagSubmitRequest(
            name="cyclic",
            nodes=[
                DagNodeSpec(node_id="a", job_type="sim", params={}, depends_on=["c"]),
                DagNodeSpec(node_id="b", job_type="sim", params={}, depends_on=["a"]),
                DagNodeSpec(node_id="c", job_type="sim", params={}, depends_on=["b"]),
            ],
        )
        result = validate_dag(spec)
        assert result.valid is False
        assert any("ycle" in e for e in result.errors)

    def test_self_reference_detected(self):
        """A node depending on itself should be rejected."""
        spec = DagSubmitRequest(
            name="self",
            nodes=[DagNodeSpec(node_id="a", job_type="sim", params={}, depends_on=["a"])],
        )
        result = validate_dag(spec)
        assert result.valid is False
        assert any("Self-reference" in e for e in result.errors)

    def test_missing_dependency_reference(self):
        """A reference to a non-existent node should be rejected."""
        spec = DagSubmitRequest(
            name="badref",
            nodes=[DagNodeSpec(node_id="a", job_type="sim", params={}, depends_on=["ghost"])],
        )
        result = validate_dag(spec)
        assert result.valid is False
        assert any("ghost" in e for e in result.errors)

    def test_duplicate_node_ids(self):
        """Two nodes with the same ID should be rejected."""
        spec = DagSubmitRequest(
            name="dup",
            nodes=[
                DagNodeSpec(node_id="a", job_type="sim", params={}),
                DagNodeSpec(node_id="a", job_type="sim", params={}),
            ],
        )
        result = validate_dag(spec)
        assert result.valid is False
        assert any("Duplicate" in e for e in result.errors)

    def test_exceeds_max_nodes(self):
        """More than max_dag_nodes should be rejected."""
        nodes = [
            DagNodeSpec(node_id=f"n{i}", job_type="sim", params={})
            for i in range(51)  # Default max is 50
        ]
        spec = DagSubmitRequest(name="huge", nodes=nodes)
        result = validate_dag(spec)
        assert result.valid is False
        assert any("Too many" in e for e in result.errors)

    def test_invalid_node_id_format(self):
        """Node IDs not matching [a-z0-9_]+ should be rejected at schema level."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            DagNodeSpec(node_id="Invalid-ID!", job_type="sim", params={})

    def test_semantic_warnings(self):
        """Suspicious parameter values should produce warnings, not errors."""
        spec = DagSubmitRequest(
            name="warnings",
            nodes=[
                DagNodeSpec(
                    node_id="a", job_type="sim",
                    params={"reynolds_number": -5, "temperature": -100},
                ),
            ],
        )
        result = validate_dag(spec)
        assert result.valid is True  # Warnings don't block
        assert len(result.warnings) >= 2


class TestTopologicalSort:
    """Test topological ordering of DAG nodes."""

    def test_linear_order(self):
        """A -> B -> C should produce [A, B, C]."""
        nodes = [
            DagNodeSpec(node_id="a", job_type="sim", params={}),
            DagNodeSpec(node_id="b", job_type="sim", params={}, depends_on=["a"]),
            DagNodeSpec(node_id="c", job_type="sim", params={}, depends_on=["b"]),
        ]
        order = topological_sort(nodes)
        assert order == ["a", "b", "c"]

    def test_diamond_order(self):
        """Diamond: A before B and C, both before D."""
        nodes = [
            DagNodeSpec(node_id="a", job_type="sim", params={}),
            DagNodeSpec(node_id="b", job_type="sim", params={}, depends_on=["a"]),
            DagNodeSpec(node_id="c", job_type="sim", params={}, depends_on=["a"]),
            DagNodeSpec(node_id="d", job_type="sim", params={}, depends_on=["b", "c"]),
        ]
        order = topological_sort(nodes)
        assert order[0] == "a"
        assert order[-1] == "d"
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_cycle_raises(self):
        """Topological sort should raise ValueError for cycles."""
        nodes = [
            DagNodeSpec(node_id="x", job_type="sim", params={}, depends_on=["z"]),
            DagNodeSpec(node_id="y", job_type="sim", params={}, depends_on=["x"]),
            DagNodeSpec(node_id="z", job_type="sim", params={}, depends_on=["y"]),
        ]
        with pytest.raises(ValueError, match="cycle"):
            topological_sort(nodes)


class TestRootNodes:
    """Test root node detection."""

    def test_single_root(self):
        """A linear DAG has one root."""
        nodes = [
            DagNodeSpec(node_id="a", job_type="sim", params={}),
            DagNodeSpec(node_id="b", job_type="sim", params={}, depends_on=["a"]),
        ]
        roots = get_root_nodes(nodes)
        assert len(roots) == 1
        assert roots[0].node_id == "a"

    def test_multiple_roots(self):
        """Parallel nodes with no deps are all roots."""
        nodes = [
            DagNodeSpec(node_id="a", job_type="sim", params={}),
            DagNodeSpec(node_id="b", job_type="sim", params={}),
            DagNodeSpec(node_id="c", job_type="sim", params={}, depends_on=["a", "b"]),
        ]
        roots = get_root_nodes(nodes)
        root_ids = {r.node_id for r in roots}
        assert root_ids == {"a", "b"}
