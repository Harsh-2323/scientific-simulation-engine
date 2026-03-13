"""
DAG validation — structural and semantic checks before creation.

Validates the graph structure (no cycles, valid references),
enforces size limits, and checks parameter semantics. Uses
Kahn's algorithm for topological sort and cycle detection.
"""

import re
from collections import defaultdict, deque

from src.config import settings
from src.schemas.dag import DagSubmitRequest, DagNodeSpec, ValidationResult

import structlog

logger = structlog.get_logger(__name__)

# Regex pattern for valid node IDs
NODE_ID_PATTERN = re.compile(r"^[a-z0-9_]+$")


def validate_dag(spec: DagSubmitRequest) -> ValidationResult:
    """
    Run the full validation pipeline on a DAG specification.

    Checks (in order):
        1. Node ID format and uniqueness
        2. Size limits (max nodes)
        3. Dependency reference validity
        4. Cycle detection via Kahn's algorithm
        5. Semantic validation (warnings only)

    Args:
        spec: The DAG specification to validate.

    Returns:
        ValidationResult with errors (blocking) and warnings (informational).
    """
    errors: list[str] = []
    warnings: list[str] = []

    node_ids = set()
    node_map: dict[str, DagNodeSpec] = {}

    # 1. Node ID format and uniqueness
    for node in spec.nodes:
        if not NODE_ID_PATTERN.match(node.node_id):
            errors.append(
                f"Invalid node_id '{node.node_id}': must match [a-z0-9_]+"
            )
        if node.node_id in node_ids:
            errors.append(f"Duplicate node_id: '{node.node_id}'")
        node_ids.add(node.node_id)
        node_map[node.node_id] = node

    # 2. Size limits
    if len(spec.nodes) > settings.max_dag_nodes:
        errors.append(
            f"Too many nodes: {len(spec.nodes)} exceeds limit of {settings.max_dag_nodes}"
        )

    # 3. Dependency reference validation
    for node in spec.nodes:
        for dep in node.depends_on:
            if dep == node.node_id:
                errors.append(
                    f"Self-reference: node '{node.node_id}' depends on itself"
                )
            elif dep not in node_ids:
                errors.append(
                    f"Unknown dependency: node '{node.node_id}' depends on "
                    f"'{dep}' which does not exist"
                )

    # 4. Cycle detection via Kahn's algorithm
    if not errors:  # Only check cycles if references are valid
        cycle_nodes = _detect_cycles(spec.nodes)
        if cycle_nodes:
            errors.append(
                f"Cycle detected involving nodes: {', '.join(sorted(cycle_nodes))}"
            )

    # 5. Semantic validation (warnings only)
    for node in spec.nodes:
        _check_semantics(node, warnings)

    valid = len(errors) == 0
    return ValidationResult(valid=valid, errors=errors, warnings=warnings)


def _detect_cycles(nodes: list[DagNodeSpec]) -> set[str]:
    """
    Detect cycles using Kahn's topological sort algorithm.

    Builds an adjacency list and in-degree count, then repeatedly
    removes nodes with in-degree 0. If any nodes remain after
    processing, they form a cycle.

    Args:
        nodes: List of DAG node specifications.

    Returns:
        Set of node IDs involved in cycles, or empty set if acyclic.
    """
    # Build adjacency list and in-degree map
    in_degree: dict[str, int] = defaultdict(int)
    adjacency: dict[str, list[str]] = defaultdict(list)
    all_nodes = set()

    for node in nodes:
        all_nodes.add(node.node_id)
        if node.node_id not in in_degree:
            in_degree[node.node_id] = 0

        for dep in node.depends_on:
            adjacency[dep].append(node.node_id)
            in_degree[node.node_id] += 1
            all_nodes.add(dep)

    # Start with all nodes that have no incoming edges (in-degree 0)
    queue = deque(
        nid for nid in all_nodes if in_degree.get(nid, 0) == 0
    )
    visited = set()

    while queue:
        node_id = queue.popleft()
        visited.add(node_id)

        for child in adjacency.get(node_id, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Any nodes not visited are part of a cycle
    return all_nodes - visited


def topological_sort(nodes: list[DagNodeSpec]) -> list[str]:
    """
    Return node IDs in topological order (dependencies first).

    Used during DAG creation to insert jobs in the correct order.

    Args:
        nodes: List of DAG node specifications.

    Returns:
        List of node IDs sorted so that dependencies come first.

    Raises:
        ValueError: If the graph contains a cycle.
    """
    in_degree: dict[str, int] = defaultdict(int)
    adjacency: dict[str, list[str]] = defaultdict(list)
    all_nodes = []

    for node in nodes:
        all_nodes.append(node.node_id)
        if node.node_id not in in_degree:
            in_degree[node.node_id] = 0

        for dep in node.depends_on:
            adjacency[dep].append(node.node_id)
            in_degree[node.node_id] += 1

    queue = deque(
        nid for nid in all_nodes if in_degree.get(nid, 0) == 0
    )
    sorted_order = []

    while queue:
        node_id = queue.popleft()
        sorted_order.append(node_id)

        for child in adjacency.get(node_id, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(sorted_order) != len(all_nodes):
        raise ValueError("DAG contains a cycle — topological sort impossible")

    return sorted_order


def get_root_nodes(nodes: list[DagNodeSpec]) -> list[DagNodeSpec]:
    """
    Identify root nodes — nodes with no incoming dependencies.

    Root nodes are enqueued first when a DAG starts execution.

    Args:
        nodes: List of DAG node specifications.

    Returns:
        List of DagNodeSpec that have empty depends_on lists.
    """
    return [n for n in nodes if not n.depends_on]


def _check_semantics(node: DagNodeSpec, warnings: list[str]) -> None:
    """
    Run semantic checks on node parameters (warnings only).

    These don't block creation but alert the user to potentially
    incorrect parameter values.

    Args:
        node: The node to check.
        warnings: List to append warnings to.
    """
    params = node.params

    if "reynolds_number" in params and params["reynolds_number"] <= 0:
        warnings.append(
            f"Node '{node.node_id}': Reynolds number should be > 0"
        )

    if "mesh_resolution" in params and params["mesh_resolution"] <= 0:
        warnings.append(
            f"Node '{node.node_id}': Mesh resolution should be > 0"
        )

    if "temperature" in params and params["temperature"] <= 0:
        warnings.append(
            f"Node '{node.node_id}': Temperature should be > 0 K"
        )

    if "total_time" in params and params["total_time"] <= 0:
        warnings.append(
            f"Node '{node.node_id}': Total time should be > 0"
        )
