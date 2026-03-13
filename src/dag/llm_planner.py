"""
LLM-powered DAG decomposition — converts natural language into simulation DAGs.

Uses the OpenAI API to break a research goal into a structured DAG
of simulation sub-jobs. The LLM output is parsed, validated against
the DAG schema, and optionally created in the database.
"""

import json
import re
import time

from openai import AsyncOpenAI

from src.config import settings
from src.core.exceptions import LLMError, LLMParseError, LLMValidationError
from src.schemas.dag import DagSubmitRequest, DagNodeSpec

import structlog

logger = structlog.get_logger(__name__)

# System prompt template for the LLM
SYSTEM_PROMPT = """You are a scientific simulation planner. Given a natural-language research goal,
decompose it into a directed acyclic graph (DAG) of concrete simulation sub-jobs.

## Available Simulation Types
{available_types_description}

## Rules
1. Each job must have unique node_id (snake_case, descriptive, e.g., "mesh_coarse", "sim_re1e4")
2. Each job must reference a valid type from available types
3. Each job's params must include reasonable simulation parameters
4. Dependencies (depends_on) must reference existing node_ids
5. The graph must be acyclic (no circular dependencies)
6. Use minimum number of jobs necessary — don't over-decompose
7. Independent jobs should NOT depend on each other (maximize parallelism)
8. Always include post-processing/analysis job if goal implies comparison or reporting
9. Maximum {max_nodes} nodes per graph

## Output Format
You MUST respond with ONLY a JSON object matching this exact schema:
{{
  "name": "descriptive DAG name",
  "nodes": [
    {{
      "node_id": "string",
      "job_type": "simulation",
      "params": {{"steps": 100}},
      "depends_on": ["string"],
      "priority": "normal"
    }}
  ]
}}

Do NOT include any text before or after the JSON."""

# Default simulation types available for decomposition
DEFAULT_SIMULATION_TYPES = [
    {
        "type": "simulation",
        "description": "General-purpose scientific simulation",
        "params": {"steps": "int (number of simulation steps, default 100)"},
    },
    {
        "type": "cfd_simulation",
        "description": "Computational fluid dynamics simulation",
        "params": {
            "mesh_resolution": "int",
            "reynolds_number": "float",
            "time_steps": "int",
        },
    },
    {
        "type": "thermal_simulation",
        "description": "Thermal/heat transfer simulation",
        "params": {
            "temperature": "float (Kelvin)",
            "mesh_resolution": "int",
            "time_steps": "int",
        },
    },
    {
        "type": "structural_analysis",
        "description": "Structural mechanics / FEA simulation",
        "params": {
            "load_magnitude": "float",
            "mesh_resolution": "int",
            "iterations": "int",
        },
    },
    {
        "type": "post_processing",
        "description": "Analysis and comparison of simulation results",
        "params": {
            "analysis_type": "str (comparison, report, visualization)",
            "steps": "int",
        },
    },
]


async def decompose_instruction(
    instruction: str,
    *,
    available_types: list[str] | None = None,
    failure_policy: str = "skip_downstream",
) -> tuple[DagSubmitRequest, str, int]:
    """
    Use the OpenAI API to decompose a natural language instruction
    into a structured DAG specification.

    Args:
        instruction: Natural language description of the research goal.
        available_types: Optional filter — only include these simulation types.
        failure_policy: Failure policy for the generated DAG.

    Returns:
        Tuple of (DagSubmitRequest, model_used, latency_ms).

    Raises:
        LLMError: If the OpenAI API call fails.
        LLMParseError: If the LLM response is not valid JSON.
        LLMValidationError: If the parsed response doesn't match the schema.
    """
    # Filter simulation types if requested
    types_to_use = DEFAULT_SIMULATION_TYPES
    if available_types:
        types_to_use = [t for t in DEFAULT_SIMULATION_TYPES if t["type"] in available_types]
        if not types_to_use:
            types_to_use = DEFAULT_SIMULATION_TYPES

    # Build the system prompt
    types_json = json.dumps(types_to_use, indent=2)
    system_prompt = SYSTEM_PROMPT.format(
        available_types_description=types_json,
        max_nodes=settings.max_dag_nodes,
    )

    # Call the OpenAI API
    start_time = time.time()
    model = settings.llm_model

    try:
        client = AsyncOpenAI(api_key=settings.llm_api_key, base_url=settings.llm_base_url)
        response = await client.chat.completions.create(
            model=model,
            max_tokens=settings.llm_max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": instruction},
            ],
        )
    except Exception as e:
        raise LLMError(f"OpenAI API error: {e}")

    latency_ms = int((time.time() - start_time) * 1000)

    # Extract text content from the response
    raw_text = response.choices[0].message.content or ""

    if not raw_text.strip():
        raise LLMParseError(raw_text, "Empty response from LLM")

    logger.info(
        "llm_response_received",
        model=model,
        latency_ms=latency_ms,
        response_length=len(raw_text),
    )

    # Parse the JSON response (strip markdown code fences if present)
    dag_spec = _parse_llm_response(raw_text, failure_policy)

    return dag_spec, model, latency_ms


def _parse_llm_response(raw_text: str, failure_policy: str) -> DagSubmitRequest:
    """
    Parse the LLM text response into a DagSubmitRequest.

    Handles common LLM quirks: markdown code fences, leading/trailing
    text around JSON, and missing fields.

    Args:
        raw_text: Raw text from the LLM response.
        failure_policy: Failure policy to set on the DAG.

    Returns:
        Parsed and validated DagSubmitRequest.

    Raises:
        LLMParseError: If the text can't be parsed as JSON.
        LLMValidationError: If the JSON doesn't match the schema.
    """
    # Strip markdown code fences (```json ... ```)
    cleaned = raw_text.strip()
    fence_pattern = r"```(?:json)?\s*([\s\S]*?)\s*```"
    match = re.search(fence_pattern, cleaned)
    if match:
        cleaned = match.group(1).strip()

    # Try to parse as JSON
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise LLMParseError(raw_text, str(e))

    # Validate and build the DagSubmitRequest
    try:
        # Ensure required fields
        if "nodes" not in data:
            raise LLMValidationError(["Missing 'nodes' field in LLM output"])

        name = data.get("name", "LLM-generated DAG")

        nodes = []
        for node_data in data["nodes"]:
            if "node_id" not in node_data:
                raise LLMValidationError(["Node missing 'node_id' field"])
            if "job_type" not in node_data:
                raise LLMValidationError([f"Node '{node_data.get('node_id', '?')}' missing 'job_type'"])

            nodes.append(DagNodeSpec(
                node_id=node_data["node_id"],
                job_type=node_data["job_type"],
                params=node_data.get("params", {}),
                depends_on=node_data.get("depends_on", []),
                priority=node_data.get("priority", "normal"),
                max_retries=node_data.get("max_retries", 3),
            ))

        return DagSubmitRequest(
            name=name,
            description=data.get("description"),
            failure_policy=failure_policy,
            nodes=nodes,
            metadata=data.get("metadata", {}),
        )

    except LLMValidationError:
        raise
    except Exception as e:
        raise LLMValidationError([f"Failed to build DAG spec: {str(e)}"])
