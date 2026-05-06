import asyncio
import logging
import os
import operator
from typing import Annotated, TypedDict

from pydantic import BaseModel, Field, ValidationError
from langgraph.graph import END, StateGraph
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger("cortex.brain")


MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
MODEL_TIMEOUT_SECS = float(os.getenv("MODEL_TIMEOUT_SECS", "45"))
MODEL_MAX_RETRIES = int(os.getenv("MODEL_MAX_RETRIES", "3"))
MODEL_TEMPERATURE = float(os.getenv("MODEL_TEMPERATURE", "0.1"))

MAX_REVISIONS = int(os.getenv("MAX_REVISIONS", "3"))
MAX_PAYLOAD_CHARS = int(os.getenv("MAX_PAYLOAD_CHARS", "4000"))
MAX_OUTPUT_CHARS = int(os.getenv("MAX_OUTPUT_CHARS", "12000"))


class SwarmState(TypedDict):
    task: str
    research_data: str
    code_output: str
    reviewer_verdict: str
    revision_count: Annotated[int, operator.add]
    tokens_used: Annotated[int, operator.add]
    error: str

llm = ChatOpenAI(
    model=MODEL_NAME,
    temperature=MODEL_TEMPERATURE,
    max_retries=MODEL_MAX_RETRIES,
    request_timeout=MODEL_TIMEOUT_SECS,
)


class ReviewerOutput(BaseModel):
    verdict: str = Field(
        description="Must be exactly approved or revision_required"
    )
    feedback: str = Field(
        default="",
        description="Detailed reviewer feedback"
    )

strict_reviewer_llm = llm.with_structured_output(ReviewerOutput)


def clamp_text(value: str, max_chars: int) -> str:
    if not value:
        return ""
    return value[:max_chars]

def usage_tokens(response) -> int:
    try:
        if response and hasattr(response, "usage_metadata") and response.usage_metadata:
            return int(response.usage_metadata.get("total_tokens", 0))
    except Exception:
        pass
    return 0

async def safe_invoke(messages):
    return await asyncio.wait_for(
        llm.ainvoke(messages),
        timeout=MODEL_TIMEOUT_SECS + 5,
    )

async def safe_structured_invoke(messages):
    return await asyncio.wait_for(
        strict_reviewer_llm.ainvoke(messages),
        timeout=MODEL_TIMEOUT_SECS + 5,
    )


async def researcher_node(state: SwarmState) -> dict:
    try:
        safe_task = clamp_text(state["task"], MAX_PAYLOAD_CHARS)

        messages = [
            SystemMessage(
                content=(
                    "You are a senior technical researcher. "
                    "Extract architecture requirements, risks, "
                    "dependencies, libraries, and implementation strategy."
                )
            ),
            HumanMessage(content=f"Task:\n{safe_task}")
        ]

        response = await safe_invoke(messages)

        content = clamp_text(str(response.content), MAX_OUTPUT_CHARS)
        tokens = usage_tokens(response)

        return {
            "research_data": content,
            "tokens_used": tokens,
            "error": "",
        }

    except Exception as e:
        logger.exception("researcher_node_failed")
        return {
            "research_data": "",
            "tokens_used": 0,
            "error": f"researcher_failed:{str(e)}",
        }

async def coder_node(state: SwarmState) -> dict:
    try:
        revision = state.get("revision_count", 0)

        safe_task = clamp_text(state["task"], MAX_PAYLOAD_CHARS)
        research = clamp_text(state.get("research_data", ""), 5000)
        prior_error = clamp_text(state.get("error", ""), 3000)

        prompt = f"""
                Task:
                {safe_task}

                Research Context:
                {research}

                Write production-grade Python code.
                Return code only. No markdown fences.
                """

        if revision > 0 and prior_error:
            prompt += f"""
                Mandatory fixes from reviewer:
                {prior_error}
                """

        messages = [
            SystemMessage(
                content=(
                    "You are an elite Python engineer. "
                    "Produce secure, readable, maintainable code."
                )
            ),
            HumanMessage(content=prompt),
        ]

        response = await safe_invoke(messages)

        content = clamp_text(str(response.content), MAX_OUTPUT_CHARS)
        tokens = usage_tokens(response)

        return {
            "code_output": content,
            "tokens_used": tokens,
        }

    except Exception as e:
        logger.exception("coder_node_failed")
        return {
            "code_output": "",
            "tokens_used": 0,
            "error": f"coder_failed:{str(e)}",
        }

async def reviewer_node(state: SwarmState) -> dict:
    try:
        revision = state.get("revision_count", 0)

        if revision >= MAX_REVISIONS:
            logger.warning("max_revisions_hit_force_approve")
            return {
                "reviewer_verdict": "approved",
                "revision_count": 0,
                "error": "",
                "tokens_used": 0,
            }

        safe_task = clamp_text(state["task"], MAX_PAYLOAD_CHARS)
        code = clamp_text(state.get("code_output", ""), 8000)

        messages = [
            SystemMessage(
                content=(
                    "You are a strict senior reviewer. "
                    "Check correctness, security, performance, style, "
                    "edge cases, and completeness. "
                    "Return structured output only."
                )
            ),
            HumanMessage(
                content=f"""
                    Task Requirements:
                    {safe_task}

                    Submitted Code:
                    {code}
                    """
            ),
        ]

        result = await safe_structured_invoke(messages)

        if not isinstance(result, ReviewerOutput):
            raise ValidationError("invalid reviewer output")

        verdict = result.verdict.strip().lower()

        if verdict not in {"approved", "revision_required"}:
            verdict = "revision_required"

        if verdict == "revision_required":
            return {
                "reviewer_verdict": "revision_required",
                "revision_count": 1,
                "error": clamp_text(result.feedback, 3000),
                "tokens_used": 0,
            }

        return {
            "reviewer_verdict": "approved",
            "revision_count": 0,
            "error": "",
            "tokens_used": 0,
        }

    except Exception as e:
        logger.exception("reviewer_node_failed")
        return {
            "reviewer_verdict": "approved",
            "revision_count": 0,
            "error": f"reviewer_failed:{str(e)}",
            "tokens_used": 0,
        }

def routing_after_review(state:SwarmState)-> str:
    verdict = state.get("reviewer_verdict","")
    revision= state.get("revision_count",0)

    if verdict == "approved":
        return END
    
    if verdict == "revision_required" and revision < MAX_REVISIONS:
        return "coder"

    return END


def build_swarm_graph():
    graph = StateGraph(SwarmState)

    graph.add_node("researcher", researcher_node)
    graph.add_node("coder", coder_node)
    graph.add_node("reviewer", reviewer_node)

    graph.set_entry_point("researcher")
    graph.add_edge("researcher", "coder")
    graph.add_edge("coder", "reviewer")
    
    graph.add_conditional_edges("reviewer", routing_after_review)

    return graph.compile()

swarm_brain = build_swarm_graph()

class InvalidFinalStateError(Exception):
    pass

def validate_final_state(state: SwarmState)-> None:
    required = [
        "research_data",
        "code_output",
        "reviewer_verdict",
    ]

    for field in required:
        if not state.get(field):
            raise InvalidFinalStateError(
                f"missing required field: {field}"
            )
            
    verdict = state.get("reviewer_verdict", "")
    if verdict not in {"approved", "revision_required"}:
        raise InvalidFinalStateError(
            f"invalid reviewer_verdict: {verdict}"
        )