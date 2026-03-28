from __future__ import annotations

from oss_ai_stack_map.config.loader import RuntimeConfig, TechnologyAlias

ANCHOR_CATEGORY_IDS = {
    "model_access_and_providers",
    "serving_inference_and_local_runtimes",
}
ANCHOR_CAPABILITIES = {
    "inference",
    "llm_proxy",
    "local_inference",
    "model_access",
    "model_runtime",
}
ANCHOR_TECHNOLOGY_IDS = {
    "huggingface-hub",
    "litellm",
    "llama-cpp",
    "ollama",
    "openrouter",
    "pytorch",
    "tokenizers",
    "transformers",
    "vllm",
}


def is_llm_anchor_technology(technology: TechnologyAlias) -> bool:
    if technology.provider_id is not None:
        return True
    if technology.category_id in ANCHOR_CATEGORY_IDS:
        return True
    if technology.technology_id in ANCHOR_TECHNOLOGY_IDS:
        return True
    return bool(set(technology.capabilities) & ANCHOR_CAPABILITIES)


def llm_anchor_technology_ids(runtime: RuntimeConfig) -> set[str]:
    return {
        technology.technology_id
        for technology in [*runtime.aliases.technologies, *runtime.registry.technologies]
        if is_llm_anchor_technology(technology)
    }
