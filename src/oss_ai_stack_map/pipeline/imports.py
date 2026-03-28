from __future__ import annotations

import re
from pathlib import Path

from oss_ai_stack_map.config.loader import RuntimeConfig, TechnologyAlias
from oss_ai_stack_map.github.client import GitHubClient
from oss_ai_stack_map.models.core import ManifestDependency

MAX_IMPORT_SCAN_FILES = 40

PYTHON_IMPORT_RE = re.compile(
    r"^\s*(?:import\s+([A-Za-z_][\w\.]*)|from\s+([A-Za-z_][\w\.]*)\s+import\s+([A-Za-z_][\w]*))",
    re.MULTILINE,
)
JS_IMPORT_RE = re.compile(
    r"""(?:from|import\s*\(|require\()\s*["']([^"']+)["']""",
    re.MULTILINE,
)
GO_IMPORT_RE = re.compile(r'^\s*"([^"]+)"', re.MULTILINE)
RUST_IMPORT_RE = re.compile(r"^\s*use\s+([A-Za-z_][A-Za-z0-9_:]*)", re.MULTILINE)


def collect_import_dependencies(
    runtime: RuntimeConfig,
    client: GitHubClient,
    full_name: str,
    tree_paths: list[str],
    import_lookup: dict[str, TechnologyAlias],
) -> list[ManifestDependency]:
    owner, repo = full_name.split("/", 1)
    dependencies: list[ManifestDependency] = []
    for path in select_import_candidate_paths(runtime, tree_paths)[:MAX_IMPORT_SCAN_FILES]:
        text = safe_call(lambda path=path: client.get_file_text(owner, repo, path), default="")
        if not text:
            continue
        dependencies.extend(
            parse_import_dependencies(
                path=path,
                text=text,
                import_lookup=import_lookup,
            )
        )
    return dependencies


def select_import_candidate_paths(runtime: RuntimeConfig, tree_paths: list[str]) -> list[str]:
    candidates = []
    for path in tree_paths:
        if should_skip_path(runtime, path):
            continue
        if Path(path).suffix not in runtime.exclusions.source_extensions:
            continue
        candidates.append(path)
    return sorted(candidates, key=path_priority)


def parse_import_dependencies(
    path: str,
    text: str,
    import_lookup: dict[str, TechnologyAlias],
) -> list[ManifestDependency]:
    suffix = Path(path).suffix
    if suffix == ".py":
        raw_imports = parse_python_imports(text)
    elif suffix in {".js", ".jsx", ".ts", ".tsx"}:
        raw_imports = parse_js_imports(text)
    elif suffix == ".go":
        raw_imports = parse_go_imports(text)
    elif suffix == ".rs":
        raw_imports = parse_rust_imports(text)
    else:
        raw_imports = []

    dependencies: list[ManifestDependency] = []
    for raw_import in raw_imports:
        alias = resolve_alias(raw_import, import_lookup)
        if alias is None:
            continue
        dependencies.append(
            ManifestDependency(
                package_name=raw_import,
                dependency_scope="runtime",
                source_path=path,
                evidence_type="import",
                confidence="medium",
                raw_specifier=raw_import,
                technology_id=alias.technology_id,
                provider_id=alias.provider_id,
                provider_technology_id=alias.technology_id,
                entity_type=alias.entity_type,
                canonical_product_id=alias.canonical_product_id,
                match_method="import_alias",
            )
        )
    return dedupe_import_dependencies(dependencies)


def parse_python_imports(text: str) -> list[str]:
    imports: list[str] = []
    for match in PYTHON_IMPORT_RE.finditer(text):
        direct_import, from_module, from_name = match.groups()
        if direct_import:
            imports.append(direct_import)
            imports.append(direct_import.split(".", 1)[0])
        elif from_module:
            imports.append(from_module)
            imports.append(from_module.split(".", 1)[0])
            if from_name:
                imports.append(f"{from_module}.{from_name}")
    return imports


def parse_js_imports(text: str) -> list[str]:
    imports = []
    for raw in JS_IMPORT_RE.findall(text):
        imports.append(raw)
        if raw.startswith("@") and "/" in raw:
            imports.append(raw.rsplit("/", 1)[-1])
    return imports


def parse_go_imports(text: str) -> list[str]:
    imports = []
    for raw in GO_IMPORT_RE.findall(text):
        imports.append(raw)
        imports.append(raw.rsplit("/", 1)[-1])
    return imports


def parse_rust_imports(text: str) -> list[str]:
    imports = []
    for raw in RUST_IMPORT_RE.findall(text):
        imports.append(raw)
        imports.append(raw.split("::", 1)[0])
    return imports


def resolve_alias(
    raw_import: str,
    import_lookup: dict[str, TechnologyAlias],
) -> TechnologyAlias | None:
    candidates = [
        raw_import.casefold(),
        raw_import.replace("-", "_").casefold(),
        raw_import.replace("_", "-").casefold(),
    ]
    for candidate in candidates:
        alias = import_lookup.get(candidate)
        if alias is not None:
            return alias
    return None


def should_skip_path(runtime: RuntimeConfig, path: str) -> bool:
    normalized = path.casefold()
    if any(
        normalized.startswith(f"{directory.rstrip('/')}/".casefold())
        for directory in runtime.exclusions.excluded_directories
    ):
        return True
    if normalized.startswith("tests/") or "/tests/" in normalized:
        return True
    return False


def path_priority(path: str) -> tuple[int, str]:
    high_signal_prefixes = ("src/", "app/", "packages/", "pkg/", "server/", "cmd/", "lib/")
    if path.startswith(high_signal_prefixes):
        return (0, path)
    return (1, path)


def dedupe_import_dependencies(
    dependencies: list[ManifestDependency],
) -> list[ManifestDependency]:
    seen: dict[tuple[str, str, str], ManifestDependency] = {}
    for dep in dependencies:
        key = (dep.package_name, dep.source_path, dep.technology_id or "")
        seen[key] = dep
    return list(seen.values())


def safe_call(fn, default):
    try:
        return fn()
    except Exception:
        return default
