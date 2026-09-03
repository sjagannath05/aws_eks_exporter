"""Helpers for reading kubeconfig files without a live cluster connection.

Used by eks-config-exporter.py to:
  * list / select contexts (multi-cluster kubeconfigs),
  * derive the EKS cluster name, region and account from the selected
    context (cluster ARN first, ``aws eks get-token`` exec args second).

Pure functions over the parsed YAML; no kubernetes client involved.
"""

import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import yaml

DEFAULT_KUBECONFIG = os.path.join("~", ".kube", "config")
_EKS_CLUSTER_ARN = re.compile(
    r"^arn:aws[a-z-]*:eks:(?P<region>[a-z0-9-]+):(?P<account>\d{12}):cluster/(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)$"
)


class KubeconfigError(Exception):
    """Raised when a kubeconfig cannot be read or a context cannot be resolved."""


@dataclass(frozen=True)
class ContextInfo:
    name: str
    cluster: str
    user: Optional[str]
    server: Optional[str]
    namespace: Optional[str] = None


@dataclass(frozen=True)
class EksIdentity:
    cluster_name: str
    region: Optional[str]
    account: Optional[str]
    source: str  # "cluster-arn" | "exec-args"


def kubeconfig_paths(path: Optional[str] = None) -> List[str]:
    """Resolve the kubeconfig search list the same way kubectl does.

    Explicit ``path`` wins, then ``$KUBECONFIG`` (path-separator list), then
    ``~/.kube/config``. An empty ``$KUBECONFIG`` is treated as unset.
    """
    if path:
        raw = [path]
    else:
        env = os.environ.get("KUBECONFIG", "")
        raw = [p for p in env.split(os.pathsep) if p] or [DEFAULT_KUBECONFIG]
    return [os.path.expanduser(p) for p in raw]


def load_kubeconfig(path: Optional[str] = None) -> Dict:
    """Load and merge kubeconfig file(s). First file wins on duplicate names."""
    merged: Dict = {"clusters": [], "contexts": [], "users": [], "current-context": None}
    seen = {k: set() for k in ("clusters", "contexts", "users")}

    for p in kubeconfig_paths(path):
        if not os.path.isfile(p):
            raise KubeconfigError(f"kubeconfig file not found: {p}")
        try:
            with open(p, "r") as fh:
                doc = yaml.safe_load(fh) or {}
        except (OSError, yaml.YAMLError) as e:
            raise KubeconfigError(f"failed to read kubeconfig {p}: {e}") from e
        if not isinstance(doc, dict):
            raise KubeconfigError(f"kubeconfig {p} is not a mapping")

        for key in ("clusters", "contexts", "users"):
            for entry in doc.get(key) or []:
                name = (entry or {}).get("name")
                if name and name not in seen[key]:
                    seen[key].add(name)
                    merged[key].append(entry)
        if merged["current-context"] is None and doc.get("current-context"):
            merged["current-context"] = doc["current-context"]

    return merged


def list_context_names(cfg: Dict) -> List[str]:
    return [c["name"] for c in cfg.get("contexts") or [] if c.get("name")]


def current_context_name(cfg: Dict) -> Optional[str]:
    return cfg.get("current-context") or None


def _by_name(entries: List[Dict], name: str) -> Optional[Dict]:
    for e in entries or []:
        if e.get("name") == name:
            return e
    return None


def resolve_context(cfg: Dict, context: Optional[str] = None) -> ContextInfo:
    """Return the named context (or current-context) with its cluster/user resolved."""
    name = context or current_context_name(cfg)
    available = list_context_names(cfg)
    if not name:
        raise KubeconfigError(
            f"kubeconfig has no current-context; pass --context (available: {', '.join(available) or 'none'})"
        )
    ctx_entry = _by_name(cfg.get("contexts"), name)
    if ctx_entry is None:
        raise KubeconfigError(
            f"context '{name}' not found in kubeconfig (available: {', '.join(available) or 'none'})"
        )
    ctx = ctx_entry.get("context") or {}
    cluster_name = ctx.get("cluster")
    if not cluster_name:
        raise KubeconfigError(f"context '{name}' has no cluster reference")
    cluster_entry = _by_name(cfg.get("clusters"), cluster_name) or {}
    server = (cluster_entry.get("cluster") or {}).get("server")
    return ContextInfo(
        name=name,
        cluster=cluster_name,
        user=ctx.get("user"),
        server=server,
        namespace=ctx.get("namespace"),
    )


def parse_eks_cluster_arn(arn: Optional[str]) -> Optional[Tuple[str, str, str]]:
    """``arn:aws:eks:<region>:<account>:cluster/<name>`` -> (region, account, name)."""
    if not arn or not isinstance(arn, str):
        return None
    m = _EKS_CLUSTER_ARN.match(arn.strip())
    if not m:
        return None
    return m.group("region"), m.group("account"), m.group("name")


def _exec_args_identity(cfg: Dict, user_name: Optional[str]) -> Optional[EksIdentity]:
    """Derive identity from an ``aws eks get-token --cluster-name X --region Y`` exec block."""
    if not user_name:
        return None
    user_entry = _by_name(cfg.get("users"), user_name) or {}
    exec_cfg = (user_entry.get("user") or {}).get("exec") or {}
    args = [str(a) for a in (exec_cfg.get("args") or [])]
    if "get-token" not in args:
        return None

    def _flag(flag: str) -> Optional[str]:
        for i, a in enumerate(args):
            if a == flag and i + 1 < len(args):
                return args[i + 1]
            if a.startswith(flag + "="):
                return a.split("=", 1)[1]
        return None

    name = _flag("--cluster-name")
    if not name:
        return None
    return EksIdentity(cluster_name=name, region=_flag("--region"), account=None, source="exec-args")


def eks_identity_for_context(cfg: Dict, context: Optional[str] = None) -> Optional[EksIdentity]:
    """Best-effort EKS cluster identity for a context; None if it is not an EKS cluster."""
    ctx = resolve_context(cfg, context)
    parsed = parse_eks_cluster_arn(ctx.cluster)
    if parsed:
        region, account, name = parsed
        return EksIdentity(cluster_name=name, region=region, account=account, source="cluster-arn")
    return _exec_args_identity(cfg, ctx.user)
