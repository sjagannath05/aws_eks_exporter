import os
import textwrap

import pytest

import kubeconfig_utils as ku

ACCOUNT = "111122223333"


def _write(tmp_path, name, body):
    p = tmp_path / name
    p.write_text(textwrap.dedent(body))
    return str(p)


@pytest.fixture
def multi_kubeconfig(tmp_path):
    return _write(tmp_path, "multi.yaml", f"""
        apiVersion: v1
        kind: Config
        current-context: ch1
        clusters:
        - name: arn:aws:eks:us-east-2:{ACCOUNT}:cluster/use2-cluster
          cluster:
            server: https://use2.example
        - name: arn:aws:eks:eu-central-1:{ACCOUNT}:cluster/euc1-cluster
          cluster:
            server: https://euc1.example
        - name: plain-cluster
          cluster:
            server: https://plain.example
        contexts:
        - name: ch1
          context:
            cluster: arn:aws:eks:us-east-2:{ACCOUNT}:cluster/use2-cluster
            user: use2-user
        - name: fr5
          context:
            cluster: arn:aws:eks:eu-central-1:{ACCOUNT}:cluster/euc1-cluster
            user: euc1-user
        - name: plain
          context:
            cluster: plain-cluster
            user: plain-user
        - name: exec-only
          context:
            cluster: plain-cluster
            user: exec-user
        users:
        - name: use2-user
          user:
            exec:
              command: aws
              args: [--region, us-east-2, eks, get-token, --cluster-name, use2-cluster]
        - name: euc1-user
          user:
            exec:
              command: aws
              args: [--region, eu-central-1, eks, get-token, --cluster-name, euc1-cluster]
        - name: plain-user
          user:
            token: abc
        - name: exec-user
          user:
            exec:
              command: aws
              args: [eks, get-token, --cluster-name, from-exec, --region, ap-southeast-2]
    """)


def test_parse_eks_cluster_arn():
    arn = f"arn:aws:eks:us-east-2:{ACCOUNT}:cluster/my-cluster"
    assert ku.parse_eks_cluster_arn(arn) == ("us-east-2", ACCOUNT, "my-cluster")


@pytest.mark.parametrize("bad", ["", None, "my-cluster", "arn:aws:iam::123:role/x",
                                 f"arn:aws:eks:us-east-2:{ACCOUNT}:nodegroup/c/ng/1"])
def test_parse_eks_cluster_arn_rejects_non_cluster_arns(bad):
    assert ku.parse_eks_cluster_arn(bad) is None


def test_list_contexts_and_current(multi_kubeconfig):
    cfg = ku.load_kubeconfig(multi_kubeconfig)
    assert ku.list_context_names(cfg) == ["ch1", "fr5", "plain", "exec-only"]
    assert ku.current_context_name(cfg) == "ch1"


def test_resolve_context_defaults_to_current(multi_kubeconfig):
    cfg = ku.load_kubeconfig(multi_kubeconfig)
    ctx = ku.resolve_context(cfg)
    assert ctx.name == "ch1"
    assert ctx.cluster == f"arn:aws:eks:us-east-2:{ACCOUNT}:cluster/use2-cluster"
    assert ctx.user == "use2-user"
    assert ctx.server == "https://use2.example"


def test_resolve_context_unknown_raises(multi_kubeconfig):
    cfg = ku.load_kubeconfig(multi_kubeconfig)
    with pytest.raises(ku.KubeconfigError, match="exec-only|fr5"):
        ku.resolve_context(cfg, "nope")


def test_eks_identity_from_cluster_arn(multi_kubeconfig):
    cfg = ku.load_kubeconfig(multi_kubeconfig)
    ident = ku.eks_identity_for_context(cfg, "fr5")
    assert ident.cluster_name == "euc1-cluster"
    assert ident.region == "eu-central-1"
    assert ident.account == ACCOUNT
    assert ident.source == "cluster-arn"


def test_eks_identity_falls_back_to_exec_args(multi_kubeconfig):
    cfg = ku.load_kubeconfig(multi_kubeconfig)
    ident = ku.eks_identity_for_context(cfg, "exec-only")
    assert ident.cluster_name == "from-exec"
    assert ident.region == "ap-southeast-2"
    assert ident.account is None
    assert ident.source == "exec-args"


def test_eks_identity_none_for_non_eks_context(multi_kubeconfig):
    cfg = ku.load_kubeconfig(multi_kubeconfig)
    assert ku.eks_identity_for_context(cfg, "plain") is None


def test_load_kubeconfig_uses_env_and_merges(tmp_path, monkeypatch):
    a = _write(tmp_path, "a.yaml", """
        apiVersion: v1
        kind: Config
        current-context: one
        clusters:
        - {name: c1, cluster: {server: https://one}}
        contexts:
        - {name: one, context: {cluster: c1, user: u1}}
        users:
        - {name: u1, user: {token: t}}
    """)
    b = _write(tmp_path, "b.yaml", """
        apiVersion: v1
        kind: Config
        current-context: two
        clusters:
        - {name: c2, cluster: {server: https://two}}
        - {name: c1, cluster: {server: https://SHOULD-LOSE}}
        contexts:
        - {name: two, context: {cluster: c2, user: u2}}
        users:
        - {name: u2, user: {token: t}}
    """)
    monkeypatch.setenv("KUBECONFIG", f"{a}{os.pathsep}{b}")
    cfg = ku.load_kubeconfig()
    assert ku.list_context_names(cfg) == ["one", "two"]
    assert ku.current_context_name(cfg) == "one"          # first file wins
    assert ku.resolve_context(cfg, "one").server == "https://one"  # first file wins on dup


def test_load_kubeconfig_missing_file_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "missing.yaml"))
    with pytest.raises(ku.KubeconfigError):
        ku.load_kubeconfig()


def test_load_kubeconfig_empty_env_falls_back_to_default(tmp_path, monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "")
    monkeypatch.setenv("HOME", str(tmp_path))
    with pytest.raises(ku.KubeconfigError, match=r"\.kube/config"):
        ku.load_kubeconfig()
