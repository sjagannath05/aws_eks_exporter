import textwrap
import types

import pytest


@pytest.fixture
def kubeconfig(tmp_path):
    p = tmp_path / "kc.yaml"
    p.write_text(textwrap.dedent("""
        apiVersion: v1
        kind: Config
        current-context: ch1
        clusters:
        - {name: 'arn:aws:eks:us-east-1:111122223333:cluster/use1', cluster: {server: https://use1}}
        - {name: 'arn:aws:eks:us-east-2:111122223333:cluster/use2', cluster: {server: https://use2}}
        contexts:
        - {name: 'arn:aws:eks:us-east-1:111122223333:cluster/use1', context: {cluster: 'arn:aws:eks:us-east-1:111122223333:cluster/use1', user: u}}
        - {name: ch1, context: {cluster: 'arn:aws:eks:us-east-2:111122223333:cluster/use2', user: u}}
        - {name: dc2, context: {cluster: 'arn:aws:eks:us-east-1:111122223333:cluster/use1', user: u}}
        - {name: broken, context: {user: u}}
        users:
        - {name: u, user: {token: t}}
    """))
    return str(p)


def _args(kubeconfig, **overrides):
    base = dict(cluster_name=None, region=None, kubeconfig=kubeconfig, output="out/eks-export-x.json")
    base.update(overrides)
    return types.SimpleNamespace(**base)


def test_output_path_for_context_suffix_and_placeholder(exporter_module):
    f = exporter_module.output_path_for_context
    assert f("out/eks-export-x.json", "fr5") == "out/eks-export-x-fr5.json"
    assert f("out/{context}/export.yaml", "sy1") == "out/sy1/export.yaml"
    assert f("noext", "ch1") == "noext-ch1"


def test_run_all_contexts_dedupes_by_cluster_and_prefers_short_name(exporter_module, kubeconfig, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(exporter_module, "run_export", lambda a, ctx, out, od: calls.append((ctx, out)))
    rc = exporter_module.run_all_contexts(_args(kubeconfig), "out")
    assert rc == 0
    assert calls == [("dc2", "out/eks-export-x-dc2.json"), ("ch1", "out/eks-export-x-ch1.json")]
    out = capsys.readouterr().out
    assert "Skipping context 'arn:aws:eks:us-east-1:111122223333:cluster/use1': same cluster as 'dc2'" in out
    assert "Skipping context 'broken'" in out


def test_run_all_contexts_continues_after_failure(exporter_module, kubeconfig, monkeypatch):
    def fake_run(a, ctx, out, od):
        if ctx == "dc2":
            raise exporter_module.ExportError("boom")
    monkeypatch.setattr(exporter_module, "run_export", fake_run)
    rc = exporter_module.run_all_contexts(_args(kubeconfig), "out")
    assert rc == 1


def test_run_all_contexts_rejects_explicit_name_or_region(exporter_module, kubeconfig, monkeypatch):
    monkeypatch.setattr(exporter_module, "run_export", lambda *a: pytest.fail("should not run"))
    assert exporter_module.run_all_contexts(_args(kubeconfig, region="us-east-1"), "out") == 1
    assert exporter_module.run_all_contexts(_args(kubeconfig, cluster_name="x"), "out") == 1


def test_list_contexts_marks_current(exporter_module, kubeconfig, capsys):
    assert exporter_module.list_contexts(kubeconfig) == 0
    lines = capsys.readouterr().out.splitlines()
    current = [l for l in lines if l.startswith("* ")]
    assert len(current) == 1 and "ch1" in current[0] and "us-east-2" in current[0]
    assert any("broken" in l and "error" in l for l in lines)


def test_output_path_for_context_sanitizes_arn_names(exporter_module):
    f = exporter_module.output_path_for_context
    arn = "arn:aws:eks:us-east-1:111122223333:cluster/prod"
    out = f("out/eks-export-all-TS.json", arn)
    assert out == "out/eks-export-all-TS-arn_aws_eks_us-east-1_111122223333_cluster_prod.json"
    assert "/" not in out[len("out/"):]
    assert f("out/{context}.json", arn).count("/") == 1


class _Ctx:
    def __init__(self, name, server):
        self.name, self.server = name, server


def _bare_exporter(exporter_module, ctx, cluster_name="prod", region="us-east-1", skip_aws=False):
    e = exporter_module.EKSConfigExporter.__new__(exporter_module.EKSConfigExporter)
    e.logger = exporter_module.logging.getLogger("t")
    e.kube_context_info = ctx
    e.cluster_name, e.region, e.skip_aws = cluster_name, region, skip_aws
    return e


def test_verify_kube_endpoint_matches(exporter_module):
    e = _bare_exporter(exporter_module, _Ctx("c", "https://ABC.gr7.us-east-1.eks.amazonaws.com"))
    e._verify_kube_endpoint("https://abc.gr7.us-east-1.eks.amazonaws.com")  # case-insensitive, no raise


def test_verify_kube_endpoint_other_eks_cluster_is_fatal(exporter_module):
    e = _bare_exporter(exporter_module, _Ctx("c", "https://AAA.gr7.us-east-1.eks.amazonaws.com"))
    with pytest.raises(exporter_module.ExportError, match="wrong cluster"):
        e._verify_kube_endpoint("https://BBB.gr7.us-east-1.eks.amazonaws.com")


def test_verify_kube_endpoint_tunnel_only_warns(exporter_module, caplog):
    e = _bare_exporter(exporter_module, _Ctx("tunnel", "https://127.0.0.1:6443"))
    with caplog.at_level("WARNING", logger="t"):
        e._verify_kube_endpoint("https://BBB.gr7.us-east-1.eks.amazonaws.com")
    assert "Cannot verify" in caplog.text


def test_verify_kube_endpoint_no_context_is_noop(exporter_module):
    e = _bare_exporter(exporter_module, None)
    e._verify_kube_endpoint("https://BBB.gr7.us-east-1.eks.amazonaws.com")


def test_skip_aws_allows_non_eks_kubeconfig_without_region(exporter_module, tmp_path, monkeypatch):
    kc = tmp_path / "plain.yaml"
    kc.write_text(textwrap.dedent("""
        apiVersion: v1
        kind: Config
        current-context: plain
        clusters: [{name: plain, cluster: {server: https://127.0.0.1:1}}]
        contexts: [{name: plain, context: {cluster: plain, user: u}}]
        users: [{name: u, user: {token: x}}]
    """))
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(exporter_module.boto3.session, "Session",
                        lambda *a, **k: types.SimpleNamespace(region_name=None))
    e = exporter_module.EKSConfigExporter("anything", kubeconfig=str(kc), skip_aws=True)
    assert e.region is None and e.aws_client is None
    e.export_cluster_info()
    assert e.export_data["cluster_info"] == {}
    with pytest.raises(ValueError, match="pass --region"):
        exporter_module.EKSConfigExporter("anything", kubeconfig=str(kc), skip_aws=False)
