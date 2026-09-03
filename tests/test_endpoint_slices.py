"""Unit test for the endpoint_slices raw-response parsing (no live cluster needed)."""
import json


def _exporter(exporter_module, discovery_v1):
    e = exporter_module.EKSConfigExporter.__new__(exporter_module.EKSConfigExporter)
    e.logger = exporter_module.logging.getLogger("t")
    e.k8s_client = {'discovery_v1': discovery_v1}
    e.export_data = {'resources': {}}
    return e


class _RawResponse:
    def __init__(self, payload):
        self.data = json.dumps(payload).encode()


class _FakeDiscoveryV1:
    """Mimics the real client: the typed call raises on endpoints=None; the
    _preload_content=False call returns the raw (unvalidated) JSON."""

    def __init__(self, items):
        self._items = items

    def list_endpoint_slice_for_all_namespaces(self, **kwargs):
        if kwargs.get('_preload_content') is False:
            return _RawResponse({'items': self._items})
        if any(i.get('endpoints') is None for i in self._items):
            raise ValueError("Invalid value for `endpoints`, must not be `None`")
        return {'items': self._items}


def test_export_endpoint_slices_handles_null_endpoints(exporter_module):
    items = [
        {
            'metadata': {'name': 'good', 'namespace': 'ns1', 'labels': {'a': 'b'},
                         'annotations': None, 'creationTimestamp': '2026-01-01T00:00:00Z'},
            'addressType': 'IPv4',
            'endpoints': [{'addresses': ['10.0.0.1']}, {'addresses': ['10.0.0.2']}],
            'ports': [{'name': 'http', 'port': 80, 'protocol': 'TCP'}],
        },
        {
            'metadata': {'name': 'headless-null', 'namespace': 'ns2', 'labels': None,
                         'annotations': None, 'creationTimestamp': None},
            'addressType': 'IPv4',
            'endpoints': None,   # the real-world case that broke the typed client call
            'ports': None,
        },
    ]
    e = _exporter(exporter_module, _FakeDiscoveryV1(items))
    e.export_endpoint_slices()

    result = e.export_data['resources']['endpoint_slices']
    assert len(result) == 2
    assert result[0] == {
        'name': 'good', 'namespace': 'ns1', 'labels': {'a': 'b'}, 'annotations': {},
        'address_type': 'IPv4', 'endpoints': 2,
        'ports': [{'name': 'http', 'port': 80, 'protocol': 'TCP'}],
        'creation_timestamp': '2026-01-01T00:00:00Z',
    }
    assert result[1]['name'] == 'headless-null'
    assert result[1]['endpoints'] == 0    # null -> empty, not a crash
    assert result[1]['ports'] == []
    assert result[1]['labels'] == {}


def test_export_endpoint_slices_no_items(exporter_module):
    e = _exporter(exporter_module, _FakeDiscoveryV1([]))
    e.export_endpoint_slices()
    assert e.export_data['resources']['endpoint_slices'] == []


def test_export_endpoint_slices_swallows_errors(exporter_module):
    class _Broken:
        def list_endpoint_slice_for_all_namespaces(self, **kwargs):
            raise RuntimeError("connection reset")
    e = _exporter(exporter_module, _Broken())
    e.export_endpoint_slices()  # must not raise
    assert 'endpoint_slices' not in e.export_data['resources'] or e.export_data['resources'].get('endpoint_slices') in (None, [])
