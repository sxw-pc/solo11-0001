"""样本库：最近 3 版本保留、100MB 总量上限自动清理、manifest 一致性。"""

from __future__ import annotations

from crawl_flow.autogen.sample_store import SampleStore, site_key_for

ENTRY = "http://blog.example.invalid/posts"


def _version(store: SampleStore, *, body_size: int = 100) -> str:
    body = "<html>" + ("x" * body_size) + "</html>"
    version = store.create_version(
        ENTRY,
        list_samples={"page_1.html": body},
        detail_samples={"d.html": body},
        spec={"engine": "generic_html"},
    )
    return version.version_id


def test_site_key_safe() -> None:
    assert site_key_for("https://blog.example.com/posts?x=1") == "blog.example.com"
    assert site_key_for("http://127.0.0.1:8020/") == "127.0.0.1_8020"


def test_keeps_only_three_latest_versions(tmp_path) -> None:
    store = SampleStore(tmp_path / "samples", keep_versions=3)
    ids = [_version(store) for _ in range(5)]
    remaining = [v.version_id for v in store.list_versions(ENTRY)]
    assert remaining == ids[-3:]


def test_versions_are_ordered(tmp_path) -> None:
    store = SampleStore(tmp_path / "samples")
    first = _version(store)
    second = _version(store)
    versions = store.list_versions(ENTRY)
    assert [v.version_id for v in versions] == [first, second]
    assert store.latest_version(ENTRY).version_id == second


def test_latest_spec_roundtrip(tmp_path) -> None:
    store = SampleStore(tmp_path / "samples")
    _version(store)
    spec = store.read_latest_spec(ENTRY)
    assert spec == {"engine": "generic_html"}


def test_global_cap_evicts_oldest_first(tmp_path) -> None:
    # 每个版本约 400 字节（list + detail + spec），上限 1KB 必然触发全局回收
    store = SampleStore(tmp_path / "samples", keep_versions=10, max_bytes=1024)
    ids = [_version(store, body_size=200) for _ in range(8)]
    assert store.total_bytes() <= 1024
    remaining = {v.version_id for v in store.list_versions(ENTRY)}
    # 最旧的版本一定被回收，最新版本必须保留
    assert ids[0] not in remaining
    assert ids[-1] in remaining


def test_default_constants_match_requirements() -> None:
    from crawl_flow.autogen.constants import (
        DEFAULT_SAMPLE_MAX_BYTES,
        DEFAULT_SAMPLE_VERSIONS,
    )

    assert DEFAULT_SAMPLE_VERSIONS == 3
    assert DEFAULT_SAMPLE_MAX_BYTES == 100 * 1024 * 1024
