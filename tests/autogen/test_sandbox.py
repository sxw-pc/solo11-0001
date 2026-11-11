"""沙箱：恶意样本拦截、规格静态审计、无动态执行原语。"""

from __future__ import annotations

from pathlib import Path

import pytest

from crawl_flow.autogen.errors import AdapterSpecError, SandboxViolation
from crawl_flow.autogen.sandbox import (
    assert_no_execution_primitives,
    assert_sample_safe,
    audit_spec,
    scan_html,
)
from crawl_flow.autogen.spec import AdapterSpec

MALICIOUS_HTML = """
<html><body>
  <h1 class="entry-title">恶意文章</h1>
  <div class="entry-content"><p>正常正文</p>
    <img src="x" onerror="alert(1)">
    <a href="javascript:alert(2)">click</a>
  </div>
  <script>window.evil = 1</script>
  <iframe src="http://evil.example"></iframe>
</body></html>
"""

CLEAN_HTML = """
<html><body>
  <article class="entry">
    <h1 class="entry-title">正常文章</h1>
    <div class="entry-content"><p>公开正文</p></div>
  </article>
</body></html>
"""


def _blog_spec(**overrides: object) -> AdapterSpec:
    payload: dict[str, object] = {
        "base_url": "http://127.0.0.1:8020",
        "list": {
            "list_path": "/posts",
            "item_selector": "section.post-list > article.post-card",
            "link_selector": "a.post-link",
            "link_attr": "href",
            "id_pattern": r"/posts/([^/?#]+)(?:[?#]|$)",
            "pagination": "next_link",
            "next_selector": "nav.pager > a[rel='next']",
        },
        "detail": {
            "detail_path": "/posts/{id}",
            "scope_selector": "article.entry",
            "fields": {
                "title": {"selector": "h1.entry-title", "mode": "text"},
                "content": {"selector": "div.entry-content", "mode": "html"},
            },
        },
    }
    payload.update(overrides)
    return AdapterSpec.from_dict(payload)


def test_scan_detects_all_threat_categories() -> None:
    threats = scan_html(MALICIOUS_HTML, source="evil")
    joined = "\n".join(threats)
    assert "<script>" in joined
    assert "onerror" in joined
    assert "危险协议" in joined  # javascript: 链接
    assert "iframe" in joined


def test_clean_sample_passes() -> None:
    assert scan_html(CLEAN_HTML) == []
    assert_sample_safe(CLEAN_HTML)  # 不抛异常即通过


def test_assert_sample_safe_raises_sandbox_violation() -> None:
    with pytest.raises(SandboxViolation) as excinfo:
        assert_sample_safe(MALICIOUS_HTML, source="evil-1")
    assert excinfo.value.threats  # 携带具体威胁清单


def test_audit_spec_accepts_declarative_selectors() -> None:
    audit_spec(_blog_spec())  # 不抛异常


def test_audit_spec_rejects_script_like_selectors() -> None:
    bad = _blog_spec()
    bad.detail.fields["title"].selector = "h1[onclick='expression(alert(1))']"
    with pytest.raises(AdapterSpecError):
        audit_spec(bad)


def test_audit_spec_rejects_unknown_engine() -> None:
    spec = _blog_spec()
    object.__setattr__(spec, "engine", "python_eval")  # type: ignore[attr-defined]
    with pytest.raises(AdapterSpecError):
        audit_spec(spec)


def test_runtime_adapter_has_no_execution_primitives() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "crawl_flow"
        / "sources"
        / "generic_html.py"
    ).read_text(encoding="utf-8")
    assert_no_execution_primitives(source)  # 不抛异常
