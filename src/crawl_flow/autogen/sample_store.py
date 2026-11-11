"""自动爬取样本的版本化存储与容量治理。

目录布局::

    <root>/<site_key>/
        manifest.json                # 版本清单与站点元数据
        versions/<UTC时间戳>/
            list/page_1.html ...    # 列表页样本
            detail/<id>.html        # 详情页样本
            spec.json                # 当时生成/校验使用的适配器规格

治理策略（对应需求硬约束）：

- 每个站点默认只保留最近 ``DEFAULT_SAMPLE_VERSIONS``（3）个版本，更旧的整目录删除；
- 整个样本库超过 ``DEFAULT_SAMPLE_MAX_BYTES``（100MB）时，按最旧版本优先全局回收；
- 所有写入都是先写临时目录再原子替换，中途失败不留半成品。
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crawl_flow.autogen.constants import (
    DEFAULT_SAMPLE_MAX_BYTES,
    DEFAULT_SAMPLE_VERSIONS,
)
from crawl_flow.errors import StoreError

_SAFE_SITE = re.compile(r"[^A-Za-z0-9._-]+")


def site_key_for(url: str) -> str:
    """把入口地址转成安全的站点目录名。"""

    host = re.sub(r"^https?://", "", url).split("/", 1)[0]
    candidate = _SAFE_SITE.sub("_", host).strip("._")
    return candidate or "unknown-site"


@dataclass(frozen=True)
class SampleVersion:
    version_id: str
    path: Path
    created_at: str


class SampleStore:
    """版本化样本库。"""

    def __init__(
        self,
        root: str | Path,
        *,
        keep_versions: int = DEFAULT_SAMPLE_VERSIONS,
        max_bytes: int = DEFAULT_SAMPLE_MAX_BYTES,
    ) -> None:
        if keep_versions < 1:
            raise ValueError("keep_versions 至少为 1")
        self.root = Path(root)
        self.keep_versions = keep_versions
        self.max_bytes = max_bytes

    # ---------- 写入 ----------

    def create_version(
        self,
        entry_url: str,
        *,
        list_samples: dict[str, str],
        detail_samples: dict[str, str],
        spec: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SampleVersion:
        """落盘一个新版本并触发保留策略，返回版本描述。"""

        site_dir = self.root / site_key_for(entry_url)
        version_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        version_dir = site_dir / "versions" / version_id
        staging = site_dir / "versions" / f".{version_id}.tmp"
        try:
            (staging / "list").mkdir(parents=True, exist_ok=True)
            (staging / "detail").mkdir(parents=True, exist_ok=True)
            for name, body in list_samples.items():
                self._write(staging / "list" / name, body)
            for name, body in detail_samples.items():
                self._write(staging / "detail" / name, body)
            if spec is not None:
                self._write(
                    staging / "spec.json",
                    json.dumps(spec, ensure_ascii=False, indent=2),
                )
            staging.replace(version_dir)
        except OSError as exc:
            shutil.rmtree(staging, ignore_errors=True)
            raise StoreError(f"样本版本写入失败: {exc}") from exc

        manifest = self._load_manifest(site_dir)
        manifest["entry_url"] = entry_url
        manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
        manifest.setdefault("versions", [])
        manifest["versions"].append(
            {
                "version_id": version_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "detail_count": len(detail_samples),
                "list_count": len(list_samples),
            }
        )
        self._write_manifest(site_dir, manifest)

        self._apply_retention(site_dir)
        self._enforce_global_cap()
        return SampleVersion(
            version_id=version_id,
            path=version_dir,
            created_at=manifest["versions"][-1]["created_at"],
        )

    # ---------- 读取 ----------

    def list_versions(self, entry_url: str) -> list[SampleVersion]:
        site_dir = self.root / site_key_for(entry_url)
        if not site_dir.is_dir():
            return []
        versions_dir = site_dir / "versions"
        result = []
        for path in sorted(p for p in versions_dir.glob("*") if p.is_dir()):
            result.append(
                SampleVersion(
                    version_id=path.name,
                    path=path,
                    created_at=datetime.fromtimestamp(
                        path.stat().st_mtime, tz=timezone.utc
                    ).isoformat(),
                )
            )
        return result

    def latest_version(self, entry_url: str) -> SampleVersion | None:
        versions = self.list_versions(entry_url)
        return versions[-1] if versions else None

    def read_latest_spec(self, entry_url: str) -> dict[str, Any] | None:
        version = self.latest_version(entry_url)
        if version is None:
            return None
        spec_path = version.path / "spec.json"
        if not spec_path.is_file():
            return None
        return json.loads(spec_path.read_text(encoding="utf-8"))

    def total_bytes(self) -> int:
        if not self.root.is_dir():
            return 0
        return sum(f.stat().st_size for f in self.root.rglob("*") if f.is_file())

    # ---------- 保留策略 ----------

    def _apply_retention(self, site_dir: Path) -> None:
        manifest = self._load_manifest(site_dir)
        versions = manifest.get("versions", [])
        if len(versions) <= self.keep_versions:
            return
        stale = versions[: len(versions) - self.keep_versions]
        manifest["versions"] = versions[len(stale) :]
        for entry in stale:
            shutil.rmtree(site_dir / "versions" / entry["version_id"], ignore_errors=True)
        self._write_manifest(site_dir, manifest)

    def _enforce_global_cap(self) -> None:
        """总量超 100MB 时，全局按最旧版本优先回收，直到回到上限内。"""

        all_versions: list[tuple[float, Path]] = []
        for site_dir in self.root.glob("*"):
            versions_dir = site_dir / "versions"
            if not versions_dir.is_dir():
                continue
            for version_dir in (p for p in versions_dir.glob("*") if p.is_dir()):
                all_versions.append((version_dir.stat().st_mtime, version_dir))
        all_versions.sort(key=lambda item: item[0])

        for _, version_dir in all_versions:
            if self.total_bytes() <= self.max_bytes:
                break
            shutil.rmtree(version_dir, ignore_errors=True)
            self._rebuild_manifests()

    def _rebuild_manifests(self) -> None:
        for site_dir in self.root.glob("*"):
            if not site_dir.is_dir():
                continue
            manifest = self._load_manifest(site_dir)
            existing = {p.name for p in (site_dir / "versions").glob("*") if p.is_dir()}
            manifest["versions"] = [
                entry
                for entry in manifest.get("versions", [])
                if entry.get("version_id") in existing
            ]
            self._write_manifest(site_dir, manifest)

    # ---------- 辅助 ----------

    @staticmethod
    def _write(path: Path, body: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")

    def _load_manifest(self, site_dir: Path) -> dict[str, Any]:
        path = site_dir / "manifest.json"
        if not path.is_file():
            return {"versions": []}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {"versions": []}
        if not isinstance(payload, dict):
            return {"versions": []}
        payload.setdefault("versions", [])
        return payload

    def _write_manifest(self, site_dir: Path, manifest: dict[str, Any]) -> None:
        site_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = site_dir / "manifest.json"
        tmp = manifest_path.with_name("manifest.json.tmp")
        tmp.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(manifest_path)
