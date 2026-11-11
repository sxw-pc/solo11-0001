"""站点能力边界检测：登录、验证码、动态渲染（JS-only）三类不支持场景。

检测只基于静态 HTML 的结构性信号，不猜测、不尝试登录，命中即通过
:class:`UnsupportedSiteError` 明确告知用户「不支持」及具体原因，
保证不会为这类站点生成无效适配器。
"""

from __future__ import annotations

from bs4 import BeautifulSoup, Tag

from crawl_flow.autogen.constants import DYNAMIC_RENDER_ENABLED_BY_DEFAULT
from crawl_flow.autogen.errors import UnsupportedSiteError

# 登录表单信号
_PASSWORD_TYPES = {"password"}
_LOGIN_HINTS = ("login", "signin", "sign-in", "logon", "auth", "登录", "登陆")
# 验证码信号
_CAPTCHA_HINTS = ("captcha", "recaptcha", "验证", "校验码")
# 前端框架挂载点信号
_APP_MOUNT_IDS = {"app", "root", "__next", "__nuxt"}
# 列表条目的最小重复次数：少于它说明静态 HTML 里没有可识别的列表结构
_MIN_REPEATED_ITEMS = 2


def detect_login(soup: BeautifulSoup) -> str | None:
    """命中登录墙时返回原因，否则返回 ``None``。"""

    for input_el in soup.find_all("input"):
        if input_el.get("type", "").lower() in _PASSWORD_TYPES:
            return "站点包含密码输入框，疑似需要登录才能访问"
    for form in soup.find_all("form"):
        action = str(form.get("action", "")).lower()
        form_id = str(form.get("id", "")).lower()
        if any(hint in action or hint in form_id for hint in _LOGIN_HINTS):
            return "站点表单指向登录入口，疑似需要登录才能访问"
    text = soup.get_text(" ", strip=True)[:2000]
    if "请先登录" in text or "登录后" in text:
        return "页面文案要求先登录，疑似需要登录才能访问"
    return None


def detect_captcha(soup: BeautifulSoup) -> str | None:
    """命中验证码时返回原因，否则返回 ``None``。"""

    for input_el in soup.find_all("input"):
        name = str(input_el.get("name", "")).lower()
        placeholder = str(input_el.get("placeholder", "")).lower()
        if any(hint in name or hint in placeholder for hint in _CAPTCHA_HINTS):
            return "站点表单包含验证码输入项，自动采集不支持验证码校验"
    for img in soup.find_all("img"):
        alt = str(img.get("alt", "")).lower()
        src = str(img.get("src", "")).lower()
        if "captcha" in alt or "captcha" in src:
            return "站点包含验证码图片，自动采集不支持验证码校验"
    if "g-recaptcha" in str(soup) or "hcaptcha" in str(soup).lower():
        return "站点接入了人机验证服务，自动采集不支持验证码校验"
    return None


def count_static_anchor_items(soup: BeautifulSoup) -> int:
    """统计静态 HTML 中带内链的锚点数量，用于判断内容是否由 JS 注入。"""

    return len(
        [
            a
            for a in soup.find_all("a", href=True)
            if isinstance(a, Tag)
            and not str(a["href"]).startswith(("#", "javascript:", "mailto:", "tel:"))
        ]
    )


def detect_javascript_only(soup: BeautifulSoup) -> bool:
    """判断页面是否为 JS-only：静态骨架几乎无内容链接，但存在明显前端挂载点/脚本。"""

    anchor_count = count_static_anchor_items(soup)
    if anchor_count >= _MIN_REPEATED_ITEMS:
        return False

    mount = soup.find(id=lambda value: value and value.lower() in _APP_MOUNT_IDS)
    has_inline_script = any(
        len(script.get_text(strip=True)) > 80 for script in soup.find_all("script")
    )
    body_text = soup.get_text(strip=True)
    return mount is not None and (has_inline_script or len(body_text) < 50)


def ensure_site_supported(
    soup: BeautifulSoup,
    *,
    enable_dynamic_rendering: bool = DYNAMIC_RENDER_ENABLED_BY_DEFAULT,
) -> None:
    """入口页能力边界总检；不支持时抛 :class:`UnsupportedSiteError`。"""

    login_reason = detect_login(soup)
    if login_reason:
        raise UnsupportedSiteError(login_reason, reason_code="login_required")

    captcha_reason = detect_captcha(soup)
    if captcha_reason:
        raise UnsupportedSiteError(captcha_reason, reason_code="captcha_required")

    if detect_javascript_only(soup):
        if not enable_dynamic_rendering:
            raise UnsupportedSiteError(
                "站点内容由 JavaScript 动态渲染，动态渲染能力默认关闭；"
                "如确需采集，请显式开启 enable_dynamic_rendering（需运行环境安装浏览器渲染依赖）",
                reason_code="dynamic_rendering_disabled",
            )
