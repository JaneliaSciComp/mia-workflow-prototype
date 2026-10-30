"""The page only requests routes that exist and are open (read or consumer), holds no token, and calls no stray host."""

import re
from importlib import resources

from mia_buffers.api import ROUTES

PAGE = (resources.files("mia_buffers") / "ui" / "index.html").read_text()


def resolve(method: str, path: str) -> str | None:
    """The capability of the route that serves `method path`, or None when no route does."""
    path = path.split("?")[0]
    for route_method, pattern, _name, capability in ROUTES:
        if route_method == method and pattern.match(path):
            return capability
    return None


def requested_paths() -> set[tuple[str, str]]:
    """Every path the page passes to api()/load(), as (method, path) with `${…}` replaced by x."""
    found = set()
    for call in re.finditer(r"\b(?:api|load)\(\s*([\"'`])(/[^\"'`]*)\1", PAGE):
        path = re.sub(r"\$\{[^}]*\}", "x", call.group(2))
        options = re.match(r"\s*,\s*\{\s*method:\s*\"(\w+)\"", PAGE[call.end() :])
        method = options.group(1) if options else "GET"
        found.add((method, path))
    return found


def test_resolver_rejects_a_bogus_path() -> None:
    assert resolve("GET", "/no/such/route") is None
    assert resolve("GET", "/buffers") == "read"
    assert resolve("PUT", "/keeper") == "worker"


def test_every_path_the_page_requests_is_an_open_route() -> None:
    requested = requested_paths()
    assert {path for _, path in requested} >= {
        "/plan",
        "/meta",
        "/catalogs",
        "/datasets",
        "/buffers",
        "/keeper",
        "/locks",
    }
    assert ("POST", "/plan") in requested
    assert resolve("GET", "/crops/folder/crop/overlay.png") == "read"  # the thumbnails' <img src>, not an api() call
    for method, path in requested:
        assert resolve(method, path) in {"read", "consumer"}, (method, path)


def test_page_holds_no_token_and_calls_only_known_hosts() -> None:
    assert not re.search(r"authorization|bearer", PAGE, re.I)
    assert not re.search(r"https?://\$\{", PAGE), "a host built from a template could be any host"
    assert len(re.findall(r"\bfetch\(", PAGE)) == 1, "every request goes through api()"
    hosts = set(re.findall(r"https?://[^/\"'`$\s)]+/?", PAGE))
    allowed = {"https://github.com/", "https://fonts.googleapis.com/", "http://www.w3.org/"}
    assert {h if h.endswith("/") else h + "/" for h in hosts} <= allowed
