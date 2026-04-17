"""Minimal block-structured parser for Nextflow config files."""

from __future__ import annotations

import re
from typing import Any


class NextflowConfigParser:
    """Parse named blocks from a Nextflow config file, including nested blocks.

    Returns a nested dict: {block_name: {key: value_or_nested_dict}}.
    Leaf values are strings from ``key = 'value'`` assignments.
    Nested blocks (e.g. ``withName: 'X' {}``) are represented as inner dicts.
    """

    _BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
    # withName: 'ALIGN' { or withLabel: 'heavy' { (single or double quoted)
    _NAMED_BLOCK = re.compile(r"""^(with\w+)\s*:\s*(['"])([^'"]+)\2\s*\{""")
    _SIMPLE_BLOCK = re.compile(r"^(\w+)\s*\{")
    _KV = re.compile(r"""^\s*(\w+)\s*=\s*(['"])([^'"]*)\2""")
    # top-level dotted assignment: docker.registry = 'quay.io'
    _DOTTED_KV = re.compile(r"""^\s*(\w+)\.(\w+)\s*=\s*(['"])([^'"]*)\3""")

    def parse(self, text: str) -> dict[str, Any]:
        """Return a nested dict of all blocks and key-value pairs."""
        text = self._BLOCK_COMMENT.sub("", text)
        root: dict[str, Any] = {}
        stack: list[dict[str, Any]] = [root]

        for raw_line in text.splitlines():
            line = self._strip_line_comment(raw_line)
            stripped = line.strip()
            opens = stripped.count("{")
            closes = stripped.count("}")
            current = stack[-1]

            if opens > closes:
                m = self._NAMED_BLOCK.match(stripped)
                if m:
                    key: str | None = f"{m.group(1)}:{m.group(3)}"
                else:
                    m2 = self._SIMPLE_BLOCK.match(stripped)
                    key = m2.group(1) if m2 else None

                new_block: dict[str, Any] = {}
                if key is not None:
                    if key not in current or not isinstance(current[key], dict):
                        current[key] = new_block
                    else:
                        new_block = current[key]
                stack.append(new_block)
                for _ in range(opens - closes - 1):
                    anon: dict[str, Any] = {}
                    stack.append(anon)

            elif closes > opens:
                kv = self._KV.match(line)
                if kv and len(stack) > 1:
                    current[kv.group(1)] = kv.group(3)
                for _ in range(closes - opens):
                    if len(stack) > 1:
                        stack.pop()

            else:
                if len(stack) > 1:
                    kv = self._KV.match(line)
                    if kv:
                        current[kv.group(1)] = kv.group(3)
                else:
                    # root level: handle dotted assignments e.g. docker.registry = 'quay.io'
                    dkv = self._DOTTED_KV.match(line)
                    if dkv:
                        block_key = dkv.group(1)
                        if block_key not in root or not isinstance(root[block_key], dict):
                            root[block_key] = {}
                        root[block_key][dkv.group(2)] = dkv.group(4)

        return root

    @staticmethod
    def get_all(blocks: dict[str, Any], key: str) -> list[str]:
        """Recursively collect all string values assigned to *key* at any depth."""
        results: list[str] = []

        def walk(d: dict[str, Any]) -> None:
            for k, v in d.items():
                if k == key and isinstance(v, str):
                    results.append(v)
                elif isinstance(v, dict):
                    walk(v)

        walk(blocks)
        return results

    @staticmethod
    def _strip_line_comment(line: str) -> str:
        # Remove // comments but preserve :// in URLs inside strings.
        in_string: str | None = None
        i = 0
        while i < len(line):
            ch = line[i]
            if in_string:
                if ch == "\\" and in_string != "'":
                    i += 2
                    continue
                if ch == in_string:
                    in_string = None
            else:
                if ch in ('"', "'"):
                    in_string = ch
                elif ch == "/" and i + 1 < len(line) and line[i + 1] == "/":
                    return line[:i]
            i += 1
        return line
