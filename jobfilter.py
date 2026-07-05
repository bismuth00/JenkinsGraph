"""config の [jobs] セクションに基づくジョブ名フィルタ。collect.py / report.py 共通。

include / exclude とも正規表現のリストで、ジョブの fullName
(フォルダを含む名前、例: "app/build-main") に対する部分一致 (re.search)。
完全一致にしたい場合は ^$ でアンカーする。

- include が空 (または未指定) なら全ジョブが対象
- exclude は include より優先される
"""

import re
import sys


def _compile(patterns, label):
    try:
        return [re.compile(p) for p in patterns]
    except re.error as e:
        sys.exit(f"config の jobs.{label} の正規表現が不正です: {e.pattern!r} ({e})")


def compile_filter(cfg):
    """config 辞書からフィルタ関数 (ジョブ名 -> bool) を作る。"""
    jobs_cfg = cfg.get("jobs", {})
    include = _compile(jobs_cfg.get("include", []), "include")
    exclude = _compile(jobs_cfg.get("exclude", []), "exclude")

    def match(name):
        if include and not any(p.search(name) for p in include):
            return False
        return not any(p.search(name) for p in exclude)

    return match
