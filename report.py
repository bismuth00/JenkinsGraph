"""SQLite に蓄積したビルド履歴から静的 HTML レポートを生成する。

タイムライン (ジョブごとの成功/失敗の帯 + 結果のパーセント表示) と、
日別×ジョブ別の失敗率ヒートマップを含む report.html を出力する。

集計はレポート閲覧時にブラウザ側で行うため、ここでは対象期間内の
ビルドデータをそのまま埋め込む。表示期間はレポート上のボタンで
config の report.days を上限に切り替えられる。
"""

import argparse
import base64
import gzip
import json
import re
import sqlite3
import tomllib
from html import escape as html_escape
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from jobfilter import compile_filter

# 「失敗」として数えるビルド結果 (テンプレート側の集計にもこの値が渡る)
FAIL_RESULTS = ["FAILURE", "UNSTABLE"]


def load_builds(db_path):
    conn = sqlite3.connect(db_path)
    # queuing カラムは後から追加されたもの。collect.py 未実行の古い DB では 0 扱い
    cols = {r[1] for r in conn.execute("PRAGMA table_info(builds)")}
    queuing = "queuing" if "queuing" in cols else "0"
    node = "node" if "node" in cols else "NULL"
    up_job = "upstream_job" if "upstream_job" in cols else "NULL"
    up_num = "upstream_build" if "upstream_build" in cols else "NULL"
    return conn.execute(
        f"SELECT job_name, result, timestamp, duration, {queuing}, number, {node},"
        f" {up_job}, {up_num}"
        " FROM builds ORDER BY job_name, timestamp"
    ).fetchall()


def load_jobs_meta(db_path):
    """jobs テーブルから (無効ジョブ名の集合, ジョブ名 -> URL,
    ジョブ名 -> 並列実行可否, ジョブ名 -> ラベル式) を返す。
    並列実行可否は 1/0/None (不明)、ラベル式は文字列/None (制限なし/不明)。

    テーブルやカラムがない古い DB では空を返す (URL は呼び出し側で組み立てる)。
    """
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "jobs" not in tables:
        return set(), {}, {}, {}
    cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
    url_col = "url" if "url" in cols else "''"
    concurrent_col = "concurrent" if "concurrent" in cols else "NULL"
    label_col = "label" if "label" in cols else "NULL"
    disabled = set()
    urls = {}
    concurrent = {}
    labels = {}
    for name, buildable, url, conc, label in conn.execute(
        f"SELECT job_name, buildable, {url_col}, {concurrent_col}, {label_col} FROM jobs"
    ):
        if not buildable:
            disabled.add(name)
        if url:
            urls[name] = url
        concurrent[name] = conc
        labels[name] = label
    return disabled, urls, concurrent, labels


def load_nodes(db_path):
    """(ノード名 -> エグゼキュータ数, node_status サンプル,
    ノード名 -> ラベル文字列) を返す。古い DB では空。"""
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    executors = {}
    node_labels = {}
    if "nodes" in tables:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(nodes)")}
        labels_col = "labels" if "labels" in cols else "NULL"
        for name, execs, labels in conn.execute(
            f"SELECT node_name, executors, {labels_col} FROM nodes"
        ):
            executors[name] = execs
            node_labels[name] = labels
    samples = []
    if "node_status" in tables:
        # temp_offline / offline_reason は後から追加されたカラム。古い DB では NULL 扱い
        cols = {r[1] for r in conn.execute("PRAGMA table_info(node_status)")}
        temp = "temp_offline" if "temp_offline" in cols else "NULL"
        reason = "offline_reason" if "offline_reason" in cols else "NULL"
        samples = conn.execute(
            f"SELECT sampled_at, node_name, offline, {temp}, {reason} FROM node_status"
            " ORDER BY node_name, sampled_at"
        ).fetchall()
    return executors, samples, node_labels


def offline_intervals(samples, node_index, window_start_ms, now_ms, current):
    """node_status のサンプル列からノードごとのオフライン区間
    [開始ms, 終了ms, 種別, 理由] を作る (種別 0 + 理由なしは末尾を省略)。

    種別: 0 = 不明 (旧 DB のサンプル)、1 = 手動 (temporarilyOffline)、
    2 = 接続断など (自発的でないオフライン)。
    オフラインのサンプルが続く間を 1 区間にまとめ、種別や理由が変わったら
    区間を分割する。区間の終わりは次にオンラインが観測された時刻。
    最後までオフラインの場合、現存ノードは現在時刻まで、削除済みノード
    (current に含まれない) は最後に観測された時刻までとする。
    粒度は収集間隔に依存する。
    """
    intervals = [[] for _ in node_index]

    def emit(name, start, end, kind, reason):
        if end <= start:
            return
        iv = [start, end, kind, reason]
        if not reason:
            iv.pop()
            if not kind:
                iv.pop()
        intervals[node_index[name]].append(iv)

    cur = {}  # name -> [開始ms, 種別, 理由]
    last_seen = {}
    for t, name, offline, temp, reason in samples:  # ノード名, 時刻順
        if name not in node_index:
            continue
        last_seen[name] = t
        if offline:
            kind = 0 if temp is None else (1 if temp else 2)
            reason = reason or ""
            c = cur.get(name)
            if c is None:
                cur[name] = [t, kind, reason]
            elif c[1] != kind or c[2] != reason:
                emit(name, c[0], t, c[1], c[2])
                cur[name] = [t, kind, reason]
        elif name in cur:
            c = cur.pop(name)
            emit(name, c[0], t, c[1], c[2])
    for name, c in cur.items():
        end = now_ms if name in current else last_seen[name]
        emit(name, c[0], end, c[1], c[2])
    for lst in intervals:
        lst[:] = [iv for iv in lst if iv[1] > window_start_ms]
    return intervals


def eval_label_expr(expr, labels):
    """Jenkins のラベル式をノードのラベル集合に対して評価する。

    対応する構文はラベル名・!・&&・||・括弧。それ以外 (->, <->, 引用符
    など、実運用ではまれ) が含まれる式は None (判定不能) を返す。
    """
    tokens = re.findall(r"\(|\)|&&|\|\||!|[^\s()!&|]+", expr)
    if "".join(tokens) != "".join(expr.split()):
        return None  # トークン化で落ちた文字がある = 対応外の構文

    pos = 0

    def parse_or():
        nonlocal pos
        v = parse_and()
        while v is not None and pos < len(tokens) and tokens[pos] == "||":
            pos += 1
            r = parse_and()
            v = None if r is None else (v or r)
        return v

    def parse_and():
        nonlocal pos
        v = parse_atom()
        while v is not None and pos < len(tokens) and tokens[pos] == "&&":
            pos += 1
            r = parse_atom()
            v = None if r is None else (v and r)
        return v

    def parse_atom():
        nonlocal pos
        if pos >= len(tokens):
            return None
        t = tokens[pos]
        if t == "!":
            pos += 1
            v = parse_atom()
            return None if v is None else (not v)
        if t == "(":
            pos += 1
            v = parse_or()
            if v is None or pos >= len(tokens) or tokens[pos] != ")":
                return None
            pos += 1
            return v
        if t in ("&&", "||", ")"):
            return None
        pos += 1
        return t in labels

    v = parse_or()
    return v if pos == len(tokens) else None


def job_url(base_url, name, urls):
    """ジョブページの URL。jobs テーブルにあればそれを、なければ fullName から組み立てる。"""
    if name in urls:
        return urls[name]
    path = "/job/".join(quote(seg, safe="") for seg in name.split("/"))
    return f"{base_url.rstrip('/')}/job/{path}/"


def select_jobs(rows, window_start_ms):
    """表示対象のジョブ: 期間内にビルドがあるか、最新の結果が失敗のままのジョブ。"""
    last_result = {}
    in_window = set()
    for job, result, ts, *_ in rows:
        last_result[job] = result
        if ts >= window_start_ms:
            in_window.add(job)
    return sorted(j for j in last_result if j in in_window or last_result[j] in FAIL_RESULTS)


def encode_builds(rows, jobs, node_index, window_start_ms):
    """ジョブごとのビルド配列 (サイズ削減のための差分エンコード形式) と、
    コード→結果名の対応表を作る。

    1 ビルド =
    [ts差分(秒), 結果コード, duration(秒), queuing(秒), number差分, ノードidx,
     欠落フラグ (, 上流ジョブidx, 上流ビルド番号)]

    - ts / number は同じジョブの直前のビルドとの差分 (先頭は絶対値)。
      テンプレート側で読み込み時に絶対値 (ミリ秒) へ展開する
    - 上流がない場合、末尾の 欠落フラグ=0 とノードidx=-1 は省略される
    - ノードidx -1 = ノード不明 (Pipeline など builtOn が取れないビルド)
    - 欠落フラグ = 次に保存されているビルドと番号が連続していない印。
      Jenkins はログローテーション後も lastFailedBuild などを残すため、
      間のビルドが取得できていない期間がありうる。その区間は
      テンプレート側で「状態不明」として扱う
    - 上流 = このビルドを起動したビルド (UpstreamCause)。上流ジョブが
      レポート対象に含まれる場合のみ付与する

    期間開始時点でどの状態だったか分かるよう、期間より前のビルドも
    直近の 1 件だけ含める。
    """
    job_index = {j: i for i, j in enumerate(jobs)}
    result_codes = {}
    by_job = defaultdict(list)
    for job, result, ts, dur, queuing, number, node, up_job, up_num in rows:
        if job not in job_index:
            continue
        code = result_codes.setdefault(result, len(result_codes))
        nidx = node_index.get(node, -1) if node is not None else -1
        by_job[job].append((ts, code, dur, queuing, number, nidx, up_job, up_num))

    builds = []
    for job in jobs:
        items = by_job[job]
        first_in = next(
            (i for i, b in enumerate(items) if b[0] >= window_start_ms), len(items)
        )
        items = items[max(first_in - 1, 0):]
        encoded = []
        prev_ts = prev_num = 0
        for i, (ts, code, dur, queuing, number, nidx, up_job, up_num) in enumerate(items):
            gap = i + 1 < len(items) and items[i + 1][4] != number + 1
            ts_s = round(ts / 1000)
            row = [ts_s - prev_ts, code, round(dur / 1000), round(queuing / 1000),
                   number - prev_num, nidx, 1 if gap else 0]
            prev_ts, prev_num = ts_s, number
            if up_job in job_index and up_num:
                row += [job_index[up_job], up_num]
            elif not gap:
                row.pop()
                if row[5] == -1:
                    row.pop()
            encoded.append(row)
        builds.append(encoded)

    results = [r for r, _ in sorted(result_codes.items(), key=lambda kv: kv[1])]
    return builds, results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()
    with open(args.config, "rb") as f:
        cfg = tomllib.load(f)

    days = cfg["report"].get("days", 60)
    now = datetime.now()
    now_ms = int(now.timestamp() * 1000)
    window_start_date = now.date() - timedelta(days=days - 1)
    window_start_ms = int(
        datetime.combine(window_start_date, datetime.min.time()).timestamp() * 1000
    )

    # [jobs] は収集対象 (DB に古いジョブが残っていても揃うようレポートでも適用)。
    # 表示対象はビューごとに別に絞れる:
    #   [report.timeline] — 概要タブの全チャートと詳細タイムライン
    #   [report.node]     — ノードタブのビルド
    #   [report.pipeline] — パイプラインタブの起点ビルド
    # [report.charts] は旧設定で、node / pipeline が未指定のときのフォールバック
    job_filter = compile_filter(cfg)
    rep = cfg.get("report", {})
    charts = rep.get("charts", {})
    tl_filter = compile_filter(cfg, rep.get("timeline", {}), "report.timeline")
    node_filter = compile_filter(cfg, rep.get("node", charts), "report.node")
    pipe_filter = compile_filter(cfg, rep.get("pipeline", charts), "report.pipeline")

    rows = [r for r in load_builds(cfg["db"]["path"]) if job_filter(r[0])]
    jobs = [j for j in select_jobs(rows, window_start_ms)
            if tl_filter(j) or node_filter(j) or pipe_filter(j)]

    # ノード一覧: nodes テーブルとビルドの実行ノードの和集合 ('' はビルトインノード)
    executors, node_samples, node_labels = load_nodes(cfg["db"]["path"])
    node_names = sorted({r[6] for r in rows if r[6] is not None} | set(executors))
    node_index = {n: i for i, n in enumerate(node_names)}

    # 最新のサンプリング時点に存在するノード = 現存。それ以外は削除済みとみなす
    # (node_status が空の古い DB では判定できないので全ノードを現存扱い)
    if node_samples:
        latest = max(s[0] for s in node_samples)
        current_nodes = {s[1] for s in node_samples if s[0] == latest}
    else:
        current_nodes = set(node_names)

    builds, results = encode_builds(rows, jobs, node_index, window_start_ms)
    disabled, urls, concurrent, job_labels = load_jobs_meta(cfg["db"]["path"])

    # ジョブのラベル式を現存ノードのラベルに対して評価し、
    # 「ビルド可能なノード数」を求める。ラベル式のないジョブ
    # (制限なし、または Pipeline などで不明) は None = 表示しない。
    # ノード自身の名前もラベルとして扱う (Jenkins のセルフラベル)
    node_label_sets = {
        n: set((node_labels.get(n) or "").split()) | ({n} if n else set())
        for n in node_names
    }

    def eligible_nodes(expr):
        count = 0
        for n in node_names:
            if n not in current_nodes:
                continue
            v = eval_label_expr(expr, node_label_sets[n])
            if v is None:
                return None  # 対応外の構文は判定不能
            if v:
                count += 1
        return count

    # ノードタブの「対象ジョブのみ実行 / 未実行」ノード数の対象パターン
    free_patterns = list(cfg["report"].get("node_free_jobs", []))
    free_filter = (
        compile_filter(cfg, {"include": free_patterns}, "report.node_free_jobs")
        if free_patterns else None
    )

    data = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M"),
        "days": days,
        "now": now_ms,
        "jobs": jobs,
        "builds": builds,
        "results": results,
        "fail_results": FAIL_RESULTS,
        "disabled": [1 if j in disabled else 0 for j in jobs],
        "concurrent": [1 if concurrent.get(j) else 0 for j in jobs],
        "label_nodes": [
            eligible_nodes(job_labels[j]) if job_labels.get(j) else None for j in jobs
        ],
        "job_labels": [job_labels.get(j) for j in jobs],
        "job_urls": [job_url(cfg["jenkins"]["url"], j, urls) for j in jobs],
        "jenkins_url": cfg["jenkins"]["url"].rstrip("/"),
        "trend_max": int(cfg["report"].get("trend_max_jobs", 50)),
        "box_max": int(cfg["report"].get("box_max_jobs", 20)),
        "scroll_rows": int(cfg["report"].get("scroll_rows", 200)),
        "tl_jobs": [1 if tl_filter(j) else 0 for j in jobs],
        "node_jobs": [1 if node_filter(j) else 0 for j in jobs],
        "pipe_jobs": [1 if pipe_filter(j) else 0 for j in jobs],
        "node_free_jobs": free_patterns,
        "node_free": [1 if free_filter(j) else 0 for j in jobs] if free_patterns else None,
        "nodes": [n if n else "(built-in)" for n in node_names],
        "node_executors": [executors.get(n, 1) for n in node_names],
        "node_deleted": [0 if n in current_nodes else 1 for n in node_names],
        "node_offline": offline_intervals(
            node_samples, node_index, window_start_ms, now_ms, current_nodes),
    }

    # サイズ削減のため、JSON を gzip + base64 で埋め込む
    # (テンプレート側で DecompressionStream を使って展開する)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    packed = base64.b64encode(gzip.compress(payload.encode("utf-8"), 9)).decode("ascii")

    title = str(cfg["report"].get("title", "Jenkins 健全性レポート"))
    template = Path(__file__).with_name("template.html").read_text(encoding="utf-8")
    html = (template
            .replace("__TITLE__", html_escape(title))
            .replace("__DATA__", json.dumps(packed)))
    out = Path(cfg["report"].get("output", "report.html"))
    out.write_text(html, encoding="utf-8")
    print(f"{out} を生成しました (ジョブ {len(jobs)} 件, 最大期間 {days} 日,"
          f" {out.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
