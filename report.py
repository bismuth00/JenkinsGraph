"""SQLite に蓄積したビルド履歴から静的 HTML レポートを生成する。

タイムライン (ジョブごとの成功/失敗の帯 + 結果のパーセント表示) と、
日別×ジョブ別の失敗率ヒートマップを含む report.html を出力する。

集計はレポート閲覧時にブラウザ側で行うため、ここでは対象期間内の
ビルドデータをそのまま埋め込む。表示期間はレポート上のボタンで
config の report.days を上限に切り替えられる。
"""

import argparse
import json
import sqlite3
import tomllib
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
    return conn.execute(
        f"SELECT job_name, result, timestamp, duration, {queuing}, number, {node}"
        " FROM builds ORDER BY job_name, timestamp"
    ).fetchall()


def load_jobs_meta(db_path):
    """jobs テーブルから (無効ジョブ名の集合, ジョブ名 -> URL) を返す。

    テーブルやカラムがない古い DB では空を返す (URL は呼び出し側で組み立てる)。
    """
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "jobs" not in tables:
        return set(), {}
    cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
    url_col = "url" if "url" in cols else "''"
    disabled = set()
    urls = {}
    for name, buildable, url in conn.execute(
        f"SELECT job_name, buildable, {url_col} FROM jobs"
    ):
        if not buildable:
            disabled.add(name)
        if url:
            urls[name] = url
    return disabled, urls


def load_nodes(db_path):
    """(ノード名 -> エグゼキュータ数, node_status サンプル) を返す。古い DB では空。"""
    conn = sqlite3.connect(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    executors = {}
    if "nodes" in tables:
        executors = dict(conn.execute("SELECT node_name, executors FROM nodes"))
    samples = []
    if "node_status" in tables:
        samples = conn.execute(
            "SELECT sampled_at, node_name, offline FROM node_status"
            " ORDER BY node_name, sampled_at"
        ).fetchall()
    return executors, samples


def offline_intervals(samples, node_index, window_start_ms, now_ms):
    """node_status のサンプル列からノードごとのオフライン区間 [開始ms, 終了ms] を作る。

    オフラインのサンプルが続く間を 1 区間にまとめる。区間の終わりは次に
    オンラインが観測された時刻 (最後までオフラインなら現在時刻)。
    粒度は収集間隔に依存する。
    """
    intervals = [[] for _ in node_index]
    cur = {}
    for t, name, offline in samples:  # ノード名, 時刻順
        if name not in node_index:
            continue
        if offline and name not in cur:
            cur[name] = t
        elif not offline and name in cur:
            intervals[node_index[name]].append([cur.pop(name), t])
    for name, start in cur.items():
        intervals[node_index[name]].append([start, now_ms])
    for lst in intervals:
        lst[:] = [iv for iv in lst if iv[1] > window_start_ms]
    return intervals


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
    """ジョブごとの
    [timestamp, 結果コード, duration, queuing, number, ノードidx(, 欠落フラグ)]
    配列と、コード→結果名の対応表を作る。ノードidx は -1 = ノード不明
    (Pipeline など builtOn が取れないビルド)。

    期間開始時点でどの状態だったか分かるよう、期間より前のビルドも
    直近の 1 件だけ含める。結果名は数値コードに置き換えてサイズを抑える。

    欠落フラグ (7 番目, 1): 次に保存されているビルドと番号が連続していない印。
    Jenkins はログローテーション後も lastFailedBuild などを残すため、
    間のビルドが取得できていない期間がありうる。その区間はこのビルドの
    結果が続いたとは言えないので、テンプレート側で「状態不明」として扱う。
    """
    job_index = {j: i for i, j in enumerate(jobs)}
    result_codes = {}
    by_job = defaultdict(list)
    for job, result, ts, dur, queuing, number, node in rows:
        if job not in job_index:
            continue
        code = result_codes.setdefault(result, len(result_codes))
        nidx = node_index.get(node, -1) if node is not None else -1
        by_job[job].append((ts, code, dur, queuing, number, nidx))

    builds = []
    for job in jobs:
        items = by_job[job]
        encoded = []
        for i, (ts, code, dur, queuing, number, nidx) in enumerate(items):
            gap = i + 1 < len(items) and items[i + 1][4] != number + 1
            encoded.append(
                [ts, code, dur, queuing, number, nidx, 1] if gap
                else [ts, code, dur, queuing, number, nidx]
            )
        first_in = next(
            (i for i, b in enumerate(encoded) if b[0] >= window_start_ms), len(encoded)
        )
        builds.append(encoded[max(first_in - 1, 0):])

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

    job_filter = compile_filter(cfg)
    rows = [r for r in load_builds(cfg["db"]["path"]) if job_filter(r[0])]
    jobs = select_jobs(rows, window_start_ms)

    # ノード一覧: nodes テーブルとビルドの実行ノードの和集合 ('' はビルトインノード)
    executors, node_samples = load_nodes(cfg["db"]["path"])
    node_names = sorted({r[6] for r in rows if r[6] is not None} | set(executors))
    node_index = {n: i for i, n in enumerate(node_names)}

    builds, results = encode_builds(rows, jobs, node_index, window_start_ms)
    disabled, urls = load_jobs_meta(cfg["db"]["path"])

    data = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M"),
        "days": days,
        "now": now_ms,
        "jobs": jobs,
        "builds": builds,
        "results": results,
        "fail_results": FAIL_RESULTS,
        "disabled": [1 if j in disabled else 0 for j in jobs],
        "job_urls": [job_url(cfg["jenkins"]["url"], j, urls) for j in jobs],
        "jenkins_url": cfg["jenkins"]["url"].rstrip("/"),
        "nodes": [n if n else "(built-in)" for n in node_names],
        "node_executors": [executors.get(n, 1) for n in node_names],
        "node_offline": offline_intervals(node_samples, node_index, window_start_ms, now_ms),
    }

    template = Path(__file__).with_name("template.html").read_text(encoding="utf-8")
    html = template.replace("__DATA__", json.dumps(data, ensure_ascii=False))
    out = Path(cfg["report"].get("output", "report.html"))
    out.write_text(html, encoding="utf-8")
    print(f"{out} を生成しました (ジョブ {len(jobs)} 件, 最大期間 {days} 日)")


if __name__ == "__main__":
    main()
