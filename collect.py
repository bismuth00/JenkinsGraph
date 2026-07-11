"""Jenkins からジョブのビルド履歴を取得して SQLite に蓄積する。

cron などで定期実行する想定。ジョブごとに前回取り込んだビルド番号を記録し、
それより新しい完了済みビルドだけを差分取得する。
"""

import argparse
import os
import sqlite3
import sys
import time
import tomllib

import requests

from jobfilter import compile_filter

SCHEMA = """
CREATE TABLE IF NOT EXISTS builds (
    job_name  TEXT    NOT NULL,
    number    INTEGER NOT NULL,
    result    TEXT    NOT NULL,  -- SUCCESS / FAILURE / UNSTABLE / ABORTED / NOT_BUILT
    timestamp INTEGER NOT NULL,  -- 実行開始時刻 (エポックミリ秒)
    duration  INTEGER NOT NULL,  -- 実行の所要時間 (ミリ秒)
    queuing   INTEGER NOT NULL DEFAULT 0,  -- 実行開始前のキュー待ち時間 (ミリ秒)。キュー投入は timestamp - queuing
    PRIMARY KEY (job_name, number)
);
CREATE TABLE IF NOT EXISTS jobs (
    job_name   TEXT PRIMARY KEY,
    buildable  INTEGER NOT NULL DEFAULT 1,  -- 0 = Jenkins 上で無効化されている
    url        TEXT NOT NULL DEFAULT '',    -- ジョブページの URL (ビルドページはこれ + 番号)
    concurrent INTEGER                      -- 1 = 並列実行可 (concurrentBuild)。NULL = 不明
);
CREATE TABLE IF NOT EXISTS nodes (
    node_name TEXT PRIMARY KEY,            -- ビルトインノードは '' で記録
    executors INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS node_status (
    sampled_at INTEGER NOT NULL,           -- サンプリング時刻 (エポックミリ秒)
    node_name  TEXT NOT NULL,
    offline    INTEGER NOT NULL
);
"""


def migrate(conn):
    """既存 DB に後から追加したカラムを補う。"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(builds)")}
    if "queuing" not in cols:
        conn.execute("ALTER TABLE builds ADD COLUMN queuing INTEGER NOT NULL DEFAULT 0")
    if "node" not in cols:
        # NULL = ノード不明 (Pipeline など builtOn を返さないビルド)、'' = ビルトインノード
        conn.execute("ALTER TABLE builds ADD COLUMN node TEXT")
    if "upstream_job" not in cols:
        # このビルドを起動した上流ビルド (UpstreamCause)。NULL = 手動/スケジュール起動など
        conn.execute("ALTER TABLE builds ADD COLUMN upstream_job TEXT")
        conn.execute("ALTER TABLE builds ADD COLUMN upstream_build INTEGER")
    job_cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
    if job_cols and "url" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN url TEXT NOT NULL DEFAULT ''")
    if job_cols and "concurrent" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN concurrent INTEGER")

# _class にこれらを含むものはジョブではなくコンテナとして再帰的にたどる
CONTAINER_CLASS_KEYWORDS = ("Folder", "MultiBranchProject", "OrganizationFolder")


def load_config(path):
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    token = os.environ.get("JENKINS_TOKEN") or cfg["jenkins"].get("token")
    if not token:
        sys.exit("Jenkins の API トークンを config.toml か環境変数 JENKINS_TOKEN で指定してください")
    cfg["jenkins"]["token"] = token
    return cfg


def iter_jobs(session, root_url):
    """フォルダ・マルチブランチを再帰的にたどり、ビルドを持つジョブを
    (fullName, url, buildable, concurrent) で列挙する。
    buildable=False は無効化されたジョブ。concurrent は並列実行の可否
    (concurrentBuild)。フィールドを返さないジョブ種別では None (不明)。"""
    stack = [root_url.rstrip("/") + "/"]
    while stack:
        url = stack.pop()
        res = session.get(
            url + "api/json",
            params={"tree": "jobs[_class,fullName,url,buildable,concurrentBuild]"},
            timeout=30,
        )
        res.raise_for_status()
        for job in res.json().get("jobs", []):
            cls = job.get("_class", "")
            if any(k in cls for k in CONTAINER_CLASS_KEYWORDS):
                stack.append(job["url"])
            else:
                concurrent = job.get("concurrentBuild")
                yield (job["fullName"], job["url"], job.get("buildable", True),
                       None if concurrent is None else (1 if concurrent else 0))


def fetch_new_builds(session, job_url, since_number):
    """since_number より新しい完了済みビルドを返す。初回 (None) は全履歴を取得する。

    2 回目以降に使う builds フィールドは直近 100 件しか返さないため、
    実行間隔はジョブのビルド頻度に対して十分短くすること。

    キュー待ち時間は Metrics プラグインが付与する TimeInQueueAction の
    queuingDurationMillis から取る。プラグインがない環境では 0 になる。
    """
    field = "allBuilds" if since_number is None else "builds"
    res = session.get(
        job_url + "api/json",
        params={"tree": f"{field}[number,result,timestamp,duration,builtOn,"
                        "actions[queuingDurationMillis,causes[upstreamProject,upstreamBuild]]]"},
        timeout=120,
    )
    res.raise_for_status()
    for b in res.json().get(field) or []:
        if b.get("result") is None:  # 実行中のビルドは次回に取り込む
            continue
        if since_number is not None and b["number"] <= since_number:
            continue
        b["queuing"] = next(
            (a["queuingDurationMillis"] for a in b.get("actions") or []
             if a and "queuingDurationMillis" in a),
            0,
        )
        # builtOn: '' はビルトインノード。フィールド自体がない (Pipeline など) は
        # None = ノード不明として記録する
        b["node"] = b.get("builtOn")
        # UpstreamCause: このビルドを起動した上流ビルド (パイプラインの build ステップ等)
        up = next(
            (c for a in b.get("actions") or [] if a
             for c in a.get("causes") or [] if c and "upstreamProject" in c),
            None,
        )
        b["up_job"] = up["upstreamProject"] if up else None
        b["up_num"] = up["upstreamBuild"] if up else None
        yield b


def sample_nodes(session, base_url, conn):
    """ノード一覧とオンライン/オフライン状態を記録する (実行のたびに 1 サンプル)。"""
    res = session.get(
        base_url.rstrip("/") + "/computer/api/json",
        params={"tree": "computer[displayName,offline,numExecutors]"},
        timeout=30,
    )
    res.raise_for_status()
    now_ms = int(time.time() * 1000)
    for c in res.json().get("computer", []):
        name = c.get("displayName", "")
        if name in ("Built-In Node", "master"):
            name = ""  # ビルドの builtOn ('') と揃える
        conn.execute(
            "INSERT INTO nodes (node_name, executors) VALUES (?, ?)"
            " ON CONFLICT(node_name) DO UPDATE SET executors = excluded.executors",
            (name, c.get("numExecutors", 1)),
        )
        conn.execute(
            "INSERT INTO node_status VALUES (?, ?, ?)",
            (now_ms, name, 1 if c.get("offline") else 0),
        )
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.toml")
    args = parser.parse_args()
    cfg = load_config(args.config)

    conn = sqlite3.connect(cfg["db"]["path"])
    conn.executescript(SCHEMA)
    migrate(conn)

    session = requests.Session()
    session.auth = (cfg["jenkins"]["user"], cfg["jenkins"]["token"])

    sample_nodes(session, cfg["jenkins"]["url"], conn)

    job_filter = compile_filter(cfg)
    total = 0
    for name, url, buildable, concurrent in iter_jobs(session, cfg["jenkins"]["url"]):
        if not job_filter(name):
            continue
        conn.execute(
            "INSERT INTO jobs (job_name, buildable, url, concurrent) VALUES (?, ?, ?, ?)"
            " ON CONFLICT(job_name) DO UPDATE SET"
            " buildable = excluded.buildable, url = excluded.url,"
            " concurrent = excluded.concurrent",
            (name, 1 if buildable else 0, url, concurrent),
        )
        since = conn.execute(
            "SELECT MAX(number) FROM builds WHERE job_name = ?", (name,)
        ).fetchone()[0]
        builds = list(fetch_new_builds(session, url, since))
        conn.executemany(
            "INSERT OR IGNORE INTO builds VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(name, b["number"], b["result"], b["timestamp"], b["duration"],
              b["queuing"], b["node"], b["up_job"], b["up_num"]) for b in builds],
        )
        conn.commit()
        if builds:
            print(f"{name}: {len(builds)} 件追加")
        total += len(builds)

    print(f"完了: {total} 件のビルドを取り込みました")


if __name__ == "__main__":
    main()
