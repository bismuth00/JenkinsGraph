"""Jenkins からジョブのビルド履歴を取得して SQLite に蓄積する。

cron などで定期実行する想定。ジョブごとに前回取り込んだビルド番号を記録し、
それより新しい完了済みビルドだけを差分取得する。
"""

import argparse
import os
import sqlite3
import sys
import tomllib

import requests

from jobfilter import compile_filter

SCHEMA = """
CREATE TABLE IF NOT EXISTS builds (
    job_name  TEXT    NOT NULL,
    number    INTEGER NOT NULL,
    result    TEXT    NOT NULL,  -- SUCCESS / FAILURE / UNSTABLE / ABORTED / NOT_BUILT
    timestamp INTEGER NOT NULL,  -- スケジュール時刻 = キュー投入時刻 (エポックミリ秒)
    duration  INTEGER NOT NULL,  -- 実行の所要時間 (ミリ秒)
    queuing   INTEGER NOT NULL DEFAULT 0,  -- キュー待ち時間 (ミリ秒)。実行開始は timestamp + queuing
    PRIMARY KEY (job_name, number)
);
"""


def migrate(conn):
    """既存 DB に後から追加したカラムを補う。"""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(builds)")}
    if "queuing" not in cols:
        conn.execute("ALTER TABLE builds ADD COLUMN queuing INTEGER NOT NULL DEFAULT 0")

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
    """フォルダ・マルチブランチを再帰的にたどり、ビルドを持つジョブを (fullName, url) で列挙する。"""
    stack = [root_url.rstrip("/") + "/"]
    while stack:
        url = stack.pop()
        res = session.get(
            url + "api/json",
            params={"tree": "jobs[_class,fullName,url]"},
            timeout=30,
        )
        res.raise_for_status()
        for job in res.json().get("jobs", []):
            cls = job.get("_class", "")
            if any(k in cls for k in CONTAINER_CLASS_KEYWORDS):
                stack.append(job["url"])
            else:
                yield job["fullName"], job["url"]


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
        params={"tree": f"{field}[number,result,timestamp,duration,actions[queuingDurationMillis]]"},
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
        yield b


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

    job_filter = compile_filter(cfg)
    total = 0
    for name, url in iter_jobs(session, cfg["jenkins"]["url"]):
        if not job_filter(name):
            continue
        since = conn.execute(
            "SELECT MAX(number) FROM builds WHERE job_name = ?", (name,)
        ).fetchone()[0]
        builds = list(fetch_new_builds(session, url, since))
        conn.executemany(
            "INSERT OR IGNORE INTO builds VALUES (?, ?, ?, ?, ?, ?)",
            [(name, b["number"], b["result"], b["timestamp"], b["duration"], b["queuing"])
             for b in builds],
        )
        conn.commit()
        if builds:
            print(f"{name}: {len(builds)} 件追加")
        total += len(builds)

    print(f"完了: {total} 件のビルドを取り込みました")


if __name__ == "__main__":
    main()
