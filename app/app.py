#!/usr/bin/env python3
"""双端 FASTQ 配对修复服务（Flask + SQLite）。

- 上传 R1/R2 两份 gzip FASTQ，后台流式配对；
- 全部结果文件发布且 SQLite 登记成功后才开放下载；
- 失败不暴露任何部分结果；重启时清理中断任务的未完成产物。
"""
import os
import shutil
import sqlite3
import threading
import time
import uuid

from flask import (Flask, abort, flash, g, redirect, render_template,
                   request, send_file, url_for)
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

import pipeline

DATA_DIR = os.path.abspath(os.environ.get(
    "DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")))
JOBS_DIR = os.path.join(DATA_DIR, "jobs")
DB_PATH = os.path.join(DATA_DIR, "app.db")

# 解压后上限（默认 256 MiB，可用环境变量覆盖以便测试）
MAX_DECOMP_BYTES = int(os.environ.get(
    "MAX_DECOMP_BYTES", str(pipeline.DEFAULT_MAX_DECOMP_BYTES)))
# 上传压缩包上限：解压上限 + 2 MiB 余量（不可压缩数据 gzip 后略大于原始）
MAX_UPLOAD_BYTES = int(os.environ.get(
    "MAX_UPLOAD_BYTES", str(MAX_DECOMP_BYTES + 2 * 1024 * 1024)))

OUTPUTS = {
    "paired_r1": "配对 R1（按 R1 原顺序）",
    "paired_r2": "配对 R2（按 R1 原顺序）",
    "unpaired_r1": "缺端 R1（R2 中无配对）",
    "unpaired_r2": "缺端 R2（R1 中无配对）",
}

# 任务终态（终态之间不可再迁移，取消与完成只能有一个胜出）
TERMINAL_STATUSES = ("done", "failed", "cancelled")
CANCELABLE_STATUSES = ("queued", "processing")
# 条件 UPDATE 共用片段：仅非终态行可被改写，保证终态唯一
_NOT_TERMINAL = "status NOT IN (" + ",".join("?" * len(TERMINAL_STATUSES)) + ")"
_CANCELABLE = "status IN (" + ",".join("?" * len(CANCELABLE_STATUSES)) + ")"
CANCEL_MESSAGE = "任务已由用户取消，上传文件与中间产物已清理"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
app.secret_key = os.environ.get("SECRET_KEY", "fastq-pairer-dev-secret")

# jid -> threading.Event：取消请求置位，处理线程在流水线协作点检查后退出
_cancel_events = {}
_cancel_lock = threading.Lock()


# ---------------- SQLite 登记库 ----------------

def connect_db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def get_db():
    if "db" not in g:
        g.db = connect_db()
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_and_recover():
    """建表 + 启动恢复。

    - 重启时处于 queued/processing 的任务：处理线程已随进程消失，标记失败
      并清理其 work/out 中间产物（上传目录沿用既有策略，由取消清理负责）；
    - 已取消（cancelled）任务：清理残留的整个任务目录，状态保持“已取消”；
    - 已完成（done）任务完全不受影响，结果仍可下载。
    """
    os.makedirs(JOBS_DIR, exist_ok=True)
    con = connect_db()
    con.execute(
        """CREATE TABLE IF NOT EXISTS jobs(
               id TEXT PRIMARY KEY,
               created_at TEXT NOT NULL,
               status TEXT NOT NULL,          -- queued/processing/done/failed/cancelled
               message TEXT NOT NULL DEFAULT '',
               r1_name TEXT NOT NULL DEFAULT '',
               r2_name TEXT NOT NULL DEFAULT '',
               paired INTEGER, r1_only INTEGER, r2_only INTEGER)""")
    con.commit()
    interrupted = [r["id"] for r in con.execute(
        f"SELECT id FROM jobs WHERE {_CANCELABLE}",
        list(CANCELABLE_STATUSES))]
    for jid in interrupted:
        job_dir = os.path.join(JOBS_DIR, jid)
        shutil.rmtree(os.path.join(job_dir, "work"), ignore_errors=True)
        shutil.rmtree(os.path.join(job_dir, "out"), ignore_errors=True)
        con.execute(
            "UPDATE jobs SET status='failed', "
            "message='服务重启导致任务中断，未完成产物已清理' WHERE id=?", (jid,))
    con.commit()
    # 已取消任务：继续清理残留（上传、中间产物、可能未删净的整个目录）
    cancelled = [r["id"] for r in con.execute(
        "SELECT id FROM jobs WHERE status='cancelled'")]
    for jid in cancelled:
        shutil.rmtree(os.path.join(JOBS_DIR, jid), ignore_errors=True)
    if cancelled:
        app.logger.info("已继续清理 %d 个已取消任务的残留文件", len(cancelled))
    known = {r["id"] for r in con.execute("SELECT id FROM jobs")}
    for name in os.listdir(JOBS_DIR):
        if name not in known:
            shutil.rmtree(os.path.join(JOBS_DIR, name), ignore_errors=True)
    con.close()
    if interrupted:
        app.logger.info("已清理 %d 个中断任务", len(interrupted))


# ---------------- 取消协作 ----------------

def register_cancel_event(jid):
    """处理线程启动时登记自己的取消事件，返回该事件。"""
    with _cancel_lock:
        ev = _cancel_events.get(jid)
        if ev is None:  # 取消可能在线程真正启动前就已发生
            ev = threading.Event()
            _cancel_events[jid] = ev
        return ev


def signal_cancel(jid):
    """取消请求置位事件（无论处理线程是否已启动）。"""
    with _cancel_lock:
        ev = _cancel_events.get(jid)
        if ev is None:
            ev = threading.Event()
            _cancel_events[jid] = ev
        ev.set()


def drop_cancel_event(jid):
    with _cancel_lock:
        _cancel_events.pop(jid, None)


def cleanup_job_dir(jid):
    """取消终态：整个任务目录（上传/中间产物/任何结果）全部清理。"""
    shutil.rmtree(os.path.join(JOBS_DIR, jid), ignore_errors=True)


def _terminal_status(con, jid):
    row = con.execute("SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()
    return row["status"] if row is not None else None


# ---------------- 后台处理 ----------------

def process_job(jid):
    """后台线程：跑流水线；全部结果发布 + SQLite 登记成功才算完成。

    取消与完成通过 SQLite 条件 UPDATE 互斥，保证只有一个终态：
      - 完成提交（status=done）在前 -> 取消 UPDATE 命中 0 行，结果保留；
      - 取消生效（status=cancelled）在前 -> 完成 UPDATE 命中 0 行，
        处理线程不得再发布，并清理全部文件。
    """
    job_dir = os.path.join(JOBS_DIR, jid)
    work = os.path.join(job_dir, "work")
    out = os.path.join(job_dir, "out")
    cancel_event = register_cancel_event(jid)
    con = connect_db()
    try:
        cur = con.execute(
            f"UPDATE jobs SET status='processing' "
            f"WHERE id=? AND {_CANCELABLE}",
            [jid] + list(CANCELABLE_STATUSES))
        con.commit()
        if cur.rowcount == 0:
            # 排队阶段已被取消（或已进入终态）：不得继续处理
            if cancel_event.is_set():
                con.execute(
                    f"UPDATE jobs SET status='cancelled', message=? "
                    f"WHERE id=? AND {_NOT_TERMINAL}",
                    [CANCEL_MESSAGE, jid] + list(TERMINAL_STATUSES))
                con.commit()
                cleanup_job_dir(jid)
            return
        os.makedirs(work, exist_ok=True)
        counts = pipeline.run(
            os.path.join(job_dir, "uploads", "r1.fastq.gz"),
            os.path.join(job_dir, "uploads", "r2.fastq.gz"),
            work, out, max_bytes=MAX_DECOMP_BYTES,
            cancel_event=cancel_event)
        # 结果文件已全部落盘，最后登记；条件 UPDATE 保证取消先生效时不覆盖终态
        cur = con.execute(
            f"UPDATE jobs SET status='done', paired=?, r1_only=?, r2_only=?, "
            f"message='' WHERE id=? AND {_NOT_TERMINAL}",
            [counts["paired"], counts["r1_only"], counts["r2_only"], jid]
            + list(TERMINAL_STATUSES))
        con.commit()
        if cur.rowcount == 0:
            # 终态已被别的请求确定：取消先生效则不得发布，清理并保持“已取消”
            if _terminal_status(con, jid) == "cancelled":
                shutil.rmtree(out, ignore_errors=True)
                cleanup_job_dir(jid)
            # done/failed 已先提交：尊重该终态，不动其文件
    except pipeline.JobCancelled:
        con.rollback()
        shutil.rmtree(out, ignore_errors=True)  # 取消不暴露任何部分结果
        cur = con.execute(
            f"UPDATE jobs SET status='cancelled', message=? "
            f"WHERE id=? AND {_NOT_TERMINAL}",
            [CANCEL_MESSAGE, jid] + list(TERMINAL_STATUSES))
        con.commit()
        if cur.rowcount > 0:
            cleanup_job_dir(jid)
            return
        # 条件 UPDATE 未命中：终态已由取消请求或完成提交先确定，按实际状态处置
        final_status = _terminal_status(con, jid)
        if final_status == "cancelled":
            # 取消先生效（状态可能由取消请求的 UPDATE 写入）：仍须清理本线程产物
            cleanup_job_dir(jid)
        elif final_status == "done":
            # 极窄窗口：完成已先提交。保留完整结果，不再改为取消
            app.logger.info("任务 %s 已先完成，取消不生效，结果保留", jid)
    except pipeline.PipelineError as e:
        con.rollback()
        if cancel_event.is_set():
            # 数据校验报错与取消同时发生：取消先生效则以取消为准
            cur = con.execute(
                f"UPDATE jobs SET status='cancelled', message=? "
                f"WHERE id=? AND {_NOT_TERMINAL}",
                [CANCEL_MESSAGE, jid] + list(TERMINAL_STATUSES))
            con.commit()
            if cur.rowcount > 0 or _terminal_status(con, jid) == "cancelled":
                shutil.rmtree(out, ignore_errors=True)
                cleanup_job_dir(jid)
                return
        shutil.rmtree(out, ignore_errors=True)  # 失败不暴露部分结果
        con.execute(
            f"UPDATE jobs SET status='failed', message=? "
            f"WHERE id=? AND {_NOT_TERMINAL}",
            [str(e), jid] + list(TERMINAL_STATUSES))
        con.commit()
    except Exception as e:  # 意外错误（含登记失败）：同样清理结果
        con.rollback()
        shutil.rmtree(out, ignore_errors=True)
        con.execute(
            f"UPDATE jobs SET status='failed', message=? "
            f"WHERE id=? AND {_NOT_TERMINAL}",
            [f"内部错误: {e!r}", jid] + list(TERMINAL_STATUSES))
        con.commit()
        app.logger.exception("任务 %s 处理异常", jid)
    finally:
        shutil.rmtree(work, ignore_errors=True)  # 中间产物（索引/记录文件）即清
        con.close()
        drop_cancel_event(jid)


# ---------------- 页面与接口 ----------------

@app.route("/")
def index():
    jobs = get_db().execute(
        "SELECT * FROM jobs ORDER BY created_at DESC, id DESC LIMIT 200").fetchall()
    active = any(j["status"] in ("queued", "processing") for j in jobs)
    return render_template("index.html", jobs=jobs, active=active,
                           max_mib=MAX_DECOMP_BYTES // (1024 * 1024))


@app.route("/jobs", methods=["POST"])
def create_job():
    f1 = request.files.get("r1")
    f2 = request.files.get("r2")
    if not f1 or not f1.filename or not f2 or not f2.filename:
        flash("请同时选择 R1 和 R2 两个 gzip FASTQ 文件", "error")
        return redirect(url_for("index"))
    jid = time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8]
    uploads = os.path.join(JOBS_DIR, jid, "uploads")
    os.makedirs(uploads)
    f1.save(os.path.join(uploads, "r1.fastq.gz"))
    f2.save(os.path.join(uploads, "r2.fastq.gz"))
    db = get_db()
    db.execute(
        "INSERT INTO jobs(id, created_at, status, r1_name, r2_name) "
        "VALUES (?,?,?,?,?)",
        (jid, time.strftime("%Y-%m-%d %H:%M:%S"), "queued",
         secure_filename(f1.filename) or "r1.fastq.gz",
         secure_filename(f2.filename) or "r2.fastq.gz"))
    db.commit()
    threading.Thread(target=process_job, args=(jid,), daemon=True).start()
    return redirect(url_for("job_detail", jid=jid))


@app.route("/jobs/<jid>")
def job_detail(jid):
    job = get_db().execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if job is None:
        abort(404)
    return render_template("job.html", job=job, outputs=OUTPUTS,
                           output_files=pipeline.OUTPUT_FILES)


@app.route("/jobs/<jid>/cancel", methods=["POST"])
def cancel_job(jid):
    """取消排队中/处理中的任务。

    以 SQLite 条件 UPDATE 作为唯一仲裁：仅 queued/processing 可迁移到
    cancelled。若完成（或失败）已先提交，则保留其终态并告知不能取消。
    """
    db = get_db()
    before = db.execute(
        "SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()
    if before is None:
        abort(404)
    cur = db.execute(
        f"UPDATE jobs SET status='cancelled', message=? "
        f"WHERE id=? AND {_CANCELABLE}",
        [CANCEL_MESSAGE, jid] + list(CANCELABLE_STATUSES))
    db.commit()
    if cur.rowcount == 0:
        # 终态已先确定：完成保留完整结果，失败/已取消也不可再取消
        if before["status"] == "done":
            flash("任务已完成并保留完整结果，不能取消；四份结果可正常下载。",
                  "error")
        elif before["status"] == "cancelled":
            flash("任务此前已取消。", "info")
        else:
            flash("任务已失败并清理过产物，无需取消。", "info")
        return redirect(url_for("job_detail", jid=jid))
    # 取消先生效：置位协作事件，让处理线程尽快停止解压与配对
    signal_cancel(jid)
    # 排队中（处理线程尚未真正起跑）的任务，其目录由本请求直接清理；
    # 处理中的任务由线程在退出路径清理，启动恢复也会兜底清理残留。
    if before["status"] == "queued":
        cleanup_job_dir(jid)
    flash("取消请求已生效，任务将尽快停止；状态保持“已取消”，"
          "上传与中间产物会被清理，结果不可下载。", "info")
    return redirect(url_for("job_detail", jid=jid))


@app.route("/jobs/<jid>/download/<key>")
def download(jid, key):
    if key not in OUTPUTS:
        abort(404)
    job = get_db().execute("SELECT status FROM jobs WHERE id=?", (jid,)).fetchone()
    if job is None:
        abort(404)
    if job["status"] != "done":
        # 未完成/失败任务一律不暴露产物
        abort(409)
    path = os.path.join(JOBS_DIR, jid, "out", pipeline.OUTPUT_FILES[key])
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, mimetype="application/gzip", as_attachment=True,
                     download_name=f"{jid}_{pipeline.OUTPUT_FILES[key]}")


@app.route("/healthz")
def healthz():
    return "ok"


@app.errorhandler(RequestEntityTooLarge)
def too_large(_e):
    flash(f"上传文件超过压缩大小上限（{MAX_UPLOAD_BYTES // (1024 * 1024)} MiB）",
          "error")
    return redirect(url_for("index")), 413


if __name__ == "__main__":
    init_and_recover()
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
