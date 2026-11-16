#!/usr/bin/env python3
"""取消功能关键路径自测（不依赖 pytest，直接运行）。

  PYTHONPATH=app <python> -I tests/test_cancel.py

覆盖：
  1. 处理中取消：状态 cancelled、线程尽快停止、任务目录最终清空、下载 409；
  2. 排队中取消：线程不再处理，目录被清理，状态保持已取消；
  3. 已完成再取消：终态 done 保留、四份结果可下载、取消被拒绝；
  4. 取消/完成同时发生的两个方向（条件 UPDATE 仲裁，只有一个终态）；
  5. 重启恢复：cancelled 残留被继续清理且状态不变，done 不受影响；
  6. pipeline 层 JobCancelled 协作中断，不发布任何结果；
  7. HTTP 路径：GET 取消 405、未知任务 404、详情页取消按钮与已取消展示。
"""
import gzip
import os
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

import pipeline  # noqa: E402

_tmp = tempfile.mkdtemp(prefix="cancel-test-")
os.environ["DATA_DIR"] = os.path.join(_tmp, "data")
os.environ["MAX_DECOMP_BYTES"] = str(64 * 1024 * 1024)

import app as webapp  # noqa: E402

PASS, FAILS = 0, []


def check(name, cond, detail=""):
    global PASS
    if cond:
        PASS += 1
        print(f"  OK   {name}")
    else:
        FAILS.append((name, detail))
        print(f" FAIL  {name} {detail}")


def make_fastq_pair(dirpath, n=80000, seed=3):
    """生成两份足够大的合法 gzip FASTQ，保证取消时任务仍在处理。"""
    r1, r2 = os.path.join(dirpath, "r1.fastq.gz"), os.path.join(dirpath, "r2.fastq.gz")
    seq, qual = "ACGT" * 25, "I" * 100
    with gzip.open(r1, "wt", compresslevel=1) as f:
        for i in range(n):
            f.write(f"@SIM:{(i * 7 + seed) % n}/1\n{seq}\n+\n{qual}\n")
    with gzip.open(r2, "wt", compresslevel=1) as f:
        for i in range(n):
            f.write(f"@SIM:{(i * 13 + seed) % n}/2\n{seq}\n+\n{qual}\n")
    return r1, r2


def make_small_pair(dirpath, n=200):
    r1, r2 = os.path.join(dirpath, "s_r1.gz"), os.path.join(dirpath, "s_r2.gz")
    with gzip.open(r1, "wt") as f:
        for i in range(n):
            f.write(f"@X:{i}/1\nACGT\n+\nIIII\n")
    with gzip.open(r2, "wt") as f:
        for i in range(n):
            f.write(f"@X:{i}/2\nACGT\n+\nIIII\n")
    return r1, r2


def new_queued_job(r1, r2):
    """绕过 HTTP 上传，直接登记一个 queued 任务并落好上传文件。"""
    jid = time.strftime("%Y%m%d-%H%M%S-") + os.urandom(4).hex()
    uploads = os.path.join(webapp.JOBS_DIR, jid, "uploads")
    os.makedirs(uploads)
    shutil.copy(r1, os.path.join(uploads, "r1.fastq.gz"))
    shutil.copy(r2, os.path.join(uploads, "r2.fastq.gz"))
    con = webapp.connect_db()
    con.execute(
        "INSERT INTO jobs(id, created_at, status, r1_name, r2_name) "
        "VALUES (?,?,?,?,?)",
        (jid, time.strftime("%Y-%m-%d %H:%M:%S"), "queued", "r1.gz", "r2.gz"))
    con.commit()
    con.close()
    return jid


def status_of(jid):
    con = webapp.connect_db()
    row = con.execute("SELECT status, message FROM jobs WHERE id=?", (jid,)).fetchone()
    con.close()
    return row["status"], row["message"]


def wait_status(jid, wanted, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st, _ = status_of(jid)
        if st == wanted:
            return True
        time.sleep(0.05)
    return False


def wait_thread_done(timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if threading.active_count() <= 1:
            return True
        time.sleep(0.05)
    return False


def job_dir(jid):
    return os.path.join(webapp.JOBS_DIR, jid)


class PipelineGate:
    """patch pipeline.run_started：测试在“已进入流水线但未处理”时刻取得控制。"""

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self._orig = None

    def __enter__(self):
        self._orig = pipeline.run_started
        def hook():
            self.entered.set()
            self.release.wait(30)
        pipeline.run_started = hook
        return self

    def __exit__(self, *exc):
        self.release.set()
        pipeline.run_started = self._orig


def cancel_via_db(jid):
    """模拟取消路由的仲裁 UPDATE（独立连接），返回命中行数。"""
    con = webapp.connect_db()
    cur = con.execute(
        "UPDATE jobs SET status='cancelled', message=? "
        "WHERE id=? AND status IN ('queued','processing')",
        (webapp.CANCEL_MESSAGE, jid))
    con.commit()
    rows = cur.rowcount
    con.close()
    return rows


# ---------------- 1. 处理中取消 ----------------

def test_cancel_processing():
    print("[1] 处理中取消")
    src = os.path.join(_tmp, "src1")
    os.makedirs(src)
    r1, r2 = make_fastq_pair(src)
    jid = new_queued_job(r1, r2)
    with PipelineGate() as gate:
        t = threading.Thread(target=webapp.process_job, args=(jid,), daemon=True)
        t.start()
        check("流水线已进入（任务必然 processing）", gate.entered.wait(10))
        check("取消前为 processing", status_of(jid)[0] == "processing",
              status_of(jid))
        t0 = time.time()
        rows = cancel_via_db(jid)
        check("取消 UPDATE 命中 1 行", rows == 1, f"rowcount={rows}")
        webapp.signal_cancel(jid)
        gate.release.set()
        t.join(timeout=30)
    elapsed = time.time() - t0
    check("处理线程已退出", not t.is_alive())
    check("尽快停止（< 10s）", elapsed < 10, f"elapsed={elapsed:.2f}s")
    st, msg = status_of(jid)
    check("状态保持 cancelled", st == "cancelled", st)
    check("取消说明已登记", "取消" in msg, msg)
    check("整个任务目录已清理", not os.path.exists(job_dir(jid)), job_dir(jid))
    # 下载四份结果均被拒
    client = webapp.app.test_client()
    for key in webapp.OUTPUTS:
        resp = client.get(f"/jobs/{jid}/download/{key}")
        check(f"下载 {key} 返回 409", resp.status_code == 409,
              f"got {resp.status_code}")
    detail = client.get(f"/jobs/{jid}")
    check("详情页含“已取消”", "已取消".encode() in detail.data)
    check("详情页不含取消按钮（终态）",
          f"/jobs/{jid}/cancel".encode() not in detail.data)


# ---------------- 2. 排队中取消（线程尚未起跑） ----------------

def test_cancel_queued_before_thread():
    print("[2] 排队中取消（线程尚未起跑）")
    src = os.path.join(_tmp, "src2")
    os.makedirs(src)
    r1, r2 = make_small_pair(src)
    jid = new_queued_job(r1, r2)
    # 模拟取消请求在线程启动前到达
    webapp.signal_cancel(jid)
    con = webapp.connect_db()
    cur = con.execute(
        "UPDATE jobs SET status='cancelled', message=? "
        "WHERE id=? AND status IN ('queued','processing')",
        (webapp.CANCEL_MESSAGE, jid))
    con.commit()
    con.close()
    check("排队取消 UPDATE 命中", cur.rowcount == 1)
    webapp.cleanup_job_dir(jid)  # 与路由一致：queued 由请求直接清理
    t = threading.Thread(target=webapp.process_job, args=(jid,), daemon=True)
    t.start()
    t.join(timeout=15)
    check("线程退出且未处理", not t.is_alive())
    check("状态仍为 cancelled", status_of(jid)[0] == "cancelled")
    check("目录不存在", not os.path.exists(job_dir(jid)))


# ---------------- 3. 完成后再取消：完成先提交，保留结果 ----------------

def test_cancel_after_done():
    print("[3] 已完成任务不可取消，结果保留")
    src = os.path.join(_tmp, "src3")
    os.makedirs(src)
    r1, r2 = make_small_pair(src)
    jid = new_queued_job(r1, r2)
    t = threading.Thread(target=webapp.process_job, args=(jid,), daemon=True)
    t.start()
    check("正常跑完变 done", wait_status(jid, "done"), status_of(jid))
    t.join(timeout=15)
    out = os.path.join(job_dir(jid), "out")
    files = sorted(os.listdir(out))
    check("四份结果齐全", files == sorted(pipeline.OUTPUT_FILES.values()),
          str(files))
    client = webapp.app.test_client()
    resp = client.post(f"/jobs/{jid}/cancel", follow_redirects=True)
    check("取消请求返回 200（重定向到详情）", resp.status_code == 200)
    st, _ = status_of(jid)
    check("终态仍为 done", st == "done", st)
    check("页面提示不能取消", "不能取消".encode() in resp.data)
    for key, fname in pipeline.OUTPUT_FILES.items():
        r = client.get(f"/jobs/{jid}/download/{key}")
        check(f"{fname} 仍可下载（200）", r.status_code == 200,
              f"got {r.status_code}")
    check("结果目录保留", os.path.isdir(out))


# ---------------- 4a. 完成提交与取消竞争：取消在完成 UPDATE 前生效 ----------------

def test_race_cancel_wins():
    print("[4a] 竞争：取消先生效，处理线程不得发布")
    src = os.path.join(_tmp, "src4a")
    os.makedirs(src)
    r1, r2 = make_small_pair(src)
    jid = new_queued_job(r1, r2)
    real_run = pipeline.run
    gate = threading.Event()

    def fake_run(*a, **kw):
        gate.wait(10)  # 等取消在“结果即将发布”前置位
        return real_run(*a, **kw)

    pipeline.run = fake_run
    try:
        t = threading.Thread(target=webapp.process_job, args=(jid,), daemon=True)
        t.start()
        wait_status(jid, "processing")
        # 取消先生效
        webapp.signal_cancel(jid)
        con = webapp.connect_db()
        cur = con.execute(
            "UPDATE jobs SET status='cancelled', message=? "
            "WHERE id=? AND status IN ('queued','processing')",
            (webapp.CANCEL_MESSAGE, jid))
        con.commit()
        con.close()
        check("取消先命中", cur.rowcount == 1)
        gate.set()  # 放行流水线（其内部协作点随即抛 JobCancelled）
        t.join(timeout=20)
        check("线程退出", not t.is_alive())
    finally:
        gate.set()
        pipeline.run = real_run
    st, _ = status_of(jid)
    check("终态只有 cancelled", st == "cancelled", st)
    check("任务目录被清理", not os.path.exists(job_dir(jid)))


# ---------------- 4b. 竞争：完成先提交，取消被拒 ----------------

def test_race_done_wins():
    print("[4b] 竞争：完成先提交，取消不生效")
    src = os.path.join(_tmp, "src4b")
    os.makedirs(src)
    r1, r2 = make_small_pair(src)
    jid = new_queued_job(r1, r2)
    t = threading.Thread(target=webapp.process_job, args=(jid,), daemon=True)
    t.start()
    check("任务完成", wait_status(jid, "done"))
    t.join(timeout=15)
    # 迟到的取消：路由层条件 UPDATE 必须命中 0 行
    client = webapp.app.test_client()
    resp = client.post(f"/jobs/{jid}/cancel", follow_redirects=True)
    st, _ = status_of(jid)
    check("终态保持 done", st == "done", st)
    check("提示不能取消", "不能取消".encode() in resp.data)
    check("结果目录保留", os.path.isdir(os.path.join(job_dir(jid), "out")))


# ---------------- 5. 重启恢复 ----------------

def test_restart_recovery():
    print("[5] 重启恢复：取消残留清理、完成不受影响")
    # cancelled 任务带残留目录
    src = os.path.join(_tmp, "src5")
    os.makedirs(src)
    r1, r2 = make_small_pair(src)
    jc = new_queued_job(r1, r2)
    con = webapp.connect_db()
    con.execute("UPDATE jobs SET status='cancelled', message=? WHERE id=?",
                (webapp.CANCEL_MESSAGE, jc))
    con.commit()
    con.close()
    leftover = os.path.join(job_dir(jc), "uploads", "r1.fastq.gz")
    check("取消任务残留存在", os.path.isfile(leftover))
    # done 任务带完整结果
    jd = new_queued_job(r1, r2)
    t = threading.Thread(target=webapp.process_job, args=(jd,), daemon=True)
    t.start()
    wait_status(jd, "done")
    t.join(15)
    done_files = os.listdir(os.path.join(job_dir(jd), "out"))
    # 模拟重启
    webapp._cancel_events.clear()
    webapp.init_and_recover()
    check("取消任务残留目录已删除", not os.path.exists(job_dir(jc)))
    check("取消任务状态不变", status_of(jc)[0] == "cancelled")
    check("完成任务四份结果仍在",
          sorted(os.listdir(os.path.join(job_dir(jd), "out"))) == sorted(done_files))
    check("完成任务状态仍 done", status_of(jd)[0] == "done")
    client = webapp.app.test_client()
    r = client.get(f"/jobs/{jd}/download/paired_r1")
    check("完成任务重启后仍可下载", r.status_code == 200)


# ---------------- 6. pipeline 层协作中断 ----------------

def test_pipeline_job_cancelled():
    print("[6] pipeline 层 JobCancelled：不发布任何结果")
    src = os.path.join(_tmp, "src6")
    os.makedirs(src)
    r1, r2 = make_fastq_pair(src, n=150000)
    work = os.path.join(_tmp, "p6work")
    out = os.path.join(_tmp, "p6out")
    os.makedirs(work)
    ev = threading.Event()
    ev.set()  # 进入即取消
    raised = False
    t0 = time.time()
    try:
        pipeline.run(r1, r2, work, out, cancel_event=ev)
    except pipeline.JobCancelled:
        raised = True
    check("立即抛 JobCancelled", raised and time.time() - t0 < 2)
    check("out 目录无任何结果文件",
          (not os.path.isdir(out)) or os.listdir(out) == [])
    # 中途取消：先跑一小段再置位
    ev2 = threading.Event()
    work2 = os.path.join(_tmp, "p6work2")
    out2 = os.path.join(_tmp, "p6out2")
    os.makedirs(work2)
    holder = {}

    def stopper():
        time.sleep(0.3)
        ev2.set()

    th = threading.Thread(target=stopper)
    th.start()
    try:
        pipeline.run(r1, r2, work2, out2, cancel_event=ev2)
    except pipeline.JobCancelled:
        holder["cancelled"] = True
    th.join()
    check("中途取消抛 JobCancelled", holder.get("cancelled") is True)
    check("中途取消不发布结果",
          (not os.path.isdir(out2)) or os.listdir(out2) == [])


# ---------------- 7. HTTP 细节 ----------------

def test_http_details():
    print("[7] HTTP 细节：405 / 404 / 取消按钮展示")
    src = os.path.join(_tmp, "src7")
    os.makedirs(src)
    r1, r2 = make_fastq_pair(src)
    jid = new_queued_job(r1, r2)
    with PipelineGate() as gate:
        t = threading.Thread(target=webapp.process_job, args=(jid,), daemon=True)
        t.start()
        gate.entered.wait(10)
        client = webapp.app.test_client()
        r_get = client.get(f"/jobs/{jid}/cancel")
        check("GET 取消接口返回 405", r_get.status_code == 405,
              f"got {r_get.status_code}")
        r_404 = client.post("/jobs/NO-SUCH-JOB/cancel")
        check("未知任务取消返回 404", r_404.status_code == 404)
        detail = client.get(f"/jobs/{jid}")
        check("处理中详情页含取消按钮",
              f'action="/jobs/{jid}/cancel"'.encode() in detail.data)
        # 经 HTTP 走完整取消
        r_cancel = client.post(f"/jobs/{jid}/cancel", follow_redirects=True)
        check("POST 取消后重定向展示已取消",
              r_cancel.status_code == 200 and "已取消".encode() in r_cancel.data)
        check("取消时状态已为 cancelled", status_of(jid)[0] == "cancelled")
        gate.release.set()
        t.join(30)
    check("处理线程已退出", not t.is_alive())
    check("最终状态 cancelled", status_of(jid)[0] == "cancelled")
    check("目录最终清理", not os.path.exists(job_dir(jid)))
    # 重复取消：友好提示而非报错
    r_again = client.post(f"/jobs/{jid}/cancel", follow_redirects=True)
    check("重复取消返回 200 并提示", r_again.status_code == 200 and
          "已取消".encode() in r_again.data)


def main():
    os.makedirs(webapp.JOBS_DIR, exist_ok=True)
    webapp.init_and_recover()
    for fn in (test_cancel_processing, test_cancel_queued_before_thread,
               test_cancel_after_done, test_race_cancel_wins,
               test_race_done_wins, test_restart_recovery,
               test_pipeline_job_cancelled, test_http_details):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            FAILS.append((fn.__name__, repr(exc)))
            print(f" ERROR {fn.__name__}: {exc!r}")
    wait_thread_done()
    shutil.rmtree(_tmp, ignore_errors=True)
    print(f"\n通过 {PASS} 项，失败 {len(FAILS)} 项")
    if FAILS:
        for n, d in FAILS:
            print("  -", n, d)
        sys.exit(1)
    print("全部通过")


if __name__ == "__main__":
    main()
