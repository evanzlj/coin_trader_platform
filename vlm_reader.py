#!/usr/bin/env python3
"""
vlm_reader — 无头读图出 playbook JSON,替代 vlm_worker 的浏览器层。

后端(无浏览器,无 stuck_analyzing / cookie / 1155):
  codex   GPT via codex exec(pin -m gpt-5.5,可 VLM_CODEX_MODEL 覆盖)—— ChatGPT 订阅,边际0,默认
          reasoning_effort 未显式设=跟 codex 默认;_vlm_meta 记 model_requested + 从 rollout 读回的 model_resolved/reasoning_effort(漂移可见)
  gemini  Gemini Pro(gemini-3-pro-preview)via gemini CLI —— 付费 API(宽止损/远目标风格)
  claude  Claude Opus via claude -p          —— 订阅/API(偏多/均衡)

用法:
  python3 vlm_reader.py --signal replay_materials/eth_A_20260102_0300
  python3 vlm_reader.py --signal <dir> --backend gemini
  python3 vlm_reader.py --signal <dir> --ensemble           # 三家都跑,各存一份
  # 或 import:
  from vlm_reader import vlm_read
  d = vlm_read(prompt_text, img4_path, img15_path, backend="codex")
"""
from __future__ import annotations
import argparse, json, os, re, subprocess, sys, tempfile, time
from datetime import datetime, timezone
from pathlib import Path

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

TIMEOUT = 300
GEMINI_MODEL = os.environ.get("VLM_GEMINI_MODEL", "gemini-3-pro-preview")
CODEX_MODEL  = os.environ.get("VLM_CODEX_MODEL", "gpt-5.5")   # pin,防 codex 升级偷换默认


def _codex_session_meta(since_ts: float) -> dict:
    """从本次 codex 调用产生的最新会话 rollout 读回真实 model + reasoning_effort（漂移可见）。"""
    base = Path.home() / ".codex" / "sessions"
    try:
        cands = [f for f in base.rglob("rollout-*.jsonl") if f.stat().st_mtime >= since_ts - 2]
        if not cands:
            return {}
        txt = max(cands, key=lambda f: f.stat().st_mtime).read_text(errors="ignore")
        out = {}
        m = re.search(r'"model"\s*:\s*"([^"]+)"', txt)
        if m: out["model_resolved"] = m.group(1)
        e = re.search(r'"reasoning_effort"\s*:\s*"?([A-Za-z]+)"?', txt)
        if e: out["reasoning_effort"] = e.group(1)
        return out
    except Exception:
        return {}

# ── JSON 工具(沿用 run_v7/vlm_worker)────────────────────────────────────────
def strip_fences(t: str) -> str:
    t = t.strip()
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", t, re.DOTALL)
    if m: return m.group(1).strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t); t = re.sub(r"\s*```$", "", t)
    return t.strip()

def extract_json(raw: str):
    t = strip_fences(raw or "")
    if "{" in t and "}" in t:
        t = t[t.find("{"):t.rfind("}")+1]
    for cand in (t, re.sub(r",(\s*[}\]])", r"\1", t)):
        try:
            d = json.loads(cand)
            if isinstance(d, dict): return d
        except json.JSONDecodeError:
            pass
    return None

def validate(d: dict):
    return [k for k in ("watch_summary", "playbooks") if k not in d]

def _run(cmd, cwd=None, env=None, timeout=TIMEOUT):
    try:
        r = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout)
        return r.stdout or "", r.stderr or "", r.returncode
    except subprocess.TimeoutExpired:
        return "", "timeout", 124
    except Exception as e:
        return "", str(e), 1

# ── 各后端(只负责"prompt+2图 → 模型原始文本")──────────────────────────────
def read_codex(prompt, img4, img15, model=None):
    m = model or CODEX_MODEL                            # pin gpt-5.5(不再跟 codex 默认漂移)
    fd, out = tempfile.mkstemp(suffix=".txt"); os.close(fd)
    cmd = ["codex", "exec", "--skip-git-repo-check", "-s", "read-only",
           "-i", str(img4), "-i", str(img15), "-o", out, "-m", m, prompt]
    t0 = time.time()
    so, se, rc = _run(cmd)
    txt = Path(out).read_text(encoding="utf-8", errors="ignore") if Path(out).exists() else so
    try: os.unlink(out)
    except OSError: pass
    return (txt or so), {"model_requested": m, **_codex_session_meta(t0)}

def read_gemini(prompt, img4, img15, model=None):
    img4, img15 = Path(img4), Path(img15)
    p = f"{prompt}\n\n两张信号K线图(先4H上下文,后15m详情):\n@{img4.name}\n@{img15.name}"
    env = dict(os.environ, GEMINI_CLI_TRUST_WORKSPACE="true")
    used = model or GEMINI_MODEL
    cmd = ["gemini", "-m", used, "-p", p, "--output-format", "text"]
    so, se, rc = _run(cmd, cwd=str(img4.parent), env=env)
    return so, {"model_requested": used, "model_resolved": used}

def read_claude(prompt, img4, img15, model=None):
    img4, img15 = Path(img4), Path(img15)
    p = f"{prompt}\n\n两张信号K线图(先4H上下文,后15m详情):\n@{img4.name}\n@{img15.name}"
    cmd = ["claude", "-p", p, "--dangerously-skip-permissions", "--output-format", "text"]
    if model: cmd += ["--model", model]
    so, se, rc = _run(cmd, cwd=str(img4.parent))
    return so, {"model_requested": model or "default"}

BACKENDS = {"codex": read_codex, "gemini": read_gemini, "claude": read_claude}

# ── 统一入口:返回校验过的 playbook dict(失败 None)──────────────────────────
def vlm_read(prompt, img4, img15, backend="codex", model=None, retries=1):
    fn = BACKENDS[backend]
    for _ in range(retries + 1):
        raw, meta = fn(prompt, img4, img15, model)
        d = extract_json(raw)
        if d and not validate(d):
            d["_vlm_meta"] = {"backend": backend, "recorded_at": _now_iso(), **meta}
            return d
    return None

def _materials(sig_dir: Path):
    prompt = (sig_dir / "prompt.txt").read_text(encoding="utf-8")
    img4 = sorted(sig_dir.glob("*_4h.png"))[0]
    img15 = sorted(sig_dir.glob("*_15m.png"))[0]
    return prompt, img4, img15

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signal", required=True, help="信号目录(含 prompt.txt + *_4h.png + *_15m.png)")
    ap.add_argument("--backend", default="codex", choices=list(BACKENDS))
    ap.add_argument("--model", default=None)
    ap.add_argument("--ensemble", action="store_true", help="三家都跑,各存 <backend>_response.json")
    ap.add_argument("--out", default=None, help="输出文件(默认 vlm_response.json 到信号目录)")
    args = ap.parse_args()
    sig = Path(args.signal)
    prompt, img4, img15 = _materials(sig)
    backends = list(BACKENDS) if args.ensemble else [args.backend]
    for b in backends:
        print(f"[{b}] 读图中 ...", file=sys.stderr)
        d = vlm_read(prompt, img4, img15, backend=b, model=args.model)
        if d is None:
            print(f"[{b}] 失败(无合法 JSON)", file=sys.stderr); continue
        out = Path(args.out) if (args.out and not args.ensemble) else sig / f"{b}_response.json"
        out.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[{b}] ✓ {len(d.get('playbooks',[]))} 剧本 → {out}")

if __name__ == "__main__":
    main()
