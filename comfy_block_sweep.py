"""
comfy_block_sweep.py
────────────────────
ComfyUI block sweep — randomize sliders, fixed seed, multiple batches.

Each batch has its own:
  • seed
  • image count
  • filename prefix (applied to the SaveImage node)
  • node selection (which of the 6 Flux block nodes to randomize)

Fetches slider min/max/step from /object_info at runtime so ranges
always match whatever the node files declare.

For each image queued, writes a .txt sidecar with the same base name
listing every slider value grouped by node.

Requirements:
    pip install websocket-client requests
    tkinter is included with standard Python on Windows

Place in the same folder as your workflow JSON files.
"""

import json
import os
import random
import threading
import time
import uuid
import tkinter as tk
from tkinter import ttk, scrolledtext

import requests
import websocket

# ── Paths ──────────────────────────────────────────────────────────────────────

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "block_sweep_config.json")

# ── Defaults ───────────────────────────────────────────────────────────────────

DEFAULT_SERVER = "127.0.0.1:8188"
DEFAULT_DELAY  = 45
DEFAULT_SEED   = 1
DEFAULT_COUNT  = 10
DEFAULT_PREFIX = "file/image"

# ── Node classes we care about, in display order ───────────────────────────────

NODE_CLASSES = [
    "FluxDBlockControl",
    "FluxS0S5Block",
    "FluxS6S12Block",
    "FluxS13S20Block",
    "FluxS21S28Block",
    "FluxS29S37Block",
]

# ── Config ─────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_config(data: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


# ── Workflow detection ─────────────────────────────────────────────────────────

def find_workflows() -> list:
    return sorted([
        f for f in os.listdir(SCRIPT_DIR)
        if f.endswith(".json")
        and not f.endswith("_config.json")
        and f != "config.json"
    ])


# ── /object_info fetcher ───────────────────────────────────────────────────────

def fetch_slider_ranges(server: str) -> dict:
    result = {cls: {} for cls in NODE_CLASSES}
    try:
        r = requests.get(f"http://{server}/object_info", timeout=8)
        r.raise_for_status()
        info = r.json()
        for cls in NODE_CLASSES:
            if cls not in info:
                continue
            node_info = info[cls]
            required  = node_info.get("input", {}).get("required", {})
            for param_name, param_def in required.items():
                if not isinstance(param_def, (list, tuple)) or len(param_def) < 2:
                    continue
                if param_def[0] != "FLOAT":
                    continue
                meta = param_def[1]
                if not isinstance(meta, dict):
                    continue
                if "min" in meta and "max" in meta:
                    result[cls][param_name] = {
                        "min":  float(meta["min"]),
                        "max":  float(meta["max"]),
                        "step": float(meta.get("step", 0.001)),
                    }
    except Exception as e:
        raise RuntimeError(f"Could not fetch /object_info from {server}: {e}")
    return result


def randomize_sliders(ranges: dict, active_nodes: set) -> dict:
    """
    Randomize sliders only for nodes in active_nodes.
    Returns flat dict of {slider_name: value}.
    """
    values = {}
    for cls, sliders in ranges.items():
        if cls not in active_nodes:
            continue
        for name, meta in sliders.items():
            lo      = meta["min"]
            hi      = meta["max"]
            step    = meta["step"]
            n_steps = int(round((hi - lo) / step))
            chosen  = random.randint(0, n_steps)
            val     = round(lo + chosen * step, 9)
            val     = max(lo, min(hi, val))
            values[name] = val
    return values


# ── Sidecar writer ─────────────────────────────────────────────────────────────

def write_sidecar(path: str, workflow_file: str, seed: int,
                  slider_values: dict, ranges: dict, active_nodes: set):
    lines = []
    lines.append(f"Workflow : {workflow_file}")
    lines.append(f"Seed     : {seed}")
    lines.append(f"Nodes    : {', '.join(n for n in NODE_CLASSES if n in active_nodes)}")
    lines.append("")

    for cls in NODE_CLASSES:
        if cls not in active_nodes:
            continue
        sliders = ranges.get(cls, {})
        if not sliders:
            continue
        lines.append(f"[ {cls} ]")
        for name in sliders:
            val = slider_values.get(name, 0.0)
            lines.append(f"  {name:<12}  {val:+.4f}")
        lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ── Engine ─────────────────────────────────────────────────────────────────────

class SweepEngine:
    def __init__(self, log_fn, done_fn):
        self.log      = log_fn
        self.done     = done_fn
        self.timer    = None
        self.timer_lk = threading.Lock()
        self.busy     = False
        self.running  = False
        self.ws_app   = None

        self.server        = DEFAULT_SERVER
        self.delay         = DEFAULT_DELAY
        self.workflow_file = None
        self.ranges        = {}

        # Flat queue of images to generate
        # Each entry: {seed, count, prefix, active_nodes, img_idx, img_total}
        self.queue     = []
        self.q_idx     = 0   # which batch we're in
        self.img_idx   = 0   # image within current batch

    def start(self, server, delay, workflow_file, batches):
        """
        batches: list of dicts with keys:
            seed (int), count (int), prefix (str), nodes (set of class names)
        """
        self.server        = server
        self.delay         = delay
        self.workflow_file = workflow_file
        self.running       = True
        self.busy          = False

        # Build queue entries
        self.queue   = []
        for b in batches:
            if b["count"] < 1 or not b["nodes"]:
                continue
            self.queue.append({
                "seed":   b["seed"],
                "count":  b["count"],
                "prefix": b["prefix"].strip(),
                "nodes":  set(b["nodes"]),
            })

        if not self.queue:
            self.log("❌ No valid batches configured.")
            self.done()
            return

        self.q_idx   = 0
        self.img_idx = 0

        # Fetch ranges before connecting
        try:
            self.ranges = fetch_slider_ranges(server)
        except RuntimeError as e:
            self.log(f"❌ {e}")
            self.done()
            return

        total_sliders = sum(len(v) for v in self.ranges.values())
        if total_sliders == 0:
            self.log("❌ No FLOAT sliders found in /object_info for known node classes.")
            self.done()
            return

        total_images = sum(b["count"] for b in self.queue)
        self.log("=" * 52)
        self.log("  ComfyUI Block Sweep")
        self.log(f"  Workflow : {workflow_file}")
        self.log(f"  Batches  : {len(self.queue)}  ({total_images} total images)")
        for i, b in enumerate(self.queue, 1):
            node_names = ", ".join(n.replace("Flux", "").replace("Block", "").replace("Control","D") for n in b["nodes"])
            self.log(f"  Batch {i}: seed={b['seed']}  images={b['count']}  prefix={b['prefix'] or '(workflow default)'}  nodes=[{node_names}]")
        self.log(f"  Sliders  : {total_sliders} found")
        self.log("=" * 52)

        t = threading.Thread(target=self._connect, daemon=True)
        t.start()

    def stop(self):
        self.running = False
        self._cancel_timer()
        if self.ws_app:
            try:
                self.ws_app.close()
            except Exception:
                pass
        self.log("⏹  Stopped by user.")
        self.done()

    # ── WebSocket ─────────────────────────────────────────────────────────────

    def _connect(self):
        client_id = str(uuid.uuid4())
        url = f"ws://{self.server}/ws?clientId={client_id}"
        self.ws_app = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws_app.run_forever(ping_interval=30, ping_timeout=10)

    def _on_open(self, ws):
        self.log(f"🟢 Connected to ComfyUI at {self.server}")
        self._trigger()

    def _on_message(self, ws, raw):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        msg_type = msg.get("type")
        data     = msg.get("data", {})

        if msg_type == "execution_start":
            self.log("🚀 Generation started")
            self._cancel_timer()
            self.busy = True

        elif msg_type == "status":
            qr = data.get("status", {}).get("exec_info", {}).get("queue_remaining")
            if qr == 0 and self.busy:
                self.log("✔  Generation finished")
                self.busy = False
                if self.running:
                    self._start_timer()
            elif qr is not None and qr > 0:
                self._cancel_timer()
                self.busy = True

        elif msg_type == "execution_error":
            self.log(f"⚠  Execution error — retrying in {self.delay}s")
            self.busy = False
            if self.running:
                self._start_timer()

    def _on_error(self, ws, error):
        self.log(f"WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        if not self.running:
            return
        self.log("WebSocket closed — reconnecting in 5s …")
        time.sleep(5)
        if self.running:
            self._connect()

    def _cancel_timer(self):
        with self.timer_lk:
            if self.timer:
                self.timer.cancel()
                self.timer = None

    def _start_timer(self):
        self._cancel_timer()
        with self.timer_lk:
            self.log(f"🕒 Next generation in {self.delay}s …")
            self.timer = threading.Timer(self.delay, self._trigger)
            self.timer.daemon = True
            self.timer.start()

    # ── Generation ────────────────────────────────────────────────────────────

    def _trigger(self):
        if not self.running:
            return

        # Advance to next batch if current one is done
        while self.q_idx < len(self.queue) and self.img_idx >= self.queue[self.q_idx]["count"]:
            self.q_idx  += 1
            self.img_idx = 0

        if self.q_idx >= len(self.queue):
            self.log("🏁 All batches complete.")
            self.running = False
            self.done()
            return

        batch   = self.queue[self.q_idx]
        img_num = self.img_idx + 1
        total   = batch["count"]
        self.log(f"⏱  Batch {self.q_idx + 1}  image {img_num}/{total}  seed={batch['seed']}")

        try:
            path = os.path.join(SCRIPT_DIR, self.workflow_file)
            with open(path, "r", encoding="utf-8") as f:
                workflow = json.load(f)

            slider_values = randomize_sliders(self.ranges, batch["nodes"])

            # Determine effective prefix (batch override > workflow SaveImage value)
            effective_prefix = batch["prefix"]
            if not effective_prefix:
                for node in workflow.values():
                    if node.get("class_type") in ("SaveImage", "Image Save"):
                        effective_prefix = node.get("inputs", {}).get("filename_prefix", DEFAULT_PREFIX)
                        break

            # Apply values to workflow
            for node in workflow.values():
                inputs = node.get("inputs", {})
                cls    = node.get("class_type", "")

                # Fix seed
                for key in ("seed", "noise_seed"):
                    if key in inputs and isinstance(inputs[key], int):
                        inputs[key] = batch["seed"]

                # Apply slider randomization for active nodes only
                if cls in batch["nodes"] and cls in self.ranges:
                    for name, val in slider_values.items():
                        if name in inputs:
                            inputs[name] = val

                # Override filename prefix
                if batch["prefix"] and cls in ("SaveImage", "Image Save"):
                    inputs["filename_prefix"] = batch["prefix"]

            # Queue
            client_id = str(uuid.uuid4())
            payload   = {"prompt": workflow, "client_id": client_id}
            r = requests.post(f"http://{self.server}/prompt", json=payload, timeout=10)
            r.raise_for_status()
            pid = r.json()["prompt_id"]

            # Write sidecar
            sidecar_name = f"{effective_prefix}_{img_num:05d}.txt"
            sidecar_path = os.path.join(SCRIPT_DIR, sidecar_name)
            os.makedirs(os.path.dirname(sidecar_path), exist_ok=True)
            write_sidecar(sidecar_path, self.workflow_file, batch["seed"],
                          slider_values, self.ranges, batch["nodes"])

            self.log(f"✅ Queued image {img_num}/{total}  (id={pid[:8]}…)  → {sidecar_name}")
            self.img_idx += 1

        except Exception as e:
            self.log(f"❌ {e}")


# ── UI ─────────────────────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ComfyUI Block Sweep")
        self.resizable(True, True)
        self.minsize(620, 560)
        self.configure(bg="#1e1e1e")

        self.cfg    = load_config()
        self.engine = SweepEngine(self._log, self._on_done)
        self._batch_rows = []   # list of batch row variable dicts

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        PAD  = 8
        BG   = "#1e1e1e"
        BG2  = "#2d2d2d"
        BG3  = "#252525"
        FG   = "#d4d4d4"
        ACC  = "#4ec9b0"
        FONT = ("Consolas", 9)

        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TFrame",    background=BG)
        style.configure("TLabel",    background=BG,  foreground=FG,  font=FONT)
        style.configure("TEntry",    fieldbackground=BG2, foreground=FG,
                        insertcolor=FG, font=FONT, relief="flat")
        style.configure("TScrollbar", background=BG2, troughcolor=BG, arrowcolor=FG)

        # ── Top bar ────────────────────────────────────────────────────────────
        top = ttk.Frame(self, padding=(PAD, PAD, PAD, 4))
        top.pack(fill="x")

        ttk.Label(top, text="Server:").pack(side="left")
        self.sv_server = tk.StringVar(value=self.cfg.get("server", DEFAULT_SERVER))
        ttk.Entry(top, textvariable=self.sv_server, width=22).pack(side="left", padx=(4, 12))

        ttk.Label(top, text="Delay (s):").pack(side="left")
        self.sv_delay = tk.StringVar(value=str(self.cfg.get("delay", DEFAULT_DELAY)))
        ttk.Entry(top, textvariable=self.sv_delay, width=6).pack(side="left", padx=(4, 12))

        tk.Button(top, text="⟳ Refresh", font=FONT, bg=BG2, fg=FG,
                  relief="flat", activebackground="#3a3a3a", activeforeground=FG,
                  cursor="hand2", command=self._refresh_workflows
                  ).pack(side="left", padx=(0, 6))

        self.btn_start = tk.Button(
            top, text="▶  Start", font=("Consolas", 9, "bold"),
            bg="#1a6b4a", fg="white", relief="flat",
            activebackground="#1e7d56", activeforeground="white",
            cursor="hand2", command=self._start, width=10)
        self.btn_start.pack(side="right", padx=(4, 0))

        self.btn_stop = tk.Button(
            top, text="■  Stop", font=("Consolas", 9, "bold"),
            bg="#6b1a1a", fg="white", relief="flat",
            activebackground="#7d1e1e", activeforeground="white",
            cursor="hand2", command=self._stop, width=10, state="disabled")
        self.btn_stop.pack(side="right", padx=(4, 0))

        # ── Workflow selector ──────────────────────────────────────────────────
        wf_frame = ttk.Frame(self, padding=(PAD, 4, PAD, 4))
        wf_frame.pack(fill="x")

        ttk.Label(wf_frame, text="Workflow", font=("Consolas", 9, "bold"),
                  foreground=ACC).pack(side="left")

        self.sv_workflow = tk.StringVar()
        self.wf_combo = ttk.Combobox(wf_frame, textvariable=self.sv_workflow,
                                      font=FONT, state="readonly", width=50)
        self.wf_combo.pack(side="left", padx=(8, 0))
        self._populate_workflows()

        # ── Batch list (scrollable) ────────────────────────────────────────────
        batch_outer = ttk.Frame(self, padding=(PAD, 4, PAD, 0))
        batch_outer.pack(fill="both", expand=True)

        batch_hdr = tk.Frame(batch_outer, bg="#1e1e1e")
        batch_hdr.pack(fill="x")
        ttk.Label(batch_hdr, text="Batches", font=("Consolas", 9, "bold"),
                  foreground=ACC).pack(side="left", pady=(0, 2))
        tk.Button(batch_hdr, text="+ Add Batch", font=FONT, bg=BG2, fg=FG,
                  relief="flat", activebackground="#3a3a3a", activeforeground=FG,
                  cursor="hand2", command=self._add_batch
                  ).pack(side="left", padx=(8, 0))

        canvas_frame = tk.Frame(batch_outer, bg="#1e1e1e")
        canvas_frame.pack(fill="both", expand=True)

        self.batch_canvas = tk.Canvas(canvas_frame, bg="#1e1e1e", highlightthickness=0)
        sb = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.batch_canvas.yview)
        self.batch_canvas.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.batch_canvas.pack(side="left", fill="both", expand=True)

        self.batch_inner = tk.Frame(self.batch_canvas, bg="#1e1e1e")
        self._canvas_win = self.batch_canvas.create_window((0, 0), window=self.batch_inner, anchor="nw")
        self.batch_inner.bind("<Configure>", lambda e: self.batch_canvas.configure(
            scrollregion=self.batch_canvas.bbox("all")))
        self.batch_canvas.bind("<Configure>", lambda e: self.batch_canvas.itemconfig(
            self._canvas_win, width=e.width))
        self.batch_canvas.bind_all("<MouseWheel>", lambda e: self.batch_canvas.yview_scroll(
            int(-1 * (e.delta / 120)), "units"))

        # ── Status panel ──────────────────────────────────────────────────────
        status_frame = ttk.Frame(self, padding=(PAD, 4, PAD, 4))
        status_frame.pack(fill="x")

        ttk.Label(status_frame, text="Node ranges", font=("Consolas", 9, "bold"),
                  foreground=ACC).pack(anchor="w", pady=(0, 2))

        self.status_box = tk.Text(
            status_frame, height=5, font=("Consolas", 8),
            bg=BG2, fg=FG, relief="flat", state="disabled", wrap="none")
        self.status_box.pack(fill="x")
        self.status_box.tag_config("head", foreground=ACC)
        self.status_box.tag_config("dim",  foreground="#808080")

        tk.Button(status_frame, text="Fetch ranges from ComfyUI", font=FONT,
                  bg=BG2, fg=FG, relief="flat",
                  activebackground="#3a3a3a", activeforeground=FG,
                  cursor="hand2", command=self._fetch_and_show_ranges
                  ).pack(anchor="w", pady=(4, 0))

        # ── Log ────────────────────────────────────────────────────────────────
        log_frame = ttk.Frame(self, padding=(PAD, 4, PAD, PAD))
        log_frame.pack(fill="x")

        ttk.Label(log_frame, text="Log", font=("Consolas", 9, "bold"),
                  foreground=ACC).pack(anchor="w")

        self.log_box = scrolledtext.ScrolledText(
            log_frame, height=8, font=("Consolas", 8),
            bg=BG3, fg=FG, insertbackground=FG,
            relief="flat", state="disabled", wrap="word")
        self.log_box.pack(fill="x")

        self.log_box.tag_config("green",  foreground="#4ec9b0")
        self.log_box.tag_config("yellow", foreground="#dcdcaa")
        self.log_box.tag_config("red",    foreground="#f44747")
        self.log_box.tag_config("dim",    foreground="#808080")

        # Load saved batches or start with one default
        saved_batches = self.cfg.get("batches", [])
        if saved_batches:
            for b in saved_batches:
                self._add_batch(b)
        else:
            self._add_batch()

    # ── Workflow list ──────────────────────────────────────────────────────────

    def _populate_workflows(self):
        files = find_workflows()
        self.wf_combo["values"] = files
        saved = self.cfg.get("workflow", "")
        if saved in files:
            self.sv_workflow.set(saved)
        elif files:
            self.sv_workflow.set(files[0])

    def _refresh_workflows(self):
        self._save_state()
        self._populate_workflows()
        self._log("⟳ Workflow list refreshed.")

    # ── Batch rows ────────────────────────────────────────────────────────────

    def _add_batch(self, saved: dict = None):
        BG   = "#1e1e1e"
        BG2  = "#2d2d2d"
        FG   = "#d4d4d4"
        ACC  = "#4ec9b0"
        FONT = ("Consolas", 9)

        if saved is None:
            saved = {}

        idx   = len(self._batch_rows) + 1
        frame = tk.Frame(self.batch_inner, bg=BG2, pady=4, padx=6,
                         highlightthickness=1, highlightbackground="#3a3a3a")
        frame.pack(fill="x", pady=3)

        # ── Header row: label + remove button ─────────────────────────────────
        hdr = tk.Frame(frame, bg=BG2)
        hdr.pack(fill="x")
        lbl = tk.Label(hdr, text=f"Batch {idx}", bg=BG2, fg=ACC,
                 font=("Consolas", 9, "bold"))
        lbl.pack(side="left")

        # remove button wired after row dict is built
        remove_btn = tk.Button(hdr, text="✕ Remove", font=FONT, bg=BG2, fg="#f44747",
                  relief="flat", activebackground=BG2, activeforeground="#ff6666",
                  cursor="hand2")
        remove_btn.pack(side="right")

        # ── Fields row: seed, images, prefix ──────────────────────────────────
        fields = tk.Frame(frame, bg=BG2)
        fields.pack(fill="x", pady=(2, 2))

        tk.Label(fields, text="Seed:", bg=BG2, fg=FG, font=FONT).pack(side="left")
        sv_seed = tk.StringVar(value=str(saved.get("seed", DEFAULT_SEED)))
        tk.Entry(fields, textvariable=sv_seed, width=10,
                 bg=BG, fg=FG, insertbackground=FG,
                 relief="flat", font=FONT).pack(side="left", padx=(4, 12))

        tk.Label(fields, text="Images:", bg=BG2, fg=FG, font=FONT).pack(side="left")
        sv_count = tk.StringVar(value=str(saved.get("count", DEFAULT_COUNT)))
        tk.Entry(fields, textvariable=sv_count, width=6,
                 bg=BG, fg=FG, insertbackground=FG,
                 relief="flat", font=FONT).pack(side="left", padx=(4, 12))

        tk.Label(fields, text="Prefix:", bg=BG2, fg=FG, font=FONT).pack(side="left")
        sv_prefix = tk.StringVar(value=saved.get("prefix", DEFAULT_PREFIX))
        tk.Entry(fields, textvariable=sv_prefix, width=22,
                 bg=BG, fg=FG, insertbackground=FG,
                 relief="flat", font=FONT).pack(side="left", padx=(4, 0))

        # ── Node checkboxes ───────────────────────────────────────────────────
        nodes_frame = tk.Frame(frame, bg=BG2)
        nodes_frame.pack(fill="x", pady=(2, 0))

        tk.Label(nodes_frame, text="Nodes:", bg=BG2, fg="#808080",
                 font=FONT).pack(side="left")

        saved_nodes = set(saved.get("nodes", NODE_CLASSES))  # default all on
        node_vars = {}
        for cls in NODE_CLASSES:
            short = cls.replace("Flux", "").replace("BlockControl", "D").replace("Block", "")
            var   = tk.BooleanVar(value=(cls in saved_nodes))
            tk.Checkbutton(nodes_frame, variable=var, text=short,
                           bg=BG2, fg=FG, activebackground=BG2, activeforeground=FG,
                           selectcolor=BG, font=FONT, relief="flat"
                           ).pack(side="left", padx=(4, 0))
            node_vars[cls] = var

        row = {
            "frame":      frame,
            "sv_seed":    sv_seed,
            "sv_count":   sv_count,
            "sv_prefix":  sv_prefix,
            "node_vars":  node_vars,
            "label":      lbl,
        }
        remove_btn.config(command=lambda r=row: self._remove_batch(r))

        self._batch_rows.append(row)

    def _remove_batch(self, row):
        row["frame"].destroy()
        if row in self._batch_rows:
            self._batch_rows.remove(row)
        self._renumber_batches()

    def _renumber_batches(self):
        for i, row in enumerate(self._batch_rows, 1):
            row["label"].config(text=f"Batch {i}")

    # ── Range display ──────────────────────────────────────────────────────────

    def _fetch_and_show_ranges(self):
        server = self.sv_server.get()
        self._log(f"Fetching /object_info from {server} …")

        def _fetch():
            try:
                ranges = fetch_slider_ranges(server)
                self.after(0, lambda: self._show_ranges(ranges))
            except RuntimeError as e:
                self.after(0, lambda: self._log(f"❌ {e}"))

        threading.Thread(target=_fetch, daemon=True).start()

    def _show_ranges(self, ranges: dict):
        self.status_box.config(state="normal")
        self.status_box.delete("1.0", "end")
        any_found = False
        for cls in NODE_CLASSES:
            sliders = ranges.get(cls, {})
            if not sliders:
                continue
            any_found = True
            self.status_box.insert("end", f"{cls}\n", "head")
            for name, meta in sliders.items():
                line = f"  {name:<12}  min {meta['min']:+.4f}  max {meta['max']:+.4f}  step {meta['step']}\n"
                self.status_box.insert("end", line, "dim")
        if not any_found:
            self.status_box.insert("end", "No matching node classes found in /object_info.\n", "dim")
        self.status_box.config(state="disabled")
        self._log("✔  Ranges loaded.")

    # ── State ─────────────────────────────────────────────────────────────────

    def _collect_state(self) -> dict:
        batches = []
        for row in self._batch_rows:
            try:
                seed  = int(row["sv_seed"].get())
                count = int(row["sv_count"].get())
            except ValueError:
                seed, count = DEFAULT_SEED, DEFAULT_COUNT
            nodes = [cls for cls, var in row["node_vars"].items() if var.get()]
            batches.append({
                "seed":   seed,
                "count":  count,
                "prefix": row["sv_prefix"].get().strip(),
                "nodes":  nodes,
            })
        return {
            "server":   self.sv_server.get(),
            "delay":    self._int(self.sv_delay, DEFAULT_DELAY),
            "workflow": self.sv_workflow.get(),
            "batches":  batches,
        }

    def _save_state(self):
        state = self._collect_state()
        save_config(state)
        self.cfg = state

    # ── Actions ───────────────────────────────────────────────────────────────

    def _start(self):
        self._save_state()

        workflow = self.sv_workflow.get()
        if not workflow:
            self._log("❌ No workflow selected.")
            return

        try:
            delay = int(self.sv_delay.get())
        except ValueError:
            self._log("❌ Invalid delay value.")
            return

        state   = self._collect_state()
        batches = state["batches"]

        if not batches:
            self._log("❌ No batches configured.")
            return

        for i, b in enumerate(batches, 1):
            if b["count"] < 1:
                self._log(f"❌ Batch {i}: images must be at least 1.")
                return
            if not b["nodes"]:
                self._log(f"❌ Batch {i}: select at least one node.")
                return

        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")

        self.engine.start(
            server        = state["server"],
            delay         = delay,
            workflow_file = workflow,
            batches       = batches,
        )

    def _stop(self):
        self.engine.stop()

    def _on_done(self):
        self.after(0, lambda: self.btn_start.config(state="normal"))
        self.after(0, lambda: self.btn_stop.config(state="disabled"))

    def _on_close(self):
        self._save_state()
        self.engine.stop()
        self.destroy()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _int(self, sv, default):
        try:
            return int(sv.get())
        except ValueError:
            return default

    def _log(self, text: str):
        def _write():
            self.log_box.config(state="normal")
            tag = "dim"
            if any(c in text for c in ("🟢", "✅", "✔", "🏁")):
                tag = "green"
            elif any(c in text for c in ("⚠", "❌", "ERROR")):
                tag = "red"
            elif any(c in text for c in ("🚀", "⏱", "▶", "🕒")):
                tag = "yellow"
            self.log_box.insert("end", text + "\n", tag)
            self.log_box.see("end")
            self.log_box.config(state="disabled")
        self.after(0, _write)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.mainloop()
