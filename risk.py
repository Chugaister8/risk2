import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sqlite3
import csv
import math
import json
import os
from datetime import datetime, date
from typing import Optional, List, Dict, Any

# ═══════════════════════════════════════════════════════════════
# DATABASE LAYER
# ═══════════════════════════════════════════════════════════════

DB_PATH = "risk_registry_v2.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS risks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            risk_code   TEXT NOT NULL UNIQUE,
            title       TEXT NOT NULL,
            category    TEXT NOT NULL,
            description TEXT,
            probability TEXT NOT NULL,
            impact      TEXT NOT NULL,
            risk_level  TEXT GENERATED ALWAYS AS (
                CASE
                    WHEN probability='Висока'  AND impact='Критичний' THEN 'Критичний'
                    WHEN probability='Висока'  AND impact='Високий'   THEN 'Критичний'
                    WHEN probability='Середня' AND impact='Критичний' THEN 'Критичний'
                    WHEN probability='Висока'  AND impact='Середній'  THEN 'Високий'
                    WHEN probability='Середня' AND impact='Високий'   THEN 'Високий'
                    WHEN probability='Низька'  AND impact='Критичний' THEN 'Високий'
                    WHEN probability='Середня' AND impact='Середній'  THEN 'Середній'
                    WHEN probability='Низька'  AND impact='Високий'   THEN 'Середній'
                    WHEN probability='Висока'  AND impact='Низький'   THEN 'Середній'
                    ELSE 'Низький'
                END
            ) STORED,
            owner       TEXT,
            department  TEXT,
            tags        TEXT DEFAULT '',
            residual_probability TEXT,
            residual_impact      TEXT,
            review_date TEXT,
            status      TEXT NOT NULL DEFAULT 'Активний',
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            updated_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS measures (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            risk_id       INTEGER NOT NULL REFERENCES risks(id) ON DELETE CASCADE,
            measure_code  TEXT NOT NULL,
            title         TEXT NOT NULL,
            description   TEXT,
            type          TEXT NOT NULL,
            responsible   TEXT,
            deadline      TEXT,
            status        TEXT NOT NULL DEFAULT 'Заплановано',
            effectiveness TEXT,
            cost_estimate REAL,
            progress      INTEGER DEFAULT 0,
            notes         TEXT,
            created_at    TEXT DEFAULT (datetime('now','localtime')),
            updated_at    TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            action     TEXT NOT NULL,
            entity     TEXT NOT NULL,
            entity_id  INTEGER,
            details    TEXT,
            user_name  TEXT DEFAULT 'System',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS risk_comments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            risk_id    INTEGER NOT NULL REFERENCES risks(id) ON DELETE CASCADE,
            author     TEXT DEFAULT 'Аналітик',
            text       TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS app_settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        INSERT OR IGNORE INTO app_settings (key, value) VALUES ('theme', 'dark');
        INSERT OR IGNORE INTO app_settings (key, value) VALUES ('user_name', 'Аналітик');
        INSERT OR IGNORE INTO app_settings (key, value) VALUES ('auto_refresh', '30');
    """)
    conn.commit()
    conn.close()

def log_action(action: str, entity: str, entity_id: int = None,
               details: str = "", user: str = "Аналітик"):
    conn = get_connection()
    conn.execute(
        "INSERT INTO audit_log (action, entity, entity_id, details, user_name) "
        "VALUES (?,?,?,?,?)",
        (action, entity, entity_id, details, user)
    )
    conn.commit()
    conn.close()

def get_setting(key: str, default: str = "") -> str:
    conn = get_connection()
    row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default

def set_setting(key: str, value: str):
    conn = get_connection()
    conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES (?,?)",
                 (key, value))
    conn.commit()
    conn.close()

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

CATEGORIES    = ["Операційний", "Фінансовий", "Стратегічний", "Комплаєнс",
                 "Репутаційний", "IT/Кібер", "Кадровий", "Екологічний", "Правовий", "Інший"]
PROBABILITIES = ["Висока", "Середня", "Низька"]
IMPACTS       = ["Критичний", "Високий", "Середній", "Низький"]
RISK_STATUSES = ["Активний", "Прийнятий", "Закритий", "Призупинено", "На перегляді"]
MEASURE_TYPES = ["Уникнення", "Зменшення", "Передача", "Прийняття", "Контроль", "Моніторинг"]
MEASURE_STATUSES = ["Заплановано", "В процесі", "Виконано", "Скасовано", "Відкладено"]
EFFECTIVENESS = ["Висока", "Середня", "Низька", "Не оцінено"]
DEPARTMENTS   = ["ІТ відділ", "Фінанси", "Операції", "HR", "Юридичний",
                 "Безпека", "Маркетинг", "Виробництво", "Інший"]

LEVEL_COLORS = {
    "Критичний": "#ef4444",
    "Високий":   "#f97316",
    "Середній":  "#eab308",
    "Низький":   "#22c55e",
}

LEVEL_BG = {
    "Критичний": "#2d1515",
    "Високий":   "#2d1f10",
    "Середній":  "#2a2310",
    "Низький":   "#0f2a15",
}

# ═══════════════════════════════════════════════════════════════
# THEME SYSTEM — DARK / LIGHT
# ═══════════════════════════════════════════════════════════════

THEMES = {
    "dark": {
        "BG_DARK":  "#0b0d14",
        "BG_CARD":  "#141720",
        "BG_INPUT": "#1e2130",
        "ACCENT":   "#6366f1",
        "ACCENT2":  "#818cf8",
        "ACCENT3":  "#a5b4fc",
        "TEXT_PRI": "#f1f5f9",
        "TEXT_SEC": "#64748b",
        "TEXT_MUT": "#334155",
        "BORDER":   "#1e2440",
        "SUCCESS":  "#22c55e",
        "WARNING":  "#f59e0b",
        "DANGER":   "#ef4444",
        "INFO":     "#06b6d4",
        "HOVER":    "#1a1f35",
        "SEL":      "#6366f1",
    },
    "light": {
        "BG_DARK":  "#f0f4f8",
        "BG_CARD":  "#ffffff",
        "BG_INPUT": "#f8fafc",
        "ACCENT":   "#4f46e5",
        "ACCENT2":  "#6366f1",
        "ACCENT3":  "#818cf8",
        "TEXT_PRI": "#0f172a",
        "TEXT_SEC": "#475569",
        "TEXT_MUT": "#cbd5e1",
        "BORDER":   "#e2e8f0",
        "SUCCESS":  "#16a34a",
        "WARNING":  "#d97706",
        "DANGER":   "#dc2626",
        "INFO":     "#0891b2",
        "HOVER":    "#eef2ff",
        "SEL":      "#4f46e5",
    }
}

T = THEMES[get_setting("theme", "dark")]

def reload_theme():
    global T
    T = THEMES[get_setting("theme", "dark")]

def apply_theme(root):
    reload_theme()
    style = ttk.Style(root)
    style.theme_use("clam")
    _configure_styles(style)
    root.configure(bg=T["BG_DARK"])
    return style

def _configure_styles(style):
    style.configure(".",
        background=T["BG_DARK"], foreground=T["TEXT_PRI"],
        font=("Segoe UI", 10), borderwidth=0, relief="flat")

    style.configure("TFrame", background=T["BG_DARK"])
    style.configure("Card.TFrame", background=T["BG_CARD"])
    style.configure("Input.TFrame", background=T["BG_INPUT"])
    style.configure("Sidebar.TFrame", background=T["BG_CARD"])
    style.configure("Toolbar.TFrame", background=T["BG_DARK"])

    style.configure("TLabel",
        background=T["BG_DARK"], foreground=T["TEXT_PRI"],
        font=("Segoe UI", 10))
    style.configure("Card.TLabel",
        background=T["BG_CARD"], foreground=T["TEXT_PRI"],
        font=("Segoe UI", 10))
    style.configure("Title.TLabel",
        background=T["BG_DARK"], foreground=T["TEXT_PRI"],
        font=("Segoe UI", 20, "bold"))
    style.configure("Subtitle.TLabel",
        background=T["BG_DARK"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 10))
    style.configure("CardTitle.TLabel",
        background=T["BG_CARD"], foreground=T["TEXT_PRI"],
        font=("Segoe UI", 12, "bold"))
    style.configure("SectionTitle.Card.TLabel",
        background=T["BG_CARD"], foreground=T["ACCENT2"],
        font=("Segoe UI", 10, "bold"))
    style.configure("Stat.TLabel",
        background=T["BG_CARD"], foreground=T["TEXT_PRI"],
        font=("Segoe UI", 30, "bold"))
    style.configure("StatSub.TLabel",
        background=T["BG_CARD"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 9))
    style.configure("Tag.TLabel",
        background=T["ACCENT"], foreground="white",
        font=("Segoe UI", 8, "bold"), padding=(6, 2))
    style.configure("Badge.TLabel",
        background=T["BG_INPUT"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 8), padding=(4, 1))
    style.configure("Muted.TLabel",
        background=T["BG_DARK"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 9))
    style.configure("Card.Muted.TLabel",
        background=T["BG_CARD"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 9))
    style.configure("Link.TLabel",
        background=T["BG_DARK"], foreground=T["ACCENT2"],
        font=("Segoe UI", 10, "underline"))
    style.configure("StatusBar.TLabel",
        background=T["BG_CARD"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 9), padding=(8, 4))

    style.configure("Accent.TButton",
        background=T["ACCENT"], foreground="white",
        font=("Segoe UI", 10, "bold"), borderwidth=0,
        focusthickness=0, padding=(16, 9), relief="flat")
    style.map("Accent.TButton",
        background=[("active", T["ACCENT2"]), ("pressed", "#4338ca")])

    style.configure("Success.TButton",
        background=T["SUCCESS"], foreground="white",
        font=("Segoe UI", 10), borderwidth=0, padding=(14, 8))
    style.map("Success.TButton",
        background=[("active", "#16a34a")])

    style.configure("Danger.TButton",
        background=T["DANGER"], foreground="white",
        font=("Segoe UI", 10), borderwidth=0, padding=(14, 8))
    style.map("Danger.TButton",
        background=[("active", "#b91c1c")])

    style.configure("Ghost.TButton",
        background=T["BG_INPUT"], foreground=T["TEXT_PRI"],
        font=("Segoe UI", 10), borderwidth=0, padding=(14, 8))
    style.map("Ghost.TButton",
        background=[("active", T["BORDER"]), ("pressed", T["BG_INPUT"])])

    style.configure("Small.Accent.TButton",
        background=T["ACCENT"], foreground="white",
        font=("Segoe UI", 9), borderwidth=0, padding=(10, 5))
    style.map("Small.Accent.TButton",
        background=[("active", T["ACCENT2"])])

    style.configure("Small.Ghost.TButton",
        background=T["BG_INPUT"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 9), borderwidth=0, padding=(8, 4))
    style.map("Small.Ghost.TButton",
        background=[("active", T["BORDER"])])

    style.configure("Small.Danger.TButton",
        background=T["DANGER"], foreground="white",
        font=("Segoe UI", 9), borderwidth=0, padding=(8, 4))
    style.map("Small.Danger.TButton",
        background=[("active", "#b91c1c")])

    style.configure("TEntry",
        fieldbackground=T["BG_INPUT"], foreground=T["TEXT_PRI"],
        insertcolor=T["TEXT_PRI"], bordercolor=T["BORDER"],
        lightcolor=T["BORDER"], darkcolor=T["BORDER"],
        borderwidth=1, relief="flat", padding=9)
    style.map("TEntry",
        bordercolor=[("focus", T["ACCENT"])],
        fieldbackground=[("disabled", T["BG_CARD"])])

    style.configure("TCombobox",
        fieldbackground=T["BG_INPUT"], foreground=T["TEXT_PRI"],
        background=T["BG_INPUT"], arrowcolor=T["TEXT_SEC"],
        bordercolor=T["BORDER"], lightcolor=T["BORDER"],
        darkcolor=T["BORDER"], padding=9)
    style.map("TCombobox",
        fieldbackground=[("readonly", T["BG_INPUT"]), ("disabled", T["BG_CARD"])],
        bordercolor=[("focus", T["ACCENT"])])

    style.configure("TNotebook",
        background=T["BG_DARK"], borderwidth=0,
        tabmargins=[0, 0, 0, 0])
    style.configure("TNotebook.Tab",
        background=T["BG_CARD"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 10), padding=(20, 10), borderwidth=0)
    style.map("TNotebook.Tab",
        background=[("selected", T["ACCENT"]), ("active", T["BG_INPUT"])],
        foreground=[("selected", "white"), ("active", T["TEXT_PRI"])])

    style.configure("Treeview",
        background=T["BG_CARD"], foreground=T["TEXT_PRI"],
        fieldbackground=T["BG_CARD"], borderwidth=0,
        rowheight=34, font=("Segoe UI", 9))
    style.configure("Treeview.Heading",
        background=T["BG_INPUT"], foreground=T["TEXT_SEC"],
        font=("Segoe UI", 9, "bold"), borderwidth=0, relief="flat")
    style.map("Treeview",
        background=[("selected", T["ACCENT"])],
        foreground=[("selected", "white")])

    style.configure("Vertical.TScrollbar",
        background=T["BG_INPUT"], troughcolor=T["BG_DARK"],
        borderwidth=0, arrowcolor=T["TEXT_SEC"], width=8)
    style.configure("Horizontal.TScrollbar",
        background=T["BG_INPUT"], troughcolor=T["BG_DARK"],
        borderwidth=0, arrowcolor=T["TEXT_SEC"])

    style.configure("TSeparator", background=T["BORDER"])
    style.configure("TSpinbox",
        fieldbackground=T["BG_INPUT"], foreground=T["TEXT_PRI"],
        background=T["BG_INPUT"], bordercolor=T["BORDER"],
        arrowcolor=T["TEXT_SEC"])
    style.configure("TScale",
        background=T["BG_CARD"], troughcolor=T["BG_INPUT"],
        borderwidth=0)

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def make_scrollable(parent, orient="both", bg=None):
    reload_theme()
    bg = bg or T["BG_DARK"]
    outer = ttk.Frame(parent, style="TFrame")
    outer.grid_rowconfigure(0, weight=1)
    outer.grid_columnconfigure(0, weight=1)

    canvas = tk.Canvas(outer, bg=bg, highlightthickness=0, bd=0)
    canvas.grid(row=0, column=0, sticky="nsew")

    if orient in ("both", "vertical"):
        vsb = ttk.Scrollbar(outer, orient="vertical",
                            command=canvas.yview,
                            style="Vertical.TScrollbar")
        vsb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)
    if orient in ("both", "horizontal"):
        hsb = ttk.Scrollbar(outer, orient="horizontal",
                            command=canvas.xview,
                            style="Horizontal.TScrollbar")
        hsb.grid(row=1, column=0, sticky="ew")
        canvas.configure(xscrollcommand=hsb.set)

    inner = ttk.Frame(canvas, style="TFrame")
    win = canvas.create_window((0, 0), window=inner, anchor="nw")

    def _on_configure(e):
        canvas.configure(scrollregion=canvas.bbox("all"))
    def _on_canvas(e):
        canvas.itemconfig(win, width=e.width)

    inner.bind("<Configure>", _on_configure)
    canvas.bind("<Configure>", _on_canvas)

    def _mousewheel(e):
        canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
    canvas.bind("<MouseWheel>", _mousewheel)
    inner.bind("<MouseWheel>", _mousewheel)

    return outer, inner

def lbl_entry(parent, row, col, text, var=None,
              colspan=1, required=False, bg="card", width=None):
    bg_key = "Card.TLabel" if bg == "card" else "TLabel"
    txt = (text + "  *") if required else text
    tk.Label(parent, text=txt,
             bg=T["BG_CARD"] if bg == "card" else T["BG_DARK"],
             fg=T["TEXT_SEC"],
             font=("Segoe UI", 9)).grid(
        row=row, column=col, sticky="w", padx=(0, 8), pady=(0, 2))
    if var is not None:
        kw = {"textvariable": var, "style": "TEntry"}
        if width:
            kw["width"] = width
        ent = ttk.Entry(parent, **kw)
        ent.grid(row=row + 1, column=col, columnspan=colspan,
                 sticky="ew", padx=(0, 8), pady=(0, 12))
        return ent
    return None

def lbl_combo(parent, row, col, text, var, values,
              colspan=1, bg="card"):
    tk.Label(parent, text=text,
             bg=T["BG_CARD"] if bg == "card" else T["BG_DARK"],
             fg=T["TEXT_SEC"],
             font=("Segoe UI", 9)).grid(
        row=row, column=col, sticky="w", padx=(0, 8), pady=(0, 2))
    cb = ttk.Combobox(parent, textvariable=var, values=values,
                      state="readonly", style="TCombobox")
    cb.grid(row=row + 1, column=col, columnspan=colspan,
            sticky="ew", padx=(0, 8), pady=(0, 12))
    return cb

def lbl_textarea(parent, row, col, text, height=4, colspan=1, bg="card"):
    tk.Label(parent, text=text,
             bg=T["BG_CARD"] if bg == "card" else T["BG_DARK"],
             fg=T["TEXT_SEC"],
             font=("Segoe UI", 9)).grid(
        row=row, column=col, sticky="w", padx=(0, 8), pady=(0, 2))
    fr = ttk.Frame(parent, style="Card.TFrame" if bg == "card" else "TFrame")
    fr.grid(row=row + 1, column=col, columnspan=colspan,
            sticky="ew", padx=(0, 8), pady=(0, 12))
    fr.grid_columnconfigure(0, weight=1)
    ta = tk.Text(fr, height=height,
                 bg=T["BG_INPUT"], fg=T["TEXT_PRI"],
                 insertbackground=T["TEXT_PRI"],
                 bd=0, padx=10, pady=8,
                 font=("Segoe UI", 10), relief="flat",
                 wrap="word", highlightthickness=1,
                 highlightbackground=T["BORDER"],
                 highlightcolor=T["ACCENT"])
    ta.grid(row=0, column=0, sticky="ew")
    return ta

def next_code(prefix, table):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()[0] + 1
    conn.close()
    return f"{prefix}-{n:04d}"

def format_date(dt_str: str) -> str:
    if not dt_str:
        return ""
    try:
        return dt_str[:10]
    except Exception:
        return dt_str

def is_overdue(deadline_str: str) -> bool:
    if not deadline_str:
        return False
    try:
        d = datetime.strptime(deadline_str[:10], "%Y-%m-%d").date()
        return d < date.today()
    except Exception:
        return False

def calc_level(prob: str, imp: str) -> str:
    matrix = {
        ("Висока", "Критичний"): "Критичний",
        ("Висока", "Високий"):   "Критичний",
        ("Середня","Критичний"): "Критичний",
        ("Висока", "Середній"):  "Високий",
        ("Середня","Високий"):   "Високий",
        ("Низька", "Критичний"): "Високий",
        ("Середня","Середній"):  "Середній",
        ("Низька", "Високий"):   "Середній",
        ("Висока", "Низький"):   "Середній",
    }
    return matrix.get((prob, imp), "Низький")

# ═══════════════════════════════════════════════════════════════
# TOAST NOTIFICATION
# ═══════════════════════════════════════════════════════════════

class ToastManager:
    _queue: List = []
    _root = None

    @classmethod
    def init(cls, root):
        cls._root = root

    @classmethod
    def show(cls, message: str, kind: str = "info", duration: int = 3000):
        if not cls._root:
            return
        colors = {
            "success": (T["SUCCESS"], "#0f2a15"),
            "error":   (T["DANGER"],  "#2d1515"),
            "warning": (T["WARNING"], "#2a1f0a"),
            "info":    (T["INFO"],    "#0a2030"),
        }
        fg, bg = colors.get(kind, (T["TEXT_PRI"], T["BG_CARD"]))
        icons  = {"success": "OK", "error": "ERR", "warning": "!", "info": "i"}
        icon   = icons.get(kind, "i")

        toast = tk.Toplevel(cls._root)
        toast.overrideredirect(True)
        toast.attributes("-topmost", True)
        toast.configure(bg=bg)

        frame = tk.Frame(toast, bg=bg, padx=16, pady=10)
        frame.pack()
        tk.Label(frame, text=f"[{icon}]", bg=bg, fg=fg,
                 font=("Segoe UI", 11, "bold")).pack(side="left", padx=(0, 8))
        tk.Label(frame, text=message, bg=bg, fg=T["TEXT_PRI"],
                 font=("Segoe UI", 10), wraplength=300).pack(side="left")

        cls._root.update_idletasks()
        rw = cls._root.winfo_x() + cls._root.winfo_width()
        rh = cls._root.winfo_y() + cls._root.winfo_height()
        tw = toast.winfo_reqwidth()
        th = toast.winfo_reqheight()
        offset = len(cls._queue) * (th + 8)
        toast.geometry(f"+{rw - tw - 20}+{rh - th - 20 - offset}")

        cls._queue.append(toast)

        def _remove():
            try:
                if toast in cls._queue:
                    cls._queue.remove(toast)
                toast.destroy()
            except Exception:
                pass

        toast.after(duration, _remove)


# ═══════════════════════════════════════════════════════════════
# STAT CARD WIDGET
# ═══════════════════════════════════════════════════════════════

class StatCard(tk.Frame):
    def __init__(self, parent, title, value, color=None, subtitle="",
                 trend=None, **kwargs):
        color = color or T["ACCENT"]
        super().__init__(parent, bg=T["BG_CARD"], **kwargs)
        self.configure(pady=0)

        # Top accent line
        accent = tk.Frame(self, bg=color, height=3)
        accent.pack(fill="x")

        body = tk.Frame(self, bg=T["BG_CARD"], padx=20, pady=18)
        body.pack(fill="both", expand=True)

        # Value
        val_frame = tk.Frame(body, bg=T["BG_CARD"])
        val_frame.pack(anchor="w", fill="x")
        tk.Label(val_frame, text=str(value),
                 bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 32, "bold")).pack(side="left")

        if trend is not None:
            trend_color = T["SUCCESS"] if trend >= 0 else T["DANGER"]
            trend_sym = "+" if trend > 0 else ""
            tk.Label(val_frame, text=f"  {trend_sym}{trend}",
                     bg=T["BG_CARD"], fg=trend_color,
                     font=("Segoe UI", 12, "bold")).pack(side="left", pady=(12, 0))

        tk.Label(body, text=title, bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 10, "bold")).pack(anchor="w")
        if subtitle:
            tk.Label(body, text=subtitle, bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 8)).pack(anchor="w")

        # Hover effect
        self.bind("<Enter>", lambda e: self.configure(
            highlightbackground=color, highlightthickness=1))
        self.bind("<Leave>", lambda e: self.configure(
            highlightthickness=0))

# ═══════════════════════════════════════════════════════════════
# RISK DETAIL DIALOG
# ═══════════════════════════════════════════════════════════════

class RiskDetailDialog(tk.Toplevel):
    def __init__(self, parent, risk_id: int, app):
        super().__init__(parent)
        self.app = app
        self.risk_id = risk_id
        self.title("Деталі ризику")
        self.geometry("860x640")
        self.configure(bg=T["BG_DARK"])
        self.transient(parent)

        self._build()
        self._load()

    def _build(self):
        nb = ttk.Notebook(self, style="TNotebook")
        nb.pack(fill="both", expand=True, padx=16, pady=16)

        self.tab_info    = ttk.Frame(nb, style="TFrame")
        self.tab_measures= ttk.Frame(nb, style="TFrame")
        self.tab_comments= ttk.Frame(nb, style="TFrame")
        self.tab_history = ttk.Frame(nb, style="TFrame")

        nb.add(self.tab_info,     text="  Загальне  ")
        nb.add(self.tab_measures, text="  Заходи  ")
        nb.add(self.tab_comments, text="  Коментарі  ")
        nb.add(self.tab_history,  text="  Аудит  ")

        # Buttons
        btn_f = tk.Frame(self, bg=T["BG_DARK"], pady=12, padx=16)
        btn_f.pack(fill="x")
        ttk.Button(btn_f, text="Редагувати",
                   style="Accent.TButton",
                   command=self._edit).pack(side="left", padx=(0, 8))
        ttk.Button(btn_f, text="Додати захід",
                   style="Success.TButton",
                   command=self._add_measure).pack(side="left", padx=(0, 8))
        ttk.Button(btn_f, text="Закрити",
                   style="Ghost.TButton",
                   command=self.destroy).pack(side="right")

    def _load(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM risks WHERE id=?", (self.risk_id,))
        r = cur.fetchone()
        if not r:
            conn.close()
            self.destroy()
            return

        # INFO TAB
        for w in self.tab_info.winfo_children():
            w.destroy()
        so, si = make_scrollable(self.tab_info, orient="vertical")
        so.pack(fill="both", expand=True)
        si.grid_columnconfigure(0, weight=1)

        card = tk.Frame(si, bg=T["BG_CARD"], padx=24, pady=20)
        card.grid(sticky="ew", pady=(0, 12))
        card.grid_columnconfigure((0, 1, 2), weight=1)

        # Level badge
        lvl = r["risk_level"]
        lvl_color = LEVEL_COLORS.get(lvl, T["ACCENT"])
        badge_frame = tk.Frame(card, bg=lvl_color, padx=10, pady=4)
        badge_frame.grid(row=0, column=0, sticky="w", pady=(0, 12))
        tk.Label(badge_frame, text=lvl, bg=lvl_color, fg="white",
                 font=("Segoe UI", 11, "bold")).pack()

        tk.Label(card, text=r["title"], bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 16, "bold"), wraplength=700, justify="left").grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(0, 16))

        fields = [
            ("Код", r["risk_code"]),        ("Категорія", r["category"]),
            ("Ймовірність", r["probability"]), ("Вплив", r["impact"]),
            ("Власник", r["owner"] or "—"),    ("Відділ", r["department"] or "—"),
            ("Статус", r["status"]),           ("Дата огляду", format_date(r["review_date"]) or "—"),
            ("Створено", format_date(r["created_at"])), ("Оновлено", format_date(r["updated_at"])),
        ]
        for i, (label, value) in enumerate(fields):
            col = i % 3
            row_num = (i // 3) * 2 + 2
            tk.Label(card, text=label, bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 8)).grid(row=row_num, column=col, sticky="w", padx=(0, 16))
            tk.Label(card, text=value, bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                     font=("Segoe UI", 10, "bold")).grid(row=row_num + 1, column=col, sticky="w",
                                                          padx=(0, 16), pady=(0, 10))

        if r["description"]:
            desc_card = tk.Frame(si, bg=T["BG_CARD"], padx=24, pady=16)
            desc_card.grid(sticky="ew")
            tk.Label(desc_card, text="Опис", bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 9, "bold")).pack(anchor="w")
            tk.Label(desc_card, text=r["description"], bg=T["BG_CARD"],
                     fg=T["TEXT_PRI"], font=("Segoe UI", 10),
                     wraplength=700, justify="left").pack(anchor="w", pady=(6, 0))

        # MEASURES TAB
        for w in self.tab_measures.winfo_children():
            w.destroy()
        cur.execute("""
            SELECT measure_code, title, type, responsible, deadline, status, effectiveness, progress
            FROM measures WHERE risk_id=? ORDER BY created_at
        """, (self.risk_id,))
        measures = cur.fetchall()

        ms_so, ms_si = make_scrollable(self.tab_measures, orient="vertical")
        ms_so.pack(fill="both", expand=True)
        ms_si.grid_columnconfigure(0, weight=1)

        if not measures:
            tk.Label(ms_si, text="Заходів не знайдено",
                     bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 11)).grid(row=0, column=0, pady=40)
        else:
            for idx, m in enumerate(measures):
                mf = tk.Frame(ms_si, bg=T["BG_CARD"], padx=16, pady=12)
                mf.grid(row=idx, column=0, sticky="ew", pady=(0, 6))
                mf.grid_columnconfigure(1, weight=1)

                st_color = {
                    "Виконано": T["SUCCESS"], "В процесі": T["WARNING"],
                    "Скасовано": T["DANGER"], "Заплановано": T["ACCENT2"],
                    "Відкладено": T["TEXT_SEC"]
                }.get(m["status"], T["TEXT_SEC"])

                tk.Label(mf, text=m["measure_code"],
                         bg=T["ACCENT"], fg="white",
                         font=("Segoe UI", 8, "bold"), padx=6, pady=2).grid(
                    row=0, column=0, sticky="nw", padx=(0, 10))
                tk.Label(mf, text=m["title"], bg=T["BG_CARD"],
                         fg=T["TEXT_PRI"], font=("Segoe UI", 10, "bold")).grid(
                    row=0, column=1, sticky="w")
                tk.Label(mf, text=m["status"], bg=T["BG_CARD"],
                         fg=st_color, font=("Segoe UI", 9, "bold")).grid(
                    row=0, column=2, sticky="e")

                info_row = f"{m['type']}  |  {m['responsible'] or '—'}  |  Термін: {format_date(m['deadline']) or '—'}  |  Ефект: {m['effectiveness'] or '—'}"
                tk.Label(mf, text=info_row, bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                         font=("Segoe UI", 8)).grid(row=1, column=0, columnspan=3, sticky="w", pady=(4, 6))

                # Progress bar
                prog = m["progress"] or 0
                prog_bg = tk.Frame(mf, bg=T["BG_INPUT"], height=4)
                prog_bg.grid(row=2, column=0, columnspan=3, sticky="ew")
                if prog > 0:
                    prog_fill = tk.Frame(prog_bg, bg=st_color, height=4)
                    prog_fill.place(x=0, y=0, relwidth=prog / 100)
                tk.Label(mf, text=f"{prog}%", bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                         font=("Segoe UI", 8)).grid(row=3, column=2, sticky="e")

        # COMMENTS TAB
        for w in self.tab_comments.winfo_children():
            w.destroy()
        self._build_comments_tab(cur)

        # HISTORY TAB
        for w in self.tab_history.winfo_children():
            w.destroy()
        cur.execute("""
            SELECT action, entity, details, user_name, created_at
            FROM audit_log WHERE entity='risk' AND entity_id=?
            ORDER BY created_at DESC LIMIT 50
        """, (self.risk_id,))
        logs = cur.fetchall()

        hs_so, hs_si = make_scrollable(self.tab_history, orient="vertical")
        hs_so.pack(fill="both", expand=True)
        hs_si.grid_columnconfigure(0, weight=1)

        if not logs:
            tk.Label(hs_si, text="Журнал порожній",
                     bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 11)).grid(row=0, column=0, pady=40)
        else:
            for idx, lg in enumerate(logs):
                lf = tk.Frame(hs_si, bg=T["BG_CARD"], padx=14, pady=8)
                lf.grid(row=idx, column=0, sticky="ew", pady=(0, 4))
                lf.grid_columnconfigure(1, weight=1)
                tk.Label(lf, text=lg["action"],
                         bg=T["ACCENT"], fg="white",
                         font=("Segoe UI", 8, "bold"), padx=6, pady=2).grid(
                    row=0, column=0, sticky="w", padx=(0, 10))
                tk.Label(lf, text=lg["details"] or "", bg=T["BG_CARD"],
                         fg=T["TEXT_PRI"], font=("Segoe UI", 9)).grid(
                    row=0, column=1, sticky="w")
                tk.Label(lf, text=f"{lg['user_name']}  {format_date(lg['created_at'])}",
                         bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                         font=("Segoe UI", 8)).grid(row=0, column=2, sticky="e")

        conn.close()

    def _build_comments_tab(self, cur):
        cur.execute("""
            SELECT id, author, text, created_at FROM risk_comments
            WHERE risk_id=? ORDER BY created_at
        """, (self.risk_id,))
        comments = cur.fetchall()

        outer = tk.Frame(self.tab_comments, bg=T["BG_DARK"])
        outer.pack(fill="both", expand=True)
        outer.grid_rowconfigure(0, weight=1)
        outer.grid_columnconfigure(0, weight=1)
        outer.grid_propagate(False)

        cs_so, cs_si = make_scrollable(outer, orient="vertical")
        cs_so.grid(row=0, column=0, sticky="nsew")
        cs_si.grid_columnconfigure(0, weight=1)

        if not comments:
            tk.Label(cs_si, text="Коментарів немає",
                     bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 11)).grid(row=0, column=0, pady=20)
        else:
            for idx, cm in enumerate(comments):
                cf = tk.Frame(cs_si, bg=T["BG_CARD"], padx=16, pady=10)
                cf.grid(row=idx, column=0, sticky="ew", pady=(0, 4))
                cf.grid_columnconfigure(0, weight=1)
                hdr = tk.Frame(cf, bg=T["BG_CARD"])
                hdr.grid(row=0, column=0, sticky="ew")
                tk.Label(hdr, text=cm["author"], bg=T["BG_CARD"],
                         fg=T["ACCENT2"], font=("Segoe UI", 9, "bold")).pack(side="left")
                tk.Label(hdr, text=format_date(cm["created_at"]),
                         bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                         font=("Segoe UI", 8)).pack(side="right")
                tk.Label(cf, text=cm["text"], bg=T["BG_CARD"],
                         fg=T["TEXT_PRI"], font=("Segoe UI", 10),
                         wraplength=680, justify="left").grid(
                    row=1, column=0, sticky="w", pady=(4, 0))

        # New comment input
        inp_frame = tk.Frame(self.tab_comments, bg=T["BG_CARD"],
                              padx=16, pady=12)
        inp_frame.pack(fill="x", side="bottom")
        tk.Label(inp_frame, text="Новий коментар:",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).pack(anchor="w")
        self.comment_ta = tk.Text(inp_frame, height=3,
                                   bg=T["BG_INPUT"], fg=T["TEXT_PRI"],
                                   insertbackground=T["TEXT_PRI"],
                                   bd=0, padx=8, pady=6,
                                   font=("Segoe UI", 10), relief="flat",
                                   highlightthickness=1,
                                   highlightbackground=T["BORDER"],
                                   highlightcolor=T["ACCENT"])
        self.comment_ta.pack(fill="x", pady=(4, 8))
        ttk.Button(inp_frame, text="Додати коментар",
                   style="Small.Accent.TButton",
                   command=self._add_comment).pack(anchor="e")

    def _add_comment(self):
        text = self.comment_ta.get("1.0", "end").strip()
        if not text:
            return
        user = get_setting("user_name", "Аналітик")
        conn = get_connection()
        conn.execute(
            "INSERT INTO risk_comments (risk_id, author, text) VALUES (?,?,?)",
            (self.risk_id, user, text)
        )
        conn.commit()
        conn.close()
        self.comment_ta.delete("1.0", "end")
        self._load()
        ToastManager.show("Коментар додано", "success")

    def _edit(self):
        self.app.risk_form.load_for_edit(self.risk_id)
        self.app.navigate("risk_form")
        self.destroy()

    def _add_measure(self):
        self.app.measure_form.set_risk(self.risk_id)
        self.app.measure_form._reset()
        self.app.measure_form.set_risk(self.risk_id)
        self.app.navigate("measure_form")
        self.destroy()

# ═══════════════════════════════════════════════════════════════
# SETTINGS DIALOG
# ═══════════════════════════════════════════════════════════════

class SettingsDialog(tk.Toplevel):
    def __init__(self, parent, app):
        super().__init__(parent)
        self.app = app
        self.title("Налаштування")
        self.geometry("480x380")
        self.configure(bg=T["BG_DARK"])
        self.transient(parent)
        self.resizable(False, False)
        self._build()

    def _build(self):
        tk.Label(self, text="Налаштування застосунку",
                 bg=T["BG_DARK"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 16, "bold")).pack(anchor="w", padx=24, pady=(20, 4))
        tk.Label(self, text="Персоналізуйте роботу системи",
                 bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).pack(anchor="w", padx=24, pady=(0, 16))

        tk.Frame(self, bg=T["BORDER"], height=1).pack(fill="x")

        content = tk.Frame(self, bg=T["BG_DARK"])
        content.pack(fill="both", expand=True, padx=24, pady=16)

        # User name
        tk.Label(content, text="Ім'я користувача",
                 bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=0, column=0, sticky="w", pady=(0, 2))
        self.v_user = tk.StringVar(value=get_setting("user_name", "Аналітик"))
        ttk.Entry(content, textvariable=self.v_user, style="TEntry",
                  width=30).grid(row=1, column=0, sticky="ew", pady=(0, 16))

        # Theme
        tk.Label(content, text="Тема оформлення",
                 bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=2, column=0, sticky="w", pady=(0, 2))
        self.v_theme = tk.StringVar(value=get_setting("theme", "dark"))
        ttk.Combobox(content, textvariable=self.v_theme,
                     values=["dark", "light"],
                     state="readonly", width=28).grid(row=3, column=0, sticky="ew",
                                                       pady=(0, 16))

        # Auto-refresh
        tk.Label(content, text="Авто-оновлення (секунди, 0 = вимк)",
                 bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=4, column=0, sticky="w", pady=(0, 2))
        self.v_refresh = tk.StringVar(value=get_setting("auto_refresh", "30"))
        ttk.Entry(content, textvariable=self.v_refresh,
                  style="TEntry", width=10).grid(row=5, column=0, sticky="w",
                                                  pady=(0, 16))

        content.grid_columnconfigure(0, weight=1)

        # Buttons
        btn_frame = tk.Frame(self, bg=T["BG_DARK"], padx=24, pady=12)
        btn_frame.pack(fill="x")
        ttk.Button(btn_frame, text="Зберегти",
                   style="Accent.TButton",
                   command=self._save).pack(side="left", padx=(0, 8))
        ttk.Button(btn_frame, text="Скасувати",
                   style="Ghost.TButton",
                   command=self.destroy).pack(side="left")

    def _save(self):
        set_setting("user_name", self.v_user.get().strip() or "Аналітик")
        set_setting("theme", self.v_theme.get())
        set_setting("auto_refresh", self.v_refresh.get())
        ToastManager.show("Налаштування збережено. Перезапустіть для зміни теми.", "success")
        self.destroy()

# ═══════════════════════════════════════════════════════════════
# DASHBOARD PAGE
# ═══════════════════════════════════════════════════════════════

class DashboardPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="TFrame")
        self.app = app
        self.cat_data = []
        self.matrix_data = []
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._build()

    def _build(self):
        scroll_outer, self.scroll_inner = make_scrollable(self, orient="vertical")
        scroll_outer.grid(row=1, column=0, sticky="nsew")
        self.scroll_inner.grid_columnconfigure((0, 1, 2, 3), weight=1)

        # Stat cards
        self.stat_frame = tk.Frame(self.scroll_inner, bg=T["BG_DARK"])
        self.stat_frame.grid(row=0, column=0, columnspan=4,
                              sticky="ew", pady=(0, 20))
        for i in range(4):
            self.stat_frame.grid_columnconfigure(i, weight=1)

        # Row 2: matrix + trend
        row2 = tk.Frame(self.scroll_inner, bg=T["BG_DARK"])
        row2.grid(row=1, column=0, columnspan=4, sticky="ew", pady=(0, 16))
        row2.grid_columnconfigure(0, weight=2)
        row2.grid_columnconfigure(1, weight=1)
        row2.grid_columnconfigure(2, weight=2)

        # Matrix card
        matrix_card = tk.Frame(row2, bg=T["BG_CARD"])
        matrix_card.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        matrix_card.grid_columnconfigure(0, weight=1)
        self._make_card_header(matrix_card, "Матриця ризиків", "Розподіл Ймовірність x Вплив")
        self.matrix_canvas = tk.Canvas(matrix_card, bg=T["BG_CARD"],
                                        height=240, highlightthickness=0)
        self.matrix_canvas.grid(row=1, column=0, sticky="ew", padx=20, pady=(0, 16))
        matrix_card.bind("<Configure>", lambda e: self.after(50, self._draw_matrix))

        # Mini donut
        donut_card = tk.Frame(row2, bg=T["BG_CARD"])
        donut_card.grid(row=0, column=1, sticky="nsew", padx=(0, 10))
        donut_card.grid_columnconfigure(0, weight=1)
        self._make_card_header(donut_card, "За рівнем", "")
        self.donut_canvas = tk.Canvas(donut_card, bg=T["BG_CARD"],
                                       height=200, width=200, highlightthickness=0)
        self.donut_canvas.grid(row=1, column=0, padx=16, pady=(0, 16))
        self.donut_legend = tk.Frame(donut_card, bg=T["BG_CARD"])
        self.donut_legend.grid(row=2, column=0, sticky="ew", padx=16, pady=(0, 16))

        # Recent risks
        recent_card = tk.Frame(row2, bg=T["BG_CARD"])
        recent_card.grid(row=0, column=2, sticky="nsew")
        recent_card.grid_columnconfigure(0, weight=1)
        recent_card.grid_rowconfigure(1, weight=1)
        self._make_card_header(recent_card, "Останні ризики", "10 останніх записів")

        cols = ("Код", "Назва", "Рівень", "Статус")
        self.recent_tree = ttk.Treeview(recent_card, columns=cols,
                                         show="headings", height=7)
        for c in cols:
            self.recent_tree.heading(c, text=c)
        self.recent_tree.column("Код",    width=75,  stretch=False)
        self.recent_tree.column("Назва",  width=140, stretch=True)
        self.recent_tree.column("Рівень", width=75,  stretch=False)
        self.recent_tree.column("Статус", width=85,  stretch=False)
        self.recent_tree.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 16))
        self.recent_tree.bind("<Double-1>", self._open_recent)

        for lvl, col in LEVEL_COLORS.items():
            self.recent_tree.tag_configure(lvl, foreground=col)

        # Row 3: Category bars
        cat_card = tk.Frame(self.scroll_inner, bg=T["BG_CARD"])
        cat_card.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(0, 16))
        cat_card.grid_columnconfigure(0, weight=1)
        self._make_card_header(cat_card, "Розподіл за категоріями",
                               "Кількість ризиків по кожній категорії")
        self.cat_canvas = tk.Canvas(cat_card, bg=T["BG_CARD"],
                                     height=200, highlightthickness=0)
        self.cat_canvas.grid(row=1, column=0, sticky="ew", padx=20, pady=(0, 16))
        cat_card.bind("<Configure>", lambda e: self.after(50, self._draw_categories))

        # Row 4: Overdue measures
        overdue_card = tk.Frame(self.scroll_inner, bg=T["BG_CARD"])
        overdue_card.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(0, 16))
        overdue_card.grid_columnconfigure(0, weight=1)
        overdue_card.grid_rowconfigure(1, weight=1)
        self._make_card_header(overdue_card, "Прострочені заходи",
                               "Заходи з пропущеним терміном виконання")

        od_cols = ("Код", "Назва", "Ризик", "Відповідальний", "Термін", "Статус")
        self.overdue_tree = ttk.Treeview(overdue_card, columns=od_cols,
                                          show="headings", height=5)
        ow = [80, 200, 100, 130, 100, 100]
        for c, w in zip(od_cols, ow):
            self.overdue_tree.heading(c, text=c)
            self.overdue_tree.column(c, width=w)
        self.overdue_tree.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 16))
        self.overdue_tree.tag_configure("overdue", foreground=T["DANGER"])

        self.refresh()

    def _make_card_header(self, parent, title: str, subtitle: str):
        hf = tk.Frame(parent, bg=T["BG_CARD"])
        hf.grid(row=0, column=0, sticky="ew", padx=20, pady=(16, 8))
        tk.Label(hf, text=title, bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 12, "bold")).pack(anchor="w")
        if subtitle:
            tk.Label(hf, text=subtitle, bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 8)).pack(anchor="w")

    def _open_recent(self, event):
        sel = self.recent_tree.selection()
        if sel:
            iid = int(sel[0])
            RiskDetailDialog(self, iid, self.app)

    def refresh(self):
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM risks")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM risks WHERE status='Активний'")
        active = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM risks WHERE risk_level IN ('Критичний','Високий')")
        high = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM measures WHERE status='В процесі'")
        measures_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM measures WHERE status='Виконано'")
        done_cnt = cur.fetchone()[0]

        # Trend (last 7 days vs prev 7 days)
        cur.execute("""
            SELECT COUNT(*) FROM risks
            WHERE date(created_at) >= date('now','-7 days')
        """)
        new_7 = cur.fetchone()[0]

        for w in self.stat_frame.winfo_children():
            w.destroy()
        data_cards = [
            ("Усього ризиків",     total,       T["ACCENT"],  f"активних: {active}"),
            ("Критичних/Високих",  high,        T["DANGER"],  "потребують уваги"),
            ("Заходів в роботі",   measures_cnt,T["WARNING"], f"виконано: {done_cnt}"),
            ("Нових за 7 днів",    new_7,       T["INFO"],    "нових записів"),
        ]
        for i, (title, val, col, sub) in enumerate(data_cards):
            c = StatCard(self.stat_frame, title, val, col, sub)
            c.grid(row=0, column=i, sticky="ew",
                   padx=(0, 10) if i < 3 else 0)

        # Recent
        for row in self.recent_tree.get_children():
            self.recent_tree.delete(row)
        cur.execute("""
            SELECT id, risk_code, title, risk_level, status
            FROM risks ORDER BY created_at DESC LIMIT 10
        """)
        for r in cur.fetchall():
            self.recent_tree.insert("", "end", iid=str(r[0]),
                                    values=(r[1], r[2], r[3], r[4]),
                                    tags=(r[3],))

        # Cat data
        cur.execute("SELECT category, COUNT(*) FROM risks GROUP BY category ORDER BY 2 DESC")
        self.cat_data = cur.fetchall()

        # Matrix data
        cur.execute("SELECT probability, impact, COUNT(*) FROM risks GROUP BY probability, impact")
        self.matrix_data = cur.fetchall()

        # Level data for donut
        cur.execute("SELECT risk_level, COUNT(*) FROM risks GROUP BY risk_level")
        self.level_data = cur.fetchall()

        # Overdue measures
        for row in self.overdue_tree.get_children():
            self.overdue_tree.delete(row)
        cur.execute("""
            SELECT m.measure_code, m.title, r.risk_code,
                   m.responsible, m.deadline, m.status
            FROM measures m JOIN risks r ON m.risk_id=r.id
            WHERE m.deadline IS NOT NULL AND m.deadline != ''
              AND m.status NOT IN ('Виконано','Скасовано')
              AND date(m.deadline) < date('now')
            ORDER BY m.deadline
        """)
        for r in cur.fetchall():
            self.overdue_tree.insert("", "end",
                                     values=tuple(r),
                                     tags=("overdue",))

        conn.close()
        self.after(100, self._draw_matrix)
        self.after(100, self._draw_categories)
        self.after(100, self._draw_donut)

    def _draw_matrix(self):
        c = self.matrix_canvas
        c.delete("all")
        w = c.winfo_width() or 400
        h = 240
        pad_l = 70
        pad_t = 10
        pad_b = 40
        cols_count = 4
        rows_count = 3
        cell_w = (w - pad_l - 20) // cols_count
        cell_h = (h - pad_t - pad_b) // rows_count

        probs = ["Висока", "Середня", "Низька"]
        imps  = ["Низький", "Середній", "Високий", "Критичний"]

        mc = [
            ["#22c55e", "#eab308", "#f97316", "#ef4444"],
            ["#eab308", "#f97316", "#ef4444", "#ef4444"],
            ["#f97316", "#ef4444", "#ef4444", "#991b1b"],
        ]

        data_map = {(r[0], r[1]): r[2] for r in self.matrix_data}

        for ri, prob in enumerate(probs):
            for ci, imp in enumerate(imps):
                x0 = pad_l + ci * cell_w
                y0 = pad_t + ri * cell_h
                col = mc[ri][ci]
                c.create_rectangle(x0, y0, x0 + cell_w, y0 + cell_h,
                                   fill=col + "33", outline=col + "88",
                                   width=1)
                count = data_map.get((prob, imp), 0)
                if count:
                    # Bubble
                    cx_ = x0 + cell_w // 2
                    cy_ = y0 + cell_h // 2
                    r_  = min(18, count * 6 + 10)
                    c.create_oval(cx_ - r_, cy_ - r_, cx_ + r_, cy_ + r_,
                                  fill=col, outline="")
                    c.create_text(cx_, cy_, text=str(count), fill="white",
                                  font=("Segoe UI", 10, "bold"))

        # Y labels
        for ri, p in enumerate(probs):
            c.create_text(pad_l - 8, pad_t + ri * cell_h + cell_h // 2,
                          text=p, fill=T["TEXT_SEC"], anchor="e",
                          font=("Segoe UI", 9))
        # X labels
        for ci, imp in enumerate(imps):
            c.create_text(pad_l + ci * cell_w + cell_w // 2,
                          pad_t + rows_count * cell_h + 16,
                          text=imp, fill=T["TEXT_SEC"],
                          font=("Segoe UI", 8))

        # Axis titles
        c.create_text(10, h // 2, text="Ймов.", fill=T["TEXT_SEC"],
                      angle=90, font=("Segoe UI", 8))
        c.create_text(pad_l + cols_count * cell_w // 2,
                      h - 4, text="Вплив", fill=T["TEXT_SEC"],
                      font=("Segoe UI", 8))

    def _draw_donut(self):
        c = self.donut_canvas
        c.delete("all")
        for w in self.donut_legend.winfo_children():
            w.destroy()

        if not self.level_data:
            c.create_text(100, 100, text="Немає даних",
                          fill=T["TEXT_SEC"], font=("Segoe UI", 10))
            return

        total = sum(r[1] for r in self.level_data) or 1
        cx, cy, outer_r, inner_r = 100, 100, 80, 50
        start = -90.0

        order = ["Критичний", "Високий", "Середній", "Низький"]
        lvl_map = {r[0]: r[1] for r in self.level_data}

        for lvl in order:
            val = lvl_map.get(lvl, 0)
            if val == 0:
                continue
            extent = 360 * val / total
            col = LEVEL_COLORS.get(lvl, T["ACCENT"])
            c.create_arc(cx - outer_r, cy - outer_r,
                         cx + outer_r, cy + outer_r,
                         start=start, extent=extent,
                         fill=col, outline=T["BG_CARD"], width=2,
                         style="pieslice")
            start += extent

        # Hole
        c.create_oval(cx - inner_r, cy - inner_r,
                      cx + inner_r, cy + inner_r,
                      fill=T["BG_CARD"], outline="")
        c.create_text(cx, cy, text=str(total), fill=T["TEXT_PRI"],
                      font=("Segoe UI", 14, "bold"))
        c.create_text(cx, cy + 18, text="всього", fill=T["TEXT_SEC"],
                      font=("Segoe UI", 8))

        # Legend
        for i, lvl in enumerate(order):
            val = lvl_map.get(lvl, 0)
            if val == 0:
                continue
            col = LEVEL_COLORS.get(lvl, T["ACCENT"])
            row_f = tk.Frame(self.donut_legend, bg=T["BG_CARD"])
            row_f.pack(fill="x", pady=1)
            tk.Frame(row_f, bg=col, width=10, height=10).pack(side="left", padx=(0, 6))
            tk.Label(row_f, text=lvl, bg=T["BG_CARD"],
                     fg=T["TEXT_SEC"], font=("Segoe UI", 8)).pack(side="left")
            tk.Label(row_f, text=str(val), bg=T["BG_CARD"],
                     fg=col, font=("Segoe UI", 8, "bold")).pack(side="right")

    def _draw_categories(self):
        c = self.cat_canvas
        c.delete("all")
        if not self.cat_data:
            c.create_text(200, 60, text="Даних немає",
                          fill=T["TEXT_SEC"], font=("Segoe UI", 11))
            return
        w = c.winfo_width() or 600
        max_val = max(r[1] for r in self.cat_data) or 1
        bar_h = 26
        gap = 10
        pad_left = 120
        pad_right = 60
        colors = [T["ACCENT"], T["ACCENT2"], T["WARNING"], T["SUCCESS"],
                  T["DANGER"], T["INFO"], "#ec4899", "#84cc16", "#f97316", "#a855f7"]

        for i, (cat, val) in enumerate(self.cat_data):
            y = i * (bar_h + gap) + 8
            avail = max(10, w - pad_left - pad_right)
            bar_w = int((val / max_val) * avail)
            col = colors[i % len(colors)]

            # Background track
            c.create_rectangle(pad_left, y + 2, pad_left + avail, y + bar_h - 2,
                                fill=T["BG_INPUT"], outline="")
            # Actual bar
            c.create_rectangle(pad_left, y + 2, pad_left + bar_w, y + bar_h - 2,
                                fill=col, outline="")
            # Rounded right edge (simulate with oval)
            if bar_w > 6:
                c.create_oval(pad_left + bar_w - 6, y + 2,
                              pad_left + bar_w + 2, y + bar_h - 2,
                              fill=col, outline="")

            c.create_text(pad_left - 6, y + bar_h // 2,
                          text=cat[:15], fill=T["TEXT_PRI"],
                          anchor="e", font=("Segoe UI", 9))
            c.create_text(pad_left + bar_w + 8, y + bar_h // 2,
                          text=str(val), fill=T["TEXT_SEC"],
                          anchor="w", font=("Segoe UI", 9, "bold"))

        total_h = len(self.cat_data) * (bar_h + gap) + 16
        c.configure(height=total_h)

# ═══════════════════════════════════════════════════════════════
# RISK FORM PAGE
# ═══════════════════════════════════════════════════════════════

class RiskFormPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="TFrame")
        self.app = app
        self.edit_id: Optional[int] = None
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._build()

    def _build(self):
        scroll_outer, self.scroll_inner = make_scrollable(self, orient="vertical")
        scroll_outer.grid(row=1, column=0, sticky="nsew")
        self.scroll_inner.grid_columnconfigure(0, weight=1)
        self._build_form(self.scroll_inner)

    def _build_form(self, parent):
        # Header
        hdr_card = tk.Frame(parent, bg=T["BG_CARD"], padx=24, pady=20)
        hdr_card.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        hdr_card.grid_columnconfigure(0, weight=1)
        self.title_lbl = tk.Label(hdr_card, text="Новий ризик",
                                   bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                                   font=("Segoe UI", 18, "bold"))
        self.title_lbl.grid(row=0, column=0, sticky="w")
        tk.Label(hdr_card, text="Заповніть усi обов'язковi поля позначенi *",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=1, column=0, sticky="w")

        # Section 1
        s1 = tk.Frame(parent, bg=T["BG_CARD"], padx=24, pady=20)
        s1.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        s1.grid_columnconfigure((0, 1, 2), weight=1)
        tk.Label(s1, text="ОСНОВНА IНФОРМАЦIЯ",
                 bg=T["BG_CARD"], fg=T["ACCENT2"],
                 font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 14))

        self.v_code  = tk.StringVar()
        self.v_title = tk.StringVar()
        self.v_cat   = tk.StringVar()
        self.v_owner = tk.StringVar()
        self.v_dept  = tk.StringVar()
        self.v_status = tk.StringVar(value="Активний")
        self.v_review = tk.StringVar()
        self.v_tags   = tk.StringVar()

        lbl_entry(s1, 1, 0, "Код ризику *", self.v_code, required=True)
        lbl_entry(s1, 1, 1, "Назва ризику *", self.v_title, colspan=2, required=True)
        lbl_combo(s1, 3, 0, "Категорiя *", self.v_cat, CATEGORIES)
        lbl_combo(s1, 3, 1, "Вiддiл", self.v_dept, DEPARTMENTS)
        lbl_combo(s1, 3, 2, "Статус *", self.v_status, RISK_STATUSES)
        lbl_entry(s1, 5, 0, "Власник ризику", self.v_owner)
        lbl_entry(s1, 5, 1, "Дата огляду (РРРР-ММ-ДД)", self.v_review)
        lbl_entry(s1, 5, 2, "Теги (через кому)", self.v_tags)
        self.ta_desc = lbl_textarea(s1, 7, 0, "Опис ризику", height=3, colspan=3)

        # Section 2: Assessment
        s2 = tk.Frame(parent, bg=T["BG_CARD"], padx=24, pady=20)
        s2.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        s2.grid_columnconfigure((0, 1, 2), weight=1)
        tk.Label(s2, text="ОЦIНКА РИЗИКУ",
                 bg=T["BG_CARD"], fg=T["ACCENT2"],
                 font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 14))

        self.v_prob   = tk.StringVar(value="Середня")
        self.v_impact = tk.StringVar(value="Середній")
        self.v_res_prob = tk.StringVar()
        self.v_res_imp  = tk.StringVar()

        lbl_combo(s2, 1, 0, "Ймовiрнiсть *", self.v_prob, PROBABILITIES)
        lbl_combo(s2, 1, 1, "Вплив *", self.v_impact, IMPACTS)

        # Live level indicator
        level_f = tk.Frame(s2, bg=T["BG_INPUT"], padx=16, pady=12)
        level_f.grid(row=2, column=2, sticky="sw", padx=(0, 8), pady=(0, 12))
        tk.Label(level_f, text="Рiвень ризику",
                 bg=T["BG_INPUT"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 8)).pack(anchor="w")
        self.level_indicator = tk.Label(level_f, text="Середнiй",
                                         bg=T["BG_INPUT"],
                                         fg=LEVEL_COLORS["Середній"],
                                         font=("Segoe UI", 16, "bold"))
        self.level_indicator.pack(anchor="w")

        # Residual risk
        tk.Label(s2, text="ЗАЛИШКОВИЙ РИЗИК",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 8, "bold")).grid(
            row=3, column=0, columnspan=3, sticky="w", pady=(8, 4))
        lbl_combo(s2, 4, 0, "Залишкова ймовiрнiсть",
                  self.v_res_prob, [""] + PROBABILITIES)
        lbl_combo(s2, 4, 1, "Залишковий вплив",
                  self.v_res_imp, [""] + IMPACTS)

        self.v_prob.trace_add("write", lambda *a: self._update_level())
        self.v_impact.trace_add("write", lambda *a: self._update_level())

        # Buttons
        btn_f = tk.Frame(parent, bg=T["BG_DARK"], pady=12)
        btn_f.grid(row=4, column=0, sticky="ew", pady=(0, 16))
        self.save_btn = ttk.Button(btn_f, text="Зберегти ризик",
                                    style="Accent.TButton",
                                    command=self._save)
        self.save_btn.pack(side="left", padx=(0, 8))
        ttk.Button(btn_f, text="Скинути",
                   style="Ghost.TButton",
                   command=self._reset).pack(side="left", padx=(0, 8))
        ttk.Button(btn_f, text="Скасувати",
                   style="Ghost.TButton",
                   command=lambda: self.app.navigate("registry")).pack(side="left")

        self._reset()

    def _update_level(self):
        level = calc_level(self.v_prob.get(), self.v_impact.get())
        self.level_indicator.configure(
            text=level,
            fg=LEVEL_COLORS.get(level, T["SUCCESS"]))

    def _reset(self):
        self.edit_id = None
        self.title_lbl.configure(text="Новий ризик")
        self.save_btn.configure(text="Зберегти ризик")
        self.v_code.set(next_code("R", "risks"))
        self.v_title.set("")
        self.v_cat.set("")
        self.v_dept.set("")
        self.v_owner.set("")
        self.v_status.set("Активний")
        self.v_review.set("")
        self.v_tags.set("")
        self.v_prob.set("Середня")
        self.v_impact.set("Середній")
        self.v_res_prob.set("")
        self.v_res_imp.set("")
        self.ta_desc.delete("1.0", "end")
        self._update_level()

    def load_for_edit(self, risk_id: int):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM risks WHERE id=?", (risk_id,))
        r = cur.fetchone()
        conn.close()
        if not r:
            return
        self.edit_id = risk_id
        self.title_lbl.configure(text="Редагувати ризик")
        self.save_btn.configure(text="Оновити ризик")
        self.v_code.set(r["risk_code"])
        self.v_title.set(r["title"])
        self.v_cat.set(r["category"])
        self.v_dept.set(r["department"] or "")
        self.v_owner.set(r["owner"] or "")
        self.v_status.set(r["status"])
        self.v_review.set(r["review_date"] or "")
        self.v_tags.set(r["tags"] or "")
        self.v_prob.set(r["probability"])
        self.v_impact.set(r["impact"])
        self.v_res_prob.set(r["residual_probability"] or "")
        self.v_res_imp.set(r["residual_impact"] or "")
        self.ta_desc.delete("1.0", "end")
        self.ta_desc.insert("1.0", r["description"] or "")
        self._update_level()

    def _save(self):
        code  = self.v_code.get().strip()
        title = self.v_title.get().strip()
        cat   = self.v_cat.get().strip()
        prob  = self.v_prob.get().strip()
        imp   = self.v_impact.get().strip()

        if not all([code, title, cat, prob, imp]):
            ToastManager.show("Заповнiть усi обов'язковi поля!", "error")
            return

        desc    = self.ta_desc.get("1.0", "end").strip()
        owner   = self.v_owner.get().strip()
        dept    = self.v_dept.get().strip()
        status  = self.v_status.get()
        review  = self.v_review.get().strip()
        tags    = self.v_tags.get().strip()
        res_p   = self.v_res_prob.get().strip()
        res_i   = self.v_res_imp.get().strip()
        now     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user    = get_setting("user_name", "Аналiтик")

        conn = get_connection()
        cur  = conn.cursor()
        try:
            if self.edit_id:
                cur.execute("""
                    UPDATE risks SET risk_code=?, title=?, category=?,
                    description=?, probability=?, impact=?, owner=?,
                    department=?, status=?, review_date=?, tags=?,
                    residual_probability=?, residual_impact=?,
                    updated_at=? WHERE id=?
                """, (code, title, cat, desc, prob, imp, owner,
                      dept, status, review, tags, res_p, res_i,
                      now, self.edit_id))
                log_action("UPDATE", "risk", self.edit_id, f"Оновлено: {code}", user)
                ToastManager.show(f"Ризик '{code}' оновлено", "success")
            else:
                cur.execute("""
                    INSERT INTO risks
                    (risk_code, title, category, description, probability,
                     impact, owner, department, status, review_date, tags,
                     residual_probability, residual_impact)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (code, title, cat, desc, prob, imp, owner,
                      dept, status, review, tags, res_p, res_i))
                new_id = cur.lastrowid
                log_action("CREATE", "risk", new_id, f"Створено: {code}", user)
                ToastManager.show(f"Ризик '{code}' додано", "success")
            conn.commit()
            self.app.refresh_all()
            self._reset()
            self.app.navigate("registry")
        except sqlite3.IntegrityError:
            ToastManager.show(f"Код '{code}' вже iснує!", "error")
        except Exception as ex:
            ToastManager.show(str(ex), "error")
        finally:
            conn.close()

# ═══════════════════════════════════════════════════════════════
# MEASURE FORM PAGE
# ═══════════════════════════════════════════════════════════════

class MeasureFormPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="TFrame")
        self.app = app
        self.edit_id: Optional[int] = None
        self.preselect_risk_id: Optional[int] = None
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._build()

    def _build(self):
        scroll_outer, self.scroll_inner = make_scrollable(self, orient="vertical")
        scroll_outer.grid(row=1, column=0, sticky="nsew")
        self.scroll_inner.grid_columnconfigure(0, weight=1)
        self._build_form(self.scroll_inner)

    def _build_form(self, parent):
        # Header
        hdr = tk.Frame(parent, bg=T["BG_CARD"], padx=24, pady=20)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        hdr.grid_columnconfigure(0, weight=1)
        self.title_lbl = tk.Label(hdr, text="Новий захiд мiнiмiзацiї",
                                   bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                                   font=("Segoe UI", 18, "bold"))
        self.title_lbl.grid(row=0, column=0, sticky="w")
        tk.Label(hdr, text="Визначте заходи для управлiння виявленим ризиком",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=1, column=0, sticky="w")

        # Section 1: Risk link
        s1 = tk.Frame(parent, bg=T["BG_CARD"], padx=24, pady=20)
        s1.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        s1.grid_columnconfigure(0, weight=1)
        tk.Label(s1, text="ПРИВ'ЯЗКА ДО РИЗИКУ",
                 bg=T["BG_CARD"], fg=T["ACCENT2"],
                 font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, sticky="w", pady=(0, 14))

        tk.Label(s1, text="Ризик *", bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=1, column=0, sticky="w", pady=(0, 2))
        self.v_risk = tk.StringVar()
        self.risk_cb = ttk.Combobox(s1, textvariable=self.v_risk,
                                     state="readonly", style="TCombobox")
        self.risk_cb.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        self._load_risks()

        # Section 2: Measure details
        s2 = tk.Frame(parent, bg=T["BG_CARD"], padx=24, pady=20)
        s2.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        s2.grid_columnconfigure((0, 1, 2), weight=1)
        tk.Label(s2, text="ДЕТАЛI ЗАХОДУ",
                 bg=T["BG_CARD"], fg=T["ACCENT2"],
                 font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 14))

        self.v_mcode = tk.StringVar()
        self.v_mtitle = tk.StringVar()
        self.v_mtype = tk.StringVar(value="Зменшення")
        self.v_resp = tk.StringVar()
        self.v_ddl = tk.StringVar()
        self.v_mstat = tk.StringVar(value="Заплановано")
        self.v_eff = tk.StringVar(value="Не оцiнено")
        self.v_cost = tk.StringVar()
        self.v_progress = tk.IntVar(value=0)

        lbl_entry(s2, 1, 0, "Код заходу *", self.v_mcode, required=True)
        lbl_entry(s2, 1, 1, "Назва заходу *", self.v_mtitle, colspan=2, required=True)
        lbl_combo(s2, 3, 0, "Тип заходу *", self.v_mtype, MEASURE_TYPES)
        lbl_combo(s2, 3, 1, "Статус", self.v_mstat, MEASURE_STATUSES)
        lbl_combo(s2, 3, 2, "Ефективнiсть", self.v_eff, EFFECTIVENESS)
        lbl_entry(s2, 5, 0, "Вiдповiдальний", self.v_resp)
        lbl_entry(s2, 5, 1, "Термiн (РРРР-ММ-ДД)", self.v_ddl)
        lbl_entry(s2, 5, 2, "Вартiсть (грн)", self.v_cost, width=15)

        # Progress slider
        tk.Label(s2, text="Прогрес виконання (%)",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=7, column=0, sticky="w", pady=(0, 2))
        prog_frame = tk.Frame(s2, bg=T["BG_CARD"])
        prog_frame.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        self.progress_scale = ttk.Scale(prog_frame, from_=0, to=100,
                                         variable=self.v_progress,
                                         orient="horizontal",
                                         style="TScale")
        self.progress_scale.pack(side="left", fill="x", expand=True, padx=(0, 8))
        self.progress_lbl = tk.Label(prog_frame, text="0%",
                                      bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                                      font=("Segoe UI", 10, "bold"), width=5)
        self.progress_lbl.pack(side="left")
        self.v_progress.trace_add("write", lambda *a: self.progress_lbl.configure(
            text=f"{self.v_progress.get()}%"))

        self.ta_mdesc = lbl_textarea(s2, 9, 0, "Опис заходу",
                                      height=3, colspan=3)
        self.ta_notes = lbl_textarea(s2, 11, 0, "Примiтки",
                                      height=2, colspan=3)

        # Buttons
        btn_f = tk.Frame(parent, bg=T["BG_DARK"], pady=12)
        btn_f.grid(row=4, column=0, sticky="ew")
        self.save_btn = ttk.Button(btn_f, text="Зберегти захiд",
                                    style="Accent.TButton",
                                    command=self._save)
        self.save_btn.pack(side="left", padx=(0, 8))
        ttk.Button(btn_f, text="Скинути",
                   style="Ghost.TButton",
                   command=self._reset).pack(side="left", padx=(0, 8))
        ttk.Button(btn_f, text="Скасувати",
                   style="Ghost.TButton",
                   command=lambda: self.app.navigate("measures")).pack(side="left")

        self._reset()

    def _load_risks(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, risk_code, title FROM risks ORDER BY risk_code")
        self.risks_data = cur.fetchall()
        conn.close()
        vals = [f"{r['risk_code']} - {r['title']}" for r in self.risks_data]
        self.risk_cb["values"] = vals
        if self.preselect_risk_id:
            for i, r in enumerate(self.risks_data):
                if r["id"] == self.preselect_risk_id:
                    self.v_risk.set(vals[i])
                    break

    def set_risk(self, risk_id: int):
        self.preselect_risk_id = risk_id
        self._load_risks()

    def _reset(self):
        self.edit_id = None
        self.title_lbl.configure(text="Новий захiд мiнiмiзацiї")
        self.save_btn.configure(text="Зберегти захiд")
        self.v_mcode.set(next_code("M", "measures"))
        self.v_mtitle.set("")
        self.v_mtype.set("Зменшення")
        self.v_resp.set("")
        self.v_ddl.set("")
        self.v_mstat.set("Заплановано")
        self.v_eff.set("Не оцiнено")
        self.v_cost.set("")
        self.v_progress.set(0)
        self.ta_mdesc.delete("1.0", "end")
        self.ta_notes.delete("1.0", "end")
        self._load_risks()

    def load_for_edit(self, measure_id: int):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM measures WHERE id=?", (measure_id,))
        m = cur.fetchone()
        conn.close()
        if not m:
            return
        self.edit_id = measure_id
        self.title_lbl.configure(text="Редагувати захiд")
        self.save_btn.configure(text="Оновити захiд")
        self.preselect_risk_id = m["risk_id"]
        self._load_risks()
        self.v_mcode.set(m["measure_code"])
        self.v_mtitle.set(m["title"])
        self.v_mtype.set(m["type"])
        self.v_resp.set(m["responsible"] or "")
        self.v_ddl.set(m["deadline"] or "")
        self.v_mstat.set(m["status"])
        self.v_eff.set(m["effectiveness"] or "Не оцiнено")
        self.v_cost.set(str(m["cost_estimate"] or ""))
        self.v_progress.set(m["progress"] or 0)
        self.ta_mdesc.delete("1.0", "end")
        self.ta_mdesc.insert("1.0", m["description"] or "")
        self.ta_notes.delete("1.0", "end")
        self.ta_notes.insert("1.0", m["notes"] or "")

    def _get_selected_risk_id(self) -> Optional[int]:
        sel = self.v_risk.get()
        if not sel:
            return None
        for r in self.risks_data:
            if sel.startswith(r["risk_code"]):
                return r["id"]
        return None

    def _save(self):
        risk_id = self._get_selected_risk_id()
        code = self.v_mcode.get().strip()
        title = self.v_mtitle.get().strip()
        mtype = self.v_mtype.get().strip()

        if not all([risk_id, code, title, mtype]):
            ToastManager.show("Заповнiть усi обов'язковi поля!", "error")
            return

        desc = self.ta_mdesc.get("1.0", "end").strip()
        notes = self.ta_notes.get("1.0", "end").strip()
        resp = self.v_resp.get().strip()
        ddl = self.v_ddl.get().strip()
        mstat = self.v_mstat.get()
        eff = self.v_eff.get()
        cost_str = self.v_cost.get().strip()
        cost = float(cost_str) if cost_str else None
        progress = self.v_progress.get()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user = get_setting("user_name", "Аналiтик")

        conn = get_connection()
        cur = conn.cursor()
        try:
            if self.edit_id:
                cur.execute("""
                    UPDATE measures SET risk_id=?, measure_code=?, title=?,
                    description=?, type=?, responsible=?, deadline=?,
                    status=?, effectiveness=?, cost_estimate=?,
                    progress=?, notes=?, updated_at=?
                    WHERE id=?
                """, (risk_id, code, title, desc, mtype, resp, ddl,
                      mstat, eff, cost, progress, notes, now, self.edit_id))
                log_action("UPDATE", "measure", self.edit_id, f"Оновлено: {code}", user)
                ToastManager.show(f"Захiд '{code}' оновлено", "success")
            else:
                cur.execute("""
                    INSERT INTO measures
                    (risk_id, measure_code, title, description, type,
                     responsible, deadline, status, effectiveness, cost_estimate,
                     progress, notes)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (risk_id, code, title, desc, mtype, resp, ddl,
                      mstat, eff, cost, progress, notes))
                new_id = cur.lastrowid
                log_action("CREATE", "measure", new_id, f"Створено: {code}", user)
                ToastManager.show(f"Захiд '{code}' додано", "success")
            conn.commit()
            self.app.refresh_all()
            self._reset()
            self.app.navigate("measures")
        except Exception as ex:
            ToastManager.show(str(ex), "error")
        finally:
            conn.close()

# ═══════════════════════════════════════════════════════════════
# REGISTRY PAGE (RISK TABLE)
# ═══════════════════════════════════════════════════════════════

class RegistryPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="TFrame")
        self.app = app
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)
        self._sort_col = None
        self._sort_rev = False
        self._build()

    def _build(self):
        # Header
        hdr = tk.Frame(self, bg=T["BG_DARK"])
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        hdr.grid_columnconfigure(0, weight=1)
        tk.Label(hdr, text="Реєстр ризикiв",
                 bg=T["BG_DARK"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 20, "bold")).grid(row=0, column=0, sticky="w")
        tk.Label(hdr, text="Перегляд, пошук та управлiння усiма ризиками",
                 bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 10)).grid(row=1, column=0, sticky="w")

        # Toolbar
        toolbar = tk.Frame(self, bg=T["BG_CARD"], pady=12, padx=16)
        toolbar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        toolbar.grid_columnconfigure(1, weight=1)

        ttk.Button(toolbar, text="+ Новий ризик",
                   style="Accent.TButton",
                   command=lambda: self.app.navigate("risk_form")).grid(
            row=0, column=0, sticky="w", padx=(0, 8))

        # Search & filters
        sf = tk.Frame(toolbar, bg=T["BG_CARD"])
        sf.grid(row=0, column=1, sticky="w")

        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *a: self.refresh())
        se = ttk.Entry(sf, textvariable=self.search_var, width=32, style="TEntry")
        se.insert(0, "Пошук...")
        se.bind("<FocusIn>",
                lambda e: se.delete(0, "end") if se.get() == "Пошук..." else None)
        se.grid(row=0, column=0, padx=(0, 8))

        self.filter_level = tk.StringVar(value="Усi рiвнi")
        ttk.Combobox(sf, textvariable=self.filter_level,
                     values=["Усi рiвнi", "Критичний", "Високий", "Середнiй", "Низький"],
                     state="readonly", width=14).grid(row=0, column=1, padx=(0, 8))
        self.filter_level.trace_add("write", lambda *a: self.refresh())

        self.filter_status = tk.StringVar(value="Усi статуси")
        ttk.Combobox(sf, textvariable=self.filter_status,
                     values=["Усi статуси"] + RISK_STATUSES,
                     state="readonly", width=14).grid(row=0, column=2, padx=(0, 8))
        self.filter_status.trace_add("write", lambda *a: self.refresh())

        self.filter_cat = tk.StringVar(value="Усi категорiї")
        ttk.Combobox(sf, textvariable=self.filter_cat,
                     values=["Усi категорiї"] + CATEGORIES,
                     state="readonly", width=14).grid(row=0, column=3)
        self.filter_cat.trace_add("write", lambda *a: self.refresh())

        # Export buttons
        exp_frame = tk.Frame(toolbar, bg=T["BG_CARD"])
        exp_frame.grid(row=0, column=2, sticky="e")
        ttk.Button(exp_frame, text="Iмпорт CSV",
                   style="Ghost.TButton",
                   command=self._import_csv).pack(side="left", padx=(0, 4))
        ttk.Button(exp_frame, text="Експорт CSV",
                   style="Ghost.TButton",
                   command=self._export_csv).pack(side="left", padx=(0, 4))
        ttk.Button(exp_frame, text="Експорт PDF",
                   style="Ghost.TButton",
                   command=self._export_pdf).pack(side="left")

        # Separator
        tk.Frame(self, bg=T["BORDER"], height=1).grid(
            row=2, column=0, sticky="ew", pady=(0, 8))

        # Main container: table + details panel
        main_container = tk.Frame(self, bg=T["BG_DARK"])
        main_container.grid(row=3, column=0, sticky="nsew")
        self.grid_rowconfigure(3, weight=1)
        main_container.grid_columnconfigure(0, weight=3)
        main_container.grid_columnconfigure(1, weight=1)
        main_container.grid_rowconfigure(0, weight=1)

        # Table
        tbl_frame = tk.Frame(main_container, bg=T["BG_DARK"])
        tbl_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        tbl_frame.grid_rowconfigure(0, weight=1)
        tbl_frame.grid_columnconfigure(0, weight=1)

        cols = ("Код", "Назва", "Категорiя", "Ймовiрнiсть",
                "Вплив", "Рiвень", "Власник", "Статус", "Оновлено")
        self.tree = ttk.Treeview(tbl_frame, columns=cols,
                                  show="headings", selectmode="browse")
        widths = [85, 200, 110, 95, 90, 90, 120, 90, 110]
        for c, w in zip(cols, widths):
            self.tree.heading(c, text=c,
                              command=lambda _c=c: self._sort_by(_c))
            self.tree.column(c, width=w, minwidth=50)

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical",
                            command=self.tree.yview,
                            style="Vertical.TScrollbar")
        hsb = ttk.Scrollbar(tbl_frame, orient="horizontal",
                            command=self.tree.xview,
                            style="Horizontal.TScrollbar")
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        for lvl, col in LEVEL_COLORS.items():
            self.tree.tag_configure(lvl, foreground=col)

        # Details panel
        self.details_panel = tk.Frame(main_container, bg=T["BG_CARD"],
                                       width=280)
        self.details_panel.grid(row=0, column=1, sticky="nsew")
        self.details_panel.grid_propagate(False)
        self._build_details_panel()

        # Context menu
        self.ctx = tk.Menu(self.tree, tearoff=0,
                           bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                           activebackground=T["ACCENT"],
                           activeforeground="white",
                           borderwidth=0, font=("Segoe UI", 9))
        self.ctx.add_command(label="Детальний перегляд", command=self._view_detail)
        self.ctx.add_command(label="Редагувати", command=self._edit_selected)
        self.ctx.add_command(label="Додати захiд", command=self._add_measure_for_selected)
        self.ctx.add_separator()
        self.ctx.add_command(label="Дублювати", command=self._duplicate_selected)
        self.ctx.add_separator()
        self.ctx.add_command(label="Видалити", command=self._delete_selected)

        self.tree.bind("<Button-3>", self._show_ctx)
        self.tree.bind("<Double-1>", lambda e: self._view_detail())
        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        self.tree.bind("<Delete>", lambda e: self._delete_selected())

        self.refresh()

    def _build_details_panel(self):
        for w in self.details_panel.winfo_children():
            w.destroy()

        tk.Label(self.details_panel, text="ДЕТАЛI",
                 bg=T["BG_CARD"], fg=T["ACCENT2"],
                 font=("Segoe UI", 10, "bold")).pack(anchor="w",
                                                      padx=16, pady=(16, 8))
        tk.Frame(self.details_panel, bg=T["BORDER"], height=1).pack(
            fill="x", padx=16, pady=(0, 12))

        self.detail_content = tk.Frame(self.details_panel, bg=T["BG_CARD"])
        self.detail_content.pack(fill="both", expand=True, padx=16, pady=(0, 16))

        tk.Label(self.detail_content, text="Виберiть ризик для перегляду",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).pack(pady=40)

    def _on_select(self, event):
        sel = self.tree.selection()
        if not sel:
            self._build_details_panel()
            return
        risk_id = int(sel[0])
        self._load_detail(risk_id)

    def _load_detail(self, risk_id: int):
        for w in self.detail_content.winfo_children():
            w.destroy()

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM risks WHERE id=?", (risk_id,))
        r = cur.fetchone()
        if not r:
            conn.close()
            return

        # Risk level badge
        lvl = r["risk_level"]
        badge = tk.Frame(self.detail_content,
                         bg=LEVEL_COLORS.get(lvl, T["ACCENT"]),
                         padx=10, pady=4)
        badge.pack(anchor="w", pady=(0, 8))
        tk.Label(badge, text=lvl,
                 bg=LEVEL_COLORS.get(lvl, T["ACCENT"]),
                 fg="white", font=("Segoe UI", 9, "bold")).pack()

        # Title
        tk.Label(self.detail_content, text=r["title"],
                 bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 11, "bold"),
                 wraplength=240, justify="left").pack(anchor="w", pady=(0, 12))

        # Fields
        def add_field(label, value):
            f = tk.Frame(self.detail_content, bg=T["BG_CARD"])
            f.pack(fill="x", pady=3)
            tk.Label(f, text=label, bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 8)).pack(anchor="w")
            tk.Label(f, text=value or "—", bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                     font=("Segoe UI", 9), wraplength=240, justify="left").pack(anchor="w")

        add_field("Код", r["risk_code"])
        add_field("Категорiя", r["category"])
        add_field("Власник", r["owner"])
        add_field("Вiддiл", r["department"])
        add_field("Статус", r["status"])
        add_field("Ймовiрнiсть", r["probability"])
        add_field("Вплив", r["impact"])

        # Measures count
        cur.execute("SELECT COUNT(*) FROM measures WHERE risk_id=?", (risk_id,))
        m_count = cur.fetchone()[0]
        add_field("Заходiв", str(m_count))

        # Tags
        if r["tags"]:
            tag_f = tk.Frame(self.detail_content, bg=T["BG_CARD"])
            tag_f.pack(fill="x", pady=(8, 0))
            tk.Label(tag_f, text="Теги:", bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                     font=("Segoe UI", 8)).pack(anchor="w")
            tags_row = tk.Frame(tag_f, bg=T["BG_CARD"])
            tags_row.pack(anchor="w", pady=(2, 0))
            for tag in r["tags"].split(","):
                tag = tag.strip()
                if tag:
                    tb = tk.Frame(tags_row, bg=T["ACCENT2"], padx=6, pady=2)
                    tb.pack(side="left", padx=(0, 4))
                    tk.Label(tb, text=tag, bg=T["ACCENT2"], fg="white",
                             font=("Segoe UI", 7, "bold")).pack()

        # Action buttons
        btn_f = tk.Frame(self.detail_content, bg=T["BG_CARD"])
        btn_f.pack(fill="x", pady=(16, 0))
        ttk.Button(btn_f, text="Перегляд",
                   style="Small.Accent.TButton",
                   command=lambda: RiskDetailDialog(self, risk_id, self.app)).pack(
            fill="x", pady=(0, 4))
        ttk.Button(btn_f, text="Редагувати",
                   style="Small.Ghost.TButton",
                   command=self._edit_selected).pack(fill="x", pady=(0, 4))
        ttk.Button(btn_f, text="Видалити",
                   style="Small.Danger.TButton",
                   command=self._delete_selected).pack(fill="x")

        conn.close()

    def _sort_by(self, col):
        if self._sort_col == col:
            self._sort_rev = not self._sort_rev
        else:
            self._sort_col = col
            self._sort_rev = False
        self.refresh()

    def refresh(self):
        for row in self.tree.get_children():
            self.tree.delete(row)

        q = self.search_var.get()
        if q == "Пошук...":
            q = ""
        lvl = self.filter_level.get()
        stat = self.filter_status.get()
        cat = self.filter_cat.get()

        conn = get_connection()
        cur = conn.cursor()
        sql = """
            SELECT id, risk_code, title, category, probability, impact,
                   risk_level, owner, status, updated_at
            FROM risks WHERE 1=1
        """
        params = []
        if q:
            sql += " AND (risk_code LIKE ? OR title LIKE ? OR category LIKE ? OR owner LIKE ?)"
            params += [f"%{q}%"] * 4
        if lvl != "Усi рiвнi":
            sql += " AND risk_level=?"
            params.append(lvl)
        if stat != "Усi статуси":
            sql += " AND status=?"
            params.append(stat)
        if cat != "Усi категорiї":
            sql += " AND category=?"
            params.append(cat)

        col_map = {
            "Код": "risk_code", "Назва": "title", "Категорiя": "category",
            "Ймовiрнiсть": "probability", "Вплив": "impact",
            "Рiвень": "risk_level", "Власник": "owner",
            "Статус": "status", "Оновлено": "updated_at"
        }
        if self._sort_col and self._sort_col in col_map:
            sql += f" ORDER BY {col_map[self._sort_col]}"
            if self._sort_rev:
                sql += " DESC"

        cur.execute(sql, params)
        for r in cur.fetchall():
            vals = (r["risk_code"], r["title"], r["category"],
                    r["probability"], r["impact"], r["risk_level"],
                    r["owner"] or "—", r["status"],
                    format_date(r["updated_at"]))
            self.tree.insert("", "end", iid=str(r["id"]),
                             values=vals, tags=(r["risk_level"],))
        conn.close()

    def _get_selected_id(self) -> Optional[int]:
        sel = self.tree.selection()
        return int(sel[0]) if sel else None

    def _show_ctx(self, e):
        item = self.tree.identify_row(e.y)
        if item:
            self.tree.selection_set(item)
            self.ctx.post(e.x_root, e.y_root)

    def _view_detail(self):
        rid = self._get_selected_id()
        if rid:
            RiskDetailDialog(self, rid, self.app)

    def _edit_selected(self):
        rid = self._get_selected_id()
        if rid:
            self.app.risk_form.load_for_edit(rid)
            self.app.navigate("risk_form")

    def _add_measure_for_selected(self):
        rid = self._get_selected_id()
        if rid:
            self.app.measure_form.set_risk(rid)
            self.app.measure_form._reset()
            self.app.measure_form.set_risk(rid)
            self.app.navigate("measure_form")

    def _duplicate_selected(self):
        rid = self._get_selected_id()
        if not rid:
            return
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM risks WHERE id=?", (rid,))
        r = cur.fetchone()
        if r:
            new_code = next_code("R", "risks")
            cur.execute("""
                INSERT INTO risks
                (risk_code, title, category, description, probability,
                 impact, owner, department, status, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (new_code, f"Копiя: {r['title']}", r["category"],
                  r["description"], r["probability"], r["impact"],
                  r["owner"], r["department"], r["status"], r["tags"]))
            new_id = cur.lastrowid
            log_action("DUPLICATE", "risk", new_id,
                       f"Дубль з {r['risk_code']}", get_setting("user_name"))
            conn.commit()
            ToastManager.show(f"Ризик дублiковано як {new_code}", "success")
            self.app.refresh_all()
        conn.close()

    def _delete_selected(self):
        rid = self._get_selected_id()
        if not rid:
            return
        if messagebox.askyesno("Видалення",
                               "Видалити ризик та всi пов'язанi заходи?\n\nЦю дiю неможливо скасувати."):
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT risk_code FROM risks WHERE id=?", (rid,))
            r = cur.fetchone()
            code = r[0] if r else "?"
            conn.execute("DELETE FROM risks WHERE id=?", (rid,))
            conn.commit()
            conn.close()
            log_action("DELETE", "risk", rid, f"Видалено: {code}",
                       get_setting("user_name"))
            ToastManager.show(f"Ризик '{code}' видалено", "success")
            self.app.refresh_all()
            self._build_details_panel()

    def _export_csv(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV файли", "*.csv")],
            title="Експорт реєстру ризикiв")
        if not path:
            return
        rows = [self.tree.item(i)["values"]
                for i in self.tree.get_children()]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["Код", "Назва", "Категорiя", "Ймовiрнiсть",
                        "Вплив", "Рiвень", "Власник", "Статус", "Оновлено"])
            w.writerows(rows)
        ToastManager.show(f"Експортовано: {os.path.basename(path)}", "success")

    def _import_csv(self):
        path = filedialog.askopenfilename(
            filetypes=[("CSV файли", "*.csv")],
            title="Iмпорт ризикiв з CSV")
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                conn = get_connection()
                cur = conn.cursor()
                count = 0
                for row in reader:
                    try:
                        code = row.get("Код", "").strip()
                        title = row.get("Назва", "").strip()
                        cat = row.get("Категорiя", "").strip()
                        prob = row.get("Ймовiрнiсть", "Середня").strip()
                        imp = row.get("Вплив", "Середнiй").strip()
                        owner = row.get("Власник", "").strip()
                        status = row.get("Статус", "Активний").strip()
                        if not all([code, title, cat]):
                            continue
                        cur.execute("""
                            INSERT OR IGNORE INTO risks
                            (risk_code, title, category, probability, impact, owner, status)
                            VALUES (?,?,?,?,?,?,?)
                        """, (code, title, cat, prob, imp, owner, status))
                        count += 1
                    except Exception:
                        continue
                conn.commit()
                conn.close()
            ToastManager.show(f"Iмпортовано {count} записiв", "success")
            self.app.refresh_all()
        except Exception as ex:
            ToastManager.show(f"Помилка iмпорту: {ex}", "error")

    def _export_pdf(self):
        # Simple text-based fallback if reportlab not available
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.lib.units import cm
            from reportlab.lib import colors

            path = filedialog.asksaveasfilename(
                defaultextension=".pdf",
                filetypes=[("PDF файли", "*.pdf")],
                title="Експорт у PDF")
            if not path:
                return

            doc = SimpleDocTemplate(path, pagesize=landscape(A4))
            elements = []
            styles = getSampleStyleSheet()

            # Title
            elements.append(Paragraph("Реєстр ризикiв", styles['Title']))
            elements.append(Spacer(1, 0.5 * cm))

            # Table data
            data = [["Код", "Назва", "Категорiя", "Ймовiрнiсть",
                     "Вплив", "Рiвень", "Статус"]]
            for iid in self.tree.get_children():
                vals = self.tree.item(iid)["values"]
                data.append([vals[0], vals[1][:30], vals[2],
                             vals[3], vals[4], vals[5], vals[7]])

            t = Table(data)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(t)
            doc.build(elements)
            ToastManager.show(f"PDF збережено: {os.path.basename(path)}", "success")

        except ImportError:
            # Fallback: plain text
            path = filedialog.asksaveasfilename(
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt")],
                title="Експорт (текст)")
            if not path:
                return
            with open(path, "w", encoding="utf-8") as f:
                f.write("РЕЄСТР РИЗИКIВ\n")
                f.write("=" * 80 + "\n\n")
                for iid in self.tree.get_children():
                    vals = self.tree.item(iid)["values"]
                    f.write(f"Код: {vals[0]}\n")
                    f.write(f"Назва: {vals[1]}\n")
                    f.write(f"Категорiя: {vals[2]}\n")
                    f.write(f"Рiвень: {vals[5]}\n")
                    f.write(f"Статус: {vals[7]}\n")
                    f.write("-" * 80 + "\n")
            ToastManager.show(f"Текстовий файл збережено", "success")

# ═══════════════════════════════════════════════════════════════
# MEASURES PAGE
# ═══════════════════════════════════════════════════════════════

class MeasuresPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="TFrame")
        self.app = app
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)
        self._build()

    def _build(self):
        # Header
        hdr = tk.Frame(self, bg=T["BG_DARK"])
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        hdr.grid_columnconfigure(0, weight=1)
        tk.Label(hdr, text="Заходи мiнiмiзацiї",
                 bg=T["BG_DARK"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 20, "bold")).grid(row=0, column=0, sticky="w")
        tk.Label(hdr, text="Управлiння заходами зниження ризикiв",
                 bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 10)).grid(row=1, column=0, sticky="w")

        # Toolbar
        toolbar = tk.Frame(self, bg=T["BG_CARD"], pady=12, padx=16)
        toolbar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        toolbar.grid_columnconfigure(1, weight=1)

        ttk.Button(toolbar, text="+ Новий захiд",
                   style="Accent.TButton",
                   command=lambda: self.app.navigate("measure_form")).grid(
            row=0, column=0, padx=(0, 8))

        # Filters
        sf = tk.Frame(toolbar, bg=T["BG_CARD"])
        sf.grid(row=0, column=1, sticky="w")

        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *a: self.refresh())
        se = ttk.Entry(sf, textvariable=self.search_var, width=30)
        se.insert(0, "Пошук...")
        se.bind("<FocusIn>",
                lambda e: se.delete(0, "end") if se.get() == "Пошук..." else None)
        se.grid(row=0, column=0, padx=(0, 8))

        self.filter_type = tk.StringVar(value="Всi типи")
        ttk.Combobox(sf, textvariable=self.filter_type,
                     values=["Всi типи"] + MEASURE_TYPES,
                     state="readonly", width=14).grid(row=0, column=1, padx=(0, 8))
        self.filter_type.trace_add("write", lambda *a: self.refresh())

        self.filter_status = tk.StringVar(value="Всi статуси")
        ttk.Combobox(sf, textvariable=self.filter_status,
                     values=["Всi статуси"] + MEASURE_STATUSES,
                     state="readonly", width=14).grid(row=0, column=2)
        self.filter_status.trace_add("write", lambda *a: self.refresh())

        # Bulk actions
        bulk_f = tk.Frame(toolbar, bg=T["BG_CARD"])
        bulk_f.grid(row=0, column=2, sticky="e")
        tk.Label(bulk_f, text="Масовi дiї:",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).pack(side="left", padx=(0, 6))
        self.bulk_action = tk.StringVar(value="Вибрати...")
        bulk_cb = ttk.Combobox(bulk_f, textvariable=self.bulk_action,
                                values=["Вибрати...", "Позначити виконано",
                                        "Позначити в процесi", "Позначити скасовано"],
                                state="readonly", width=18)
        bulk_cb.pack(side="left", padx=(0, 6))
        bulk_cb.bind("<<ComboboxSelected>>", self._do_bulk_action)

        tk.Frame(self, bg=T["BORDER"], height=1).grid(
            row=2, column=0, sticky="ew", pady=(0, 8))

        # Table
        tbl_frame = tk.Frame(self, bg=T["BG_DARK"])
        tbl_frame.grid(row=3, column=0, sticky="nsew")
        tbl_frame.grid_rowconfigure(0, weight=1)
        tbl_frame.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        cols = ("Код", "Ризик", "Назва", "Тип",
                "Вiдповiдальний", "Термiн", "Статус", "Ефект.", "Прогрес")
        self.tree = ttk.Treeview(tbl_frame, columns=cols,
                                  show="tree headings", selectmode="extended")
        widths = [80, 100, 200, 100, 130, 100, 110, 80, 80]
        for c, w in zip(cols, widths):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, minwidth=50)
        self.tree.column("#0", width=30, stretch=False)

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical",
                            command=self.tree.yview,
                            style="Vertical.TScrollbar")
        hsb = ttk.Scrollbar(tbl_frame, orient="horizontal",
                            command=self.tree.xview,
                            style="Horizontal.TScrollbar")
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # Tags
        self.tree.tag_configure("Виконано", foreground=T["SUCCESS"])
        self.tree.tag_configure("В процесi", foreground=T["WARNING"])
        self.tree.tag_configure("Скасовано", foreground=T["DANGER"])
        self.tree.tag_configure("Заплановано", foreground=T["ACCENT2"])
        self.tree.tag_configure("Вiдкладено", foreground=T["TEXT_SEC"])
        self.tree.tag_configure("overdue", foreground=T["DANGER"], font=("Segoe UI", 9, "bold"))

        # Context
        ctx = tk.Menu(self.tree, tearoff=0, bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                      activebackground=T["ACCENT"], activeforeground="white",
                      borderwidth=0, font=("Segoe UI", 9))
        ctx.add_command(label="Редагувати", command=self._edit_selected)
        ctx.add_separator()
        ctx.add_command(label="Позначити виконано", command=lambda: self._set_status("Виконано"))
        ctx.add_command(label="Позначити в процесi", command=lambda: self._set_status("В процесi"))
        ctx.add_separator()
        ctx.add_command(label="Видалити", command=self._delete_selected)
        self.ctx = ctx

        self.tree.bind("<Button-3>", lambda e: self._show_ctx(e))
        self.tree.bind("<Double-1>", lambda e: self._edit_selected())
        self.tree.bind("<Delete>", lambda e: self._delete_selected())

        self.refresh()

    def _do_bulk_action(self, event):
        action = self.bulk_action.get()
        if action == "Вибрати...":
            return
        sel = self.tree.selection()
        if not sel:
            ToastManager.show("Не обрано жодного заходу", "warning")
            return

        status_map = {
            "Позначити виконано": "Виконано",
            "Позначити в процесi": "В процесi",
            "Позначити скасовано": "Скасовано"
        }
        new_status = status_map.get(action)
        if not new_status:
            return

        conn = get_connection()
        for iid in sel:
            mid = int(iid)
            conn.execute("UPDATE measures SET status=?, updated_at=datetime('now','localtime') WHERE id=?",
                         (new_status, mid))
            log_action("BULK_UPDATE", "measure", mid, f"Статус -> {new_status}",
                       get_setting("user_name"))
        conn.commit()
        conn.close()
        ToastManager.show(f"Оновлено {len(sel)} заходiв", "success")
        self.bulk_action.set("Вибрати...")
        self.app.refresh_all()

    def refresh(self):
        for row in self.tree.get_children():
            self.tree.delete(row)

        q = self.search_var.get()
        if q == "Пошук...":
            q = ""
        typ = self.filter_type.get()
        stat = self.filter_status.get()

        conn = get_connection()
        cur = conn.cursor()
        sql = """
            SELECT m.id, m.measure_code, r.risk_code, m.title,
                   m.type, m.responsible, m.deadline,
                   m.status, m.effectiveness, m.progress
            FROM measures m
            JOIN risks r ON m.risk_id = r.id
            WHERE 1=1
        """
        params = []
        if q:
            sql += " AND (m.measure_code LIKE ? OR m.title LIKE ? OR r.risk_code LIKE ?)"
            params += [f"%{q}%"] * 3
        if typ != "Всi типи":
            sql += " AND m.type=?"
            params.append(typ)
        if stat != "Всi статуси":
            sql += " AND m.status=?"
            params.append(stat)
        sql += " ORDER BY m.created_at DESC"

        cur.execute(sql, params)
        for r in cur.fetchall():
            overdue = is_overdue(r["deadline"])
            tags = [r["status"]]
            if overdue:
                tags.append("overdue")
            self.tree.insert("", "end", iid=str(r["id"]),
                             text="☐",
                             values=(r["measure_code"], r["risk_code"],
                                     r["title"], r["type"],
                                     r["responsible"] or "—",
                                     format_date(r["deadline"]) or "—",
                                     r["status"], r["effectiveness"] or "—",
                                     f"{r['progress'] or 0}%"),
                             tags=tuple(tags))
        conn.close()

    def _get_selected_id(self):
        sel = self.tree.selection()
        return int(sel[0]) if sel and len(sel) == 1 else None

    def _show_ctx(self, e):
        item = self.tree.identify_row(e.y)
        if item:
            if item not in self.tree.selection():
                self.tree.selection_set(item)
            self.ctx.post(e.x_root, e.y_root)

    def _edit_selected(self):
        mid = self._get_selected_id()
        if mid:
            self.app.measure_form.load_for_edit(mid)
            self.app.navigate("measure_form")

    def _set_status(self, new_status: str):
        sel = self.tree.selection()
        if not sel:
            return
        conn = get_connection()
        for iid in sel:
            mid = int(iid)
            conn.execute("UPDATE measures SET status=?, updated_at=datetime('now','localtime') WHERE id=?",
                         (new_status, mid))
            log_action("STATUS_CHANGE", "measure", mid, f"Статус -> {new_status}",
                       get_setting("user_name"))
        conn.commit()
        conn.close()
        ToastManager.show(f"Статус оновлено", "success")
        self.app.refresh_all()

    def _delete_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        if messagebox.askyesno("Видалення", f"Видалити {len(sel)} захід(iв)?"):
            conn = get_connection()
            for iid in sel:
                mid = int(iid)
                conn.execute("DELETE FROM measures WHERE id=?", (mid,))
                log_action("DELETE", "measure", mid, "Видалено", get_setting("user_name"))
            conn.commit()
            conn.close()
            ToastManager.show(f"Видалено {len(sel)} записiв", "success")
            self.app.refresh_all()

# ═══════════════════════════════════════════════════════════════
# REPORTS PAGE
# ═══════════════════════════════════════════════════════════════

class ReportsPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="TFrame")
        self.app = app
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._build()

    def _build(self):
        scroll_outer, self.scroll_inner = make_scrollable(self, orient="vertical")
        scroll_outer.grid(row=1, column=0, sticky="nsew")
        self.scroll_inner.grid_columnconfigure((0, 1), weight=1)
        self.refresh()

    def refresh(self):
        for w in self.scroll_inner.winfo_children():
            w.destroy()

        conn = get_connection()
        cur = conn.cursor()

        # Row 0: summary cards
        summary_row = tk.Frame(self.scroll_inner, bg=T["BG_DARK"])
        summary_row.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 16))
        for i in range(3):
            summary_row.grid_columnconfigure(i, weight=1)

        cur.execute("SELECT COUNT(*) FROM risks WHERE status='Активний'")
        active_r = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM measures WHERE status IN ('В процесi','Заплановано')")
        active_m = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM measures WHERE status='Виконано'")
        done_m = cur.fetchone()[0]

        for i, (title, val, col) in enumerate([
            ("Активнi ризики", active_r, T["WARNING"]),
            ("Заходiв у роботi", active_m, T["INFO"]),
            ("Виконаних заходiв", done_m, T["SUCCESS"]),
        ]):
            c = StatCard(summary_row, title, val, col)
            c.grid(row=0, column=i, sticky="ew", padx=(0, 10) if i < 2 else 0)

        # Row 1: tables
        self._make_table(
            cur, self.scroll_inner, 1, 0,
            "Ризики за категорiями",
            "SELECT category, COUNT(*), "
            "SUM(CASE WHEN status='Активний' THEN 1 ELSE 0 END) "
            "FROM risks GROUP BY category ORDER BY 2 DESC",
            ("Категорiя", "Всього", "Активних")
        )
        self._make_table(
            cur, self.scroll_inner, 1, 1,
            "Ризики за рiвнем",
            "SELECT risk_level, COUNT(*), "
            "SUM(CASE WHEN status='Активний' THEN 1 ELSE 0 END) "
            "FROM risks GROUP BY risk_level ORDER BY "
            "CASE risk_level WHEN 'Критичний' THEN 1 WHEN 'Високий' THEN 2 "
            "WHEN 'Середнiй' THEN 3 ELSE 4 END",
            ("Рiвень", "Всього", "Активних")
        )
        self._make_table(
            cur, self.scroll_inner, 2, 0,
            "Заходи за типом",
            "SELECT type, COUNT(*), "
            "SUM(CASE WHEN status='Виконано' THEN 1 ELSE 0 END) "
            "FROM measures GROUP BY type ORDER BY 2 DESC",
            ("Тип заходу", "Всього", "Виконано")
        )
        self._make_table(
            cur, self.scroll_inner, 2, 1,
            "Заходи за статусом",
            "SELECT status, COUNT(*) FROM measures GROUP BY status ORDER BY 2 DESC",
            ("Статус", "Кiлькiсть")
        )

        # Top risks
        top_card = tk.Frame(self.scroll_inner, bg=T["BG_CARD"], padx=24, pady=20)
        top_card.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(0, 16))
        top_card.grid_columnconfigure(0, weight=1)
        top_card.grid_rowconfigure(1, weight=1)
        tk.Label(top_card, text="Топ ризикiв за кiлькiстю заходiв",
                 bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 12, "bold")).grid(
                       row=0, column=0, sticky="w", pady=(0, 12))

        cols = ("Код ризику", "Назва", "Рiвень", "Кiлькiсть заходiв", "Статус")
        top_tree = ttk.Treeview(top_card, columns=cols,
                                 show="headings", height=10)
        for c in cols:
            top_tree.heading(c, text=c)
        top_tree.column("Код ризику", width=90)
        top_tree.column("Назва", width=240)
        top_tree.column("Рiвень", width=90)
        top_tree.column("Кiлькiсть заходiв", width=130)
        top_tree.column("Статус", width=90)
        top_tree.grid(row=1, column=0, sticky="ew")

        cur.execute("""
            SELECT r.risk_code, r.title, r.risk_level,
                   COUNT(m.id) as cnt, r.status
            FROM risks r
            LEFT JOIN measures m ON r.id = m.risk_id
            GROUP BY r.id ORDER BY cnt DESC LIMIT 20
        """)
        for r in cur.fetchall():
            top_tree.insert("", "end", values=tuple(r),
                            tags=(r[2],))
        for lvl, col in LEVEL_COLORS.items():
            top_tree.tag_configure(lvl, foreground=col)

        conn.close()

    def _make_table(self, cur, parent, row, col, title, sql, columns):
        card = tk.Frame(parent, bg=T["BG_CARD"], padx=24, pady=20)
        card.grid(row=row, column=col, sticky="nsew",
                  padx=(0, 10) if col == 0 else 0, pady=(0, 16))
        card.grid_columnconfigure(0, weight=1)
        card.grid_rowconfigure(1, weight=1)

        tk.Label(card, text=title, bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, sticky="w", pady=(0, 12))

        tree = ttk.Treeview(card, columns=columns,
                             show="headings", height=7)
        for c in columns:
            tree.heading(c, text=c)
            tree.column(c, width=120)
        tree.grid(row=1, column=0, sticky="ew")

        cur.execute(sql)
        for r in cur.fetchall():
            tree.insert("", "end", values=tuple(r))

# ═══════════════════════════════════════════════════════════════
# AUDIT LOG PAGE
# ═══════════════════════════════════════════════════════════════

class AuditLogPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="TFrame")
        self.app = app
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)
        self._build()

    def _build(self):
        # Header
        hdr = tk.Frame(self, bg=T["BG_DARK"])
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        hdr.grid_columnconfigure(0, weight=1)
        tk.Label(hdr, text="Журнал подiй (Audit Log)",
                 bg=T["BG_DARK"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 20, "bold")).grid(row=0, column=0, sticky="w")
        tk.Label(hdr, text="Iсторiя всiх дiй в системi",
                 bg=T["BG_DARK"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 10)).grid(row=1, column=0, sticky="w")

        # Toolbar
        toolbar = tk.Frame(self, bg=T["BG_CARD"], pady=12, padx=16)
        toolbar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        toolbar.grid_columnconfigure(1, weight=1)

        ttk.Button(toolbar, text="Оновити",
                   style="Accent.TButton",
                   command=self.refresh).grid(row=0, column=0, padx=(0, 8))

        # Filter
        sf = tk.Frame(toolbar, bg=T["BG_CARD"])
        sf.grid(row=0, column=1, sticky="w")
        tk.Label(sf, text="Фiльтр:", bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 9)).grid(row=0, column=0, padx=(0, 6))
        self.filter_entity = tk.StringVar(value="Усе")
        ttk.Combobox(sf, textvariable=self.filter_entity,
                     values=["Усе", "risk", "measure"],
                     state="readonly", width=12).grid(row=0, column=1, padx=(0, 8))
        self.filter_entity.trace_add("write", lambda *a: self.refresh())

        self.filter_action = tk.StringVar(value="Усi дiї")
        ttk.Combobox(sf, textvariable=self.filter_action,
                     values=["Усi дiї", "CREATE", "UPDATE", "DELETE", "DUPLICATE"],
                     state="readonly", width=12).grid(row=0, column=2)
        self.filter_action.trace_add("write", lambda *a: self.refresh())

        ttk.Button(toolbar, text="Очистити лог",
                   style="Danger.TButton",
                   command=self._clear_log).grid(row=0, column=2)

        tk.Frame(self, bg=T["BORDER"], height=1).grid(
            row=2, column=0, sticky="ew", pady=(0, 8))

        # Table
        tbl_frame = tk.Frame(self, bg=T["BG_DARK"])
        tbl_frame.grid(row=3, column=0, sticky="nsew")
        tbl_frame.grid_rowconfigure(0, weight=1)
        tbl_frame.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)

        cols = ("Час", "Дiя", "Об'єкт", "ID", "Деталi", "Користувач")
        self.tree = ttk.Treeview(tbl_frame, columns=cols,
                                  show="headings", selectmode="browse")
        widths = [140, 100, 80, 60, 300, 120]
        for c, w in zip(cols, widths):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w)

        vsb = ttk.Scrollbar(tbl_frame, orient="vertical",
                            command=self.tree.yview,
                            style="Vertical.TScrollbar")
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        # Color tags
        self.tree.tag_configure("CREATE", foreground=T["SUCCESS"])
        self.tree.tag_configure("UPDATE", foreground=T["INFO"])
        self.tree.tag_configure("DELETE", foreground=T["DANGER"])
        self.tree.tag_configure("DUPLICATE", foreground=T["ACCENT2"])

        self.refresh()

    def refresh(self):
        for row in self.tree.get_children():
            self.tree.delete(row)

        ent = self.filter_entity.get()
        act = self.filter_action.get()

        conn = get_connection()
        cur = conn.cursor()
        sql = "SELECT created_at, action, entity, entity_id, details, user_name FROM audit_log WHERE 1=1"
        params = []
        if ent != "Усе":
            sql += " AND entity=?"
            params.append(ent)
        if act != "Усi дiї":
            sql += " AND action=?"
            params.append(act)
        sql += " ORDER BY created_at DESC LIMIT 500"

        cur.execute(sql, params)
        for r in cur.fetchall():
            self.tree.insert("", "end",
                             values=(r[0], r[1], r[2], r[3] or "—",
                                     r[4] or "", r[5]),
                             tags=(r[1],))
        conn.close()

    def _clear_log(self):
        if messagebox.askyesno("Очищення журналу",
                               "Видалити ВСI записи з журналу?\n\nЦю дiю неможливо скасувати."):
            conn = get_connection()
            conn.execute("DELETE FROM audit_log")
            conn.commit()
            conn.close()
            ToastManager.show("Журнал очищено", "warning")
            self.refresh()

# ═══════════════════════════════════════════════════════════════
# MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════

class RiskRegistryApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Реєстр ризикiв v2.0 — Enhanced Edition")
        self.geometry("1400x850")
        self.minsize(1100, 700)
        self.configure(bg=T["BG_DARK"])

        init_db()
        ToastManager.init(self)
        apply_theme(self)
        self._build_layout()
        self._build_pages()
        self._setup_shortcuts()
        self.navigate("dashboard")
        self._start_auto_refresh()

        # Window state
        self.sidebar_collapsed = False
        self.bind("<Configure>", self._on_resize)

    def _build_layout(self):
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # Sidebar
        self.sidebar = tk.Frame(self, bg=T["BG_CARD"], width=240)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_propagate(False)
        self.sidebar.grid_rowconfigure(100, weight=1)

        # Logo
        logo_frame = tk.Frame(self.sidebar, bg=T["BG_CARD"])
        logo_frame.grid(row=0, column=0, sticky="ew", padx=20, pady=(28, 8))
        tk.Label(logo_frame, text="⚡ RiskRegistry",
                 bg=T["BG_CARD"], fg=T["TEXT_PRI"],
                 font=("Segoe UI", 16, "bold")).pack(anchor="w")
        tk.Label(logo_frame, text="v2.0 Enhanced  |  2026",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 8)).pack(anchor="w")

        tk.Frame(self.sidebar, bg=T["BORDER"], height=1).grid(
            row=1, column=0, sticky="ew", padx=16, pady=(12, 20))

        # Nav
        nav_items = [
            ("dashboard",  "Панель",       "📊"),
            ("registry",   "Ризики",       "📋"),
            ("risk_form",  "+ Ризик",      "➕"),
            ("measures",   "Заходи",       "🛡️"),
            ("measure_form", "+ Захiд",    "✚"),
            ("reports",    "Звiти",        "📈"),
            ("audit",      "Аудит",        "📜"),
        ]
        self.nav_btns = {}
        for i, (key, label, icon) in enumerate(nav_items):
            btn = tk.Button(
                self.sidebar,
                text=f"  {icon}  {label}",
                bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                activebackground=T["ACCENT"],
                activeforeground="white",
                font=("Segoe UI", 10),
                anchor="w", bd=0, padx=12, pady=12,
                relief="flat", cursor="hand2",
                command=lambda k=key: self.navigate(k)
            )
            btn.grid(row=i + 2, column=0, sticky="ew", padx=10, pady=2)
            self.nav_btns[key] = btn

        # Footer
        tk.Frame(self.sidebar, bg=T["BORDER"], height=1).grid(
            row=100, column=0, sticky="ew", padx=16, pady=12)

        footer_f = tk.Frame(self.sidebar, bg=T["BG_CARD"])
        footer_f.grid(row=101, column=0, sticky="ew", padx=16, pady=(0, 16))

        ttk.Button(footer_f, text="⚙ Налаштування",
                   style="Ghost.TButton",
                   command=lambda: SettingsDialog(self, self)).pack(fill="x", pady=(0, 4))

        theme_btn = ttk.Button(footer_f, text="🌙 Змiнити тему",
                               style="Ghost.TButton",
                               command=self._toggle_theme)
        theme_btn.pack(fill="x", pady=(0, 8))

        tk.Label(footer_f, text="© 2026 Risk Analytics",
                 bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                 font=("Segoe UI", 8)).pack()

        # Main content
        self.content = tk.Frame(self, bg=T["BG_DARK"], padx=28, pady=24)
        self.content.grid(row=0, column=1, sticky="nsew")
        self.content.grid_columnconfigure(0, weight=1)
        self.content.grid_rowconfigure(0, weight=1)

        # Status bar
        self.statusbar = tk.Frame(self, bg=T["BG_CARD"], height=32)
        self.statusbar.grid(row=1, column=0, columnspan=2, sticky="ew")
        self.statusbar.grid_propagate(False)

        self.status_left = tk.Label(self.statusbar, text="Готово",
                                     bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                                     font=("Segoe UI", 9))
        self.status_left.pack(side="left", padx=16)

        self.status_right = tk.Label(self.statusbar, text="",
                                      bg=T["BG_CARD"], fg=T["TEXT_SEC"],
                                      font=("Segoe UI", 9))
        self.status_right.pack(side="right", padx=16)

    def _build_pages(self):
        self.pages = {}

        self.dashboard    = DashboardPage(self.content, self)
        self.registry     = RegistryPage(self.content, self)
        self.risk_form    = RiskFormPage(self.content, self)
        self.measure_form = MeasureFormPage(self.content, self)
        self.measures     = MeasuresPage(self.content, self)
        self.reports_pg   = ReportsPage(self.content, self)
        self.audit_pg     = AuditLogPage(self.content, self)

        for key, pg in [
            ("dashboard",    self.dashboard),
            ("registry",     self.registry),
            ("risk_form",    self.risk_form),
            ("measure_form", self.measure_form),
            ("measures",     self.measures),
            ("reports",      self.reports_pg),
            ("audit",        self.audit_pg),
        ]:
            pg.grid(row=0, column=0, sticky="nsew")
            self.pages[key] = pg

    def _setup_shortcuts(self):
        self.bind("<Control-n>", lambda e: self.navigate("risk_form"))
        self.bind("<Control-f>", lambda e: self._focus_search())
        self.bind("<Control-r>", lambda e: self.refresh_all())
        self.bind("<F5>", lambda e: self.refresh_all())
        self.bind("<Escape>", lambda e: self.navigate("dashboard"))

    def _focus_search(self):
        # Try to focus search box on current page
        try:
            if hasattr(self.registry, "search_var"):
                # Find the entry widget - would need to store reference
                pass
        except Exception:
            pass

    def navigate(self, page_key: str):
        for key, btn in self.nav_btns.items():
            btn.configure(
                bg=T["ACCENT"] if key == page_key else T["BG_CARD"],
                fg="white" if key == page_key else T["TEXT_SEC"])

        for key, pg in self.pages.items():
            if key == page_key:
                pg.tkraise()

        if page_key == "risk_form" and not self.risk_form.edit_id:
            self.risk_form._reset()
        if page_key == "measure_form" and not self.measure_form.edit_id:
            self.measure_form._reset()

        page_names = {
            "dashboard": "Панель управлiння",
            "registry": "Реєстр ризикiв",
            "risk_form": "Форма ризику",
            "measure_form": "Форма заходу",
            "measures": "Заходи мiнiмiзацiї",
            "reports": "Звiти",
            "audit": "Журнал аудиту"
        }
        self.status_left.configure(text=page_names.get(page_key, "Готово"))

    def refresh_all(self):
        self.dashboard.refresh()
        self.registry.refresh()
        self.measures.refresh()
        self.reports_pg.refresh()
        self.audit_pg.refresh()
        self._update_status()
        ToastManager.show("Данi оновлено", "info", 1500)

    def _update_status(self):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM risks")
        r_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM measures")
        m_count = cur.fetchone()[0]
        conn.close()
        self.status_right.configure(
            text=f"Ризикiв: {r_count}  |  Заходiв: {m_count}  |  Користувач: {get_setting('user_name')}")

    def _toggle_theme(self):
        current = get_setting("theme", "dark")
        new = "light" if current == "dark" else "dark"
        set_setting("theme", new)
        ToastManager.show("Тема буде змiнена пiсля перезапуску", "info")

    def _on_resize(self, event):
        w = self.winfo_width()
        if w < 1200 and not self.sidebar_collapsed:
            self.sidebar.configure(width=60)
            for btn in self.nav_btns.values():
                text = btn.cget("text")
                icon = text.split()[0] if text else "•"
                btn.configure(text=f" {icon}", padx=8)
            self.sidebar_collapsed = True
        elif w >= 1200 and self.sidebar_collapsed:
            self.sidebar.configure(width=240)
            nav_items = [
                ("dashboard",  "Панель",    "📊"),
                ("registry",   "Ризики",    "📋"),
                ("risk_form",  "+ Ризик",   "➕"),
                ("measures",   "Заходи",    "🛡️"),
                ("measure_form", "+ Захiд", "✚"),
                ("reports",    "Звiти",     "📈"),
                ("audit",      "Аудит",     "📜"),
            ]
            for (key, label, icon) in nav_items:
                if key in self.nav_btns:
                    self.nav_btns[key].configure(
                        text=f"  {icon}  {label}", padx=12)
            self.sidebar_collapsed = False

    def _start_auto_refresh(self):
        interval = get_setting("auto_refresh", "30")
        try:
            seconds = int(interval)
            if seconds > 0:
                self.after(seconds * 1000, self._auto_refresh_loop)
        except Exception:
            pass

    def _auto_refresh_loop(self):
        self.refresh_all()
        self._start_auto_refresh()

# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        app = RiskRegistryApp()
        app._update_status()
        app.mainloop()
    except Exception as e:
        import traceback
        print("CRITICAL ERROR:")
        print(traceback.format_exc())
        messagebox.showerror("Критична помилка",
                             f"Програма не може запуститися:\n\n{str(e)}")
                             
