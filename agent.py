"""
LANGKAH 6 - Agen AI dengan LangGraph.   Bukti: V5 VALUE.

Alur graph:

    START -> router --+--> sql_agent --+--> rag_agent --> answer --> END
                      |                |                   ^
                      |                +-------------------+   (jika hanya perlu SQL)
                      +--> rag_agent ----------------------+   (jika hanya perlu teks)

  router    : memutuskan pertanyaan butuh data angka (SQL), teks (RAG), atau keduanya
  sql_agent : Spark SQL ke Parquet (tren sensor 7 hari terakhir)
  rag_agent : mencari catatan klinis paling relevan (TF-IDF + cosine similarity)
  answer    : menyusun jawaban (LLM Groq jika ada API key, kalau tidak pakai template)
"""
import os
import re
from pathlib import Path
from typing import TypedDict

import pandas as pd
from langgraph.graph import StateGraph, START, END
from pyspark.sql import functions as F
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from config import BASE, PROCESSED
from spark_session import get_spark, to_pandas



# ------------------------------------------------ pembaca .env (tanpa library tambahan)
def _decode(raw: bytes) -> str:
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):          # file UTF-16 (sering dari PowerShell '>')
        return raw.decode("utf-16", errors="ignore")
    return raw.decode("utf-8-sig", errors="ignore")      # utf-8-sig membuang BOM dari Notepad


def load_env(folders) -> str:
    """Baca .env dari tiap folder ke os.environ (menimpa nilai lama). Kembalikan teks diagnosis
    berisi NAMA variabel saja, tidak pernah nilainya."""
    info = []
    for folder in dict.fromkeys(Path(f) for f in folders):
        f = folder / ".env"
        mirip = sorted(p.name for p in folder.glob(".env*") if p.name != ".env.example")
        if not f.is_file():
            info.append(f"{f} tidak ada (file mirip: {mirip or 'tidak ada'})")
            continue
        names = []
        for line in _decode(f.read_bytes()).splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.replace("export ", "").strip()
            v = v.strip().strip('"').strip("'")
            if v:
                os.environ[k] = v
                names.append(k)
            else:
                names.append(f"{k} (KOSONG)")
        info.append(f"{f} ada, variabel di dalamnya: {names}")
    return " | ".join(info)


class AgentState(TypedDict, total=False):
    question: str
    route: str          # "sql" | "rag" | "both"
    days: int           # periode analisis (hari ke belakang), dibaca dari pertanyaan
    patients: list      # hasil sql_agent
    evidence: list      # hasil rag_agent
    answer: str
    llm_mode: str       # LLM mana yang dipakai (atau kenapa jatuh ke template)
    trace: list         # jejak node yang dilewati (untuk ditunjukkan di video)


def _spark_with_views():
    """Daftarkan Parquet hasil Spark sebagai tabel SQL."""
    spark = get_spark("Agent")
    for name in ["patients", "vitals", "notes"]:
        spark.read.parquet(str(PROCESSED / f"{name}.parquet")).createOrReplaceTempView(name)
    return spark


# ---------------------------------------------------------------- NODE 1: ROUTER
def _parse_days(q: str) -> int:
    """Baca periode dari pertanyaan: '30 hari', '2 minggu', 'bulan ini', 'hari ini'. Default 7 hari."""
    m = re.search(r"(\d+)\s*(hari|minggu|bulan)", q)
    if m:
        return int(m.group(1)) * {"hari": 1, "minggu": 7, "bulan": 30}[m.group(2)]
    if "hari ini" in q:
        return 1
    if "bulan" in q:
        return 30
    return 7


def router_node(state: AgentState) -> AgentState:
    q = state["question"].lower()
    need_sql = any(k in q for k in ["tren", "rata", "detak", "suhu", "spo2", "iot", "risiko", "kelompok"])
    need_rag = any(k in q for k in ["catatan", "keluhan", "nyeri", "observasi", "klinis"])
    route = "both" if (need_sql and need_rag) else "rag" if need_rag else "sql"
    days = _parse_days(q)
    return {"route": route, "days": days, "trace": [f"router (route={route}, periode={days} hari)"]}


# ---------------------------------------------------------------- NODE 2: SQL AGENT
SQL = """
SELECT v.patient_id, p.umur, p.riwayat_penyakit,
       ROUND(AVG(v.heart_rate), 1) AS avg_hr,
       ROUND(AVG(v.spo2), 1)       AS avg_spo2,
       ROUND(MAX(v.temperature),1) AS max_temp,
       COUNT(*)                    AS n_pembacaan
FROM vitals v JOIN patients p ON v.patient_id = p.patient_id
WHERE to_timestamp(v.timestamp) >= date_sub(current_date(), {days})
GROUP BY v.patient_id, p.umur, p.riwayat_penyakit
HAVING AVG(v.heart_rate) > 100
ORDER BY avg_hr DESC
LIMIT 10
"""


def sql_node(state: AgentState) -> AgentState:
    df = to_pandas(_spark_with_views().sql(SQL.format(days=state["days"])))
    return {"patients": df.to_dict("records"), "trace": state["trace"] + ["sql_agent (Spark SQL)"]}


# ---------------------------------------------------------------- NODE 3: RAG AGENT
def rag_node(state: AgentState) -> AgentState:
    spark = _spark_with_views()
    notes = spark.table("notes").filter(F.to_date("tanggal") >= F.date_sub(F.current_date(), state["days"]))
    ids = [p["patient_id"] for p in state.get("patients", [])]
    if ids:                                             # fokus ke pasien hasil SQL
        notes = notes.filter(F.col("patient_id").isin(ids))
    docs = to_pandas(notes.select("patient_id", "catatan").distinct().limit(20000))

    # TF-IDF bersifat leksikal, jadi pertanyaan diperluas dengan kata kunci gejala jantung.
    query = state["question"] + " nyeri dada sesak napas berdebar keringat dingin"
    vec = TfidfVectorizer()
    X = vec.fit_transform(docs["catatan"])
    docs["skor"] = cosine_similarity(vec.transform([query]), X)[0].round(3)

    top = docs.sort_values("skor", ascending=False)
    top = top.groupby("patient_id").head(2) if ids else top.head(5)
    return {"evidence": top.to_dict("records"), "trace": state["trace"] + ["rag_agent (TF-IDF retrieval)"]}


# ---------------------------------------------------------------- NODE 4: ANSWER
def _template_answer(state: AgentState) -> str:
    pts, ev = state.get("patients", []), state.get("evidence", [])
    parts = []
    if pts:
        parts.append(f"Ditemukan {len(pts)} pasien dengan rata-rata detak jantung >100 bpm dalam {state.get('days', 7)} hari terakhir.")
        for p in pts[:3]:
            parts.append(f"- {p['patient_id']}: HR rata-rata {p['avg_hr']} bpm, SpO2 {p['avg_spo2']}%, "
                         f"riwayat {p['riwayat_penyakit']}.")
    if ev:
        parts.append("Contoh keluhan relevan dari catatan klinis:")
        for e in ev[:3]:
            parts.append(f"- {e['patient_id']}: \"{e['catatan']}\"")
    parts.append("Catatan: ini alat bantu keputusan, bukan diagnosis. Keputusan akhir tetap pada dokter.")
    return "\n".join(parts)


def _build_prompt(state: AgentState) -> str:
    return (f"Pertanyaan dokter: {state['question']}\n\nPeriode data: {state.get('days', 7)} hari terakhir.\n\n"
            f"Data sensor (Spark SQL): {state.get('patients', [])}\n\n"
            f"Catatan klinis relevan (RAG): {state.get('evidence', [])}\n\n"
            "Buat ringkasan klinis singkat dalam Bahasa Indonesia. Kelompokkan pasien menjadi risiko tinggi "
            "dan risiko sedang berdasarkan data, beri alasan singkat per kelompok. Gunakan HANYA data di atas, "
            "jangan mengarang. Ini alat bantu keputusan, bukan diagnosis.")


def _call_llm(prompt: str):
    """Kembalikan (teks, nama_llm). Prioritas: Groq, lalu Gemini. Tanpa key -> (None, keterangan)."""
    env_info = load_env([BASE, Path.cwd(), BASE.parent])   # dibaca ulang tiap pertanyaan
    if os.getenv("GROQ_API_KEY"):
        from groq import Groq
        model = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
        resp = Groq().chat.completions.create(model=model, messages=[{"role": "user", "content": prompt}])
        return resp.choices[0].message.content, f"Groq ({model})"
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if key:
        from google import genai
        model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        resp = genai.Client(api_key=key).models.generate_content(model=model, contents=prompt)
        return resp.text, f"Gemini ({model})"
    return None, f"Template (GROQ_API_KEY / GEMINI_API_KEY tidak terbaca). Diagnosis: {env_info}"


def answer_node(state: AgentState) -> AgentState:
    answer, mode = _template_answer(state), ""
    try:
        text, mode = _call_llm(_build_prompt(state))
        if text:
            answer = text
    except Exception as e:                              # LLM gagal -> tetap pakai template
        mode = f"Template (LLM gagal: {type(e).__name__}: {e})"
    return {"answer": answer, "llm_mode": mode, "trace": state["trace"] + ["answer"]}


# ---------------------------------------------------------------- GRAPH
def build_graph():
    g = StateGraph(AgentState)
    g.add_node("router", router_node)
    g.add_node("sql_agent", sql_node)
    g.add_node("rag_agent", rag_node)
    g.add_node("answer", answer_node)

    g.add_edge(START, "router")
    g.add_conditional_edges("router", lambda s: "rag_agent" if s["route"] == "rag" else "sql_agent",
                            {"rag_agent": "rag_agent", "sql_agent": "sql_agent"})
    g.add_conditional_edges("sql_agent", lambda s: "rag_agent" if s["route"] == "both" else "answer",
                            {"rag_agent": "rag_agent", "answer": "answer"})
    g.add_edge("rag_agent", "answer")
    g.add_edge("answer", END)
    return g.compile()


_graph = build_graph()


def ask(question: str) -> dict:
    return _graph.invoke({"question": question})


if __name__ == "__main__":      # uji cepat lewat terminal: python agent.py
    q = ("Berdasarkan tren data IoT terbaru dan catatan klinis minggu ini, kelompokkan pasien "
         "yang memiliki risiko tinggi terkena penyakit jantung, dan berikan ringkasan kondisinya.")
    hasil = ask(q)
    print(" -> ".join(hasil["trace"]))
    print(pd.DataFrame(hasil.get("patients", [])))
    print(f"[Mode jawaban: {hasil['llm_mode']}]")
    print(hasil["answer"])