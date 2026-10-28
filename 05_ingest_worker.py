"""
LANGKAH 5 - PEKERJA INGEST (jalankan di TERMINAL TERPISAH, bersama 04_stream_generator.py).
Bukti utama: VELOCITY (frekuensi kedatangan, latency, throughput). Bisa juga dijalankan dari tombol di dashboard.

Memantau data/incoming/. Setiap ada file baru:
  1. Spark membaca sesuai tipenya                     -> VARIETY  (JSON / TXT / CSV)
  2. Spark memvalidasi & membersihkan                 -> VERACITY (SpO2 mustahil, duplikat, tag HTML)
  3. Hasil bersih ditambahkan ke data/live/*.csv      -> VOLUME   (store terus bertambah)
  4. Spark menghitung rata-rata detak per window 1 menit, dan mencatat latency -> VELOCITY
  5. Statistik tiap batch dicatat ke data/live/stats.jsonl (dibaca dashboard Streamlit)

Agen AI (agent.py) membaca Parquet batch + data live ini -> VALUE.
Catatan: pola micro-batch manual (aman di Windows tanpa Hadoop), bukan API Structured Streaming.
"""
import json
import shutil
import time
from datetime import datetime

from pyspark.sql import functions as F

from config import INCOMING, LIVE
from spark_session import get_spark, to_pandas

V_SCHEMA = "patient_id STRING, timestamp TIMESTAMP, heart_rate INT, temperature DOUBLE, spo2 INT"
P_SCHEMA = "patient_id STRING, umur INT, gender STRING, riwayat_penyakit STRING"
W_SCHEMA = "patient_id STRING, timestamp STRING, heart_rate INT, temperature DOUBLE, spo2 INT"

shutil.rmtree(LIVE, ignore_errors=True)       # mulai bersih: statistik hanya untuk sesi ini
LIVE.mkdir(parents=True, exist_ok=True)
INCOMING.mkdir(parents=True, exist_ok=True)
spark = get_spark("Ingest", memory="1g")
spark.range(1).count()                      # pemanasan JVM agar latency batch pertama tidak membengkak
(LIVE / "ready.flag").write_text("ok")       # tanda untuk dashboard: worker siap menerima data


def simpan(pdf, nama):
    """Tambahkan baris ke live store (CSV append). Header hanya ditulis sekali."""
    f = LIVE / f"{nama}.csv"
    pdf.to_csv(f, mode="a", header=(not f.exists() or f.stat().st_size == 0), index=False, encoding="utf-8")


def proses_vitals(paths, st):
    df = spark.read.schema(V_SCHEMA).json(paths)                          # VARIETY: JSON
    kosong = F.col("heart_rate").isNull() | F.col("spo2").isNull() | F.col("temperature").isNull()
    spo2_x = ~kosong & ~F.col("spo2").between(70, 100)                    # VERACITY: nilai mustahil
    hr_x = ~kosong & ~F.col("heart_rate").between(30, 220)
    suhu_x = ~kosong & ~F.col("temperature").between(34, 42)
    valid = ~(kosong | spo2_x | hr_x | suhu_x)
    a = df.agg(F.count("*").alias("n"),
               F.sum(kosong.cast("int")).alias("kosong"), F.sum(spo2_x.cast("int")).alias("spo2"),
               F.sum(hr_x.cast("int")).alias("hr"), F.sum(suhu_x.cast("int")).alias("suhu"),
               F.sum(valid.cast("int")).alias("valid")).first()
    bersih = (df.filter(valid).dropDuplicates()
              .withColumn("timestamp", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss")))
    pdf = to_pandas(bersih)
    st.update(vit_in=a["n"], vit_ok=len(pdf), rej_null=a["kosong"] or 0, rej_spo2=a["spo2"] or 0,
              rej_hr=a["hr"] or 0, rej_temp=a["suhu"] or 0, dup_vit=(a["valid"] or 0) - len(pdf),
              hr_avg=round(float(pdf["heart_rate"].mean()), 1) if len(pdf) else None)
    if len(pdf):
        simpan(pdf[["patient_id", "timestamp", "heart_rate", "temperature", "spo2"]], "vitals")  # VOLUME


def proses_notes(paths, st):
    p = F.split(F.col("value"), r"\|")                                    # VARIETY: TXT
    raw = spark.read.text(paths).select(p[0].alias("patient_id"), p[1].alias("tanggal"),
                                        p[2].alias("catatan_mentah"))
    teks = F.regexp_replace("catatan_mentah", r"<[^>]+>", " ")            # VERACITY: buang tag HTML
    teks = F.regexp_replace(teks, r"[^a-zA-Z0-9., ]", " ")                #           karakter aneh
    teks = F.regexp_replace(teks, r"\s+", " ")                            #           spasi berlebih
    bersih = raw.withColumn("catatan", F.lower(F.trim(teks))).filter(F.length("catatan") > 0)
    a = raw.agg(F.count("*").alias("n"),
                F.sum(F.col("catatan_mentah").rlike("<[^>]+>").cast("int")).alias("html"),
                F.sum(F.col("catatan_mentah").rlike("[#@]").cast("int")).alias("khusus")).first()
    pdf = to_pandas(bersih)
    ex_raw = ex_clean = ""
    if len(pdf):
        kotor = pdf[pdf["catatan_mentah"].str.contains("<|#", regex=True)]
        if len(kotor):
            ex_raw, ex_clean = kotor.iloc[0]["catatan_mentah"], kotor.iloc[0]["catatan"]
    st.update(not_in=a["n"], not_ok=len(pdf), not_html=a["html"] or 0, not_special=a["khusus"] or 0,
              ex_raw=ex_raw, ex_clean=ex_clean)
    if len(pdf):
        simpan(pdf[["patient_id", "tanggal", "catatan"]], "notes")


def proses_patients(paths, st):
    df = spark.read.csv(paths, header=True, schema=P_SCHEMA)              # VARIETY: CSV
    a = df.agg(F.count("*").alias("n"), F.sum(F.col("umur").isNull().cast("int")).alias("kosong")).first()
    pdf = to_pandas(df.dropDuplicates(["patient_id"]).na.fill({"umur": 50}))   # VERACITY
    st.update(pat_in=a["n"], pat_ok=len(pdf), pat_dup=a["n"] - len(pdf), pat_null=a["kosong"] or 0)
    if len(pdf):
        simpan(pdf, "patients")


def hitung_window():
    """VELOCITY: rata-rata detak jantung per tumbling window 1 menit (Spark window)."""
    f = LIVE / "vitals.csv"
    if not f.exists():
        return
    df = spark.read.csv(str(f), header=True, schema=W_SCHEMA).withColumn("ts", F.to_timestamp("timestamp"))
    w = (df.groupBy(F.window("ts", "1 minute"))
         .agg(F.round(F.avg("heart_rate"), 1).alias("avg_hr"), F.count("*").alias("n"))
         .orderBy(F.desc("window")).limit(8))
    rows = [{"jendela": r["window"]["start"].strftime("%H:%M"), "avg_hr": r["avg_hr"], "n": r["n"]}
            for r in w.collect()]
    (LIVE / "window.json").write_text(json.dumps(rows[::-1]), encoding="utf-8")


def mtime_terbaru(files):
    t = []
    for p in files:
        try:
            t.append(p.stat().st_mtime)
        except OSError:
            pass
    return max(t) if t else time.time()


seen, batch_id, vit_mt = set(), 0, []        # vit_mt: waktu tiba file vitals (untuk frekuensi kedatangan)
print(f"Memantau {INCOMING} ... (Ctrl+C untuk berhenti)")
while True:
    try:
        baru = sorted(p for p in INCOMING.iterdir()
                      if p.is_file() and not p.name.startswith(".")
                      and p.suffix in (".json", ".txt", ".csv") and p not in seen)
    except OSError:                              # folder sedang dibuat ulang oleh simulator
        INCOMING.mkdir(parents=True, exist_ok=True)
        time.sleep(1)
        continue
    if not baru:
        time.sleep(1)
        continue
    seen.update(baru)
    batch_id += 1
    t0 = time.time()
    tiba = mtime_terbaru(baru)
    for p in baru:
        if p.name.startswith("vitals_"):
            try:
                vit_mt.append(p.stat().st_mtime)
            except OSError:
                pass
    vit_mt.sort()
    selisih = [b - a for a, b in zip(vit_mt[-31:-1], vit_mt[-30:])]
    st = {"ts": f"{datetime.now():%Y-%m-%d %H:%M:%S}", "batch": batch_id, "files": len(baru),
          "vit_in": 0, "vit_ok": 0, "rej_null": 0, "rej_spo2": 0, "rej_hr": 0, "rej_temp": 0,
          "dup_vit": 0, "hr_avg": None, "not_in": 0, "not_ok": 0, "not_html": 0, "not_special": 0,
          "pat_in": 0, "pat_ok": 0, "pat_dup": 0, "pat_null": 0, "ex_raw": "", "ex_clean": ""}
    try:
        v = [str(p) for p in baru if p.name.startswith("vitals_")]
        n = [str(p) for p in baru if p.name.startswith("notes_")]
        c = [str(p) for p in baru if p.name.startswith("patients_")]
        if v:
            proses_vitals(v, st)
        if n:
            proses_notes(n, st)
        if c:
            proses_patients(c, st)
        hitung_window()
    except Exception as e:                      # jangan matikan pekerja karena satu batch gagal
        st["error"] = str(e)[:200]
        print("ERROR batch", batch_id, ":", str(e)[:300])
    st["arr_interval"] = round(sorted(selisih)[len(selisih) // 2], 2) if selisih else None   # median jeda kedatangan (detik)
    st["queue_s"] = round(t0 - tiba, 2)         # menunggu giliran (data tiba -> mulai diproses)
    st["spark_s"] = round(time.time() - t0, 2)    # waktu proses Spark
    st["latency"] = round(time.time() - tiba, 2)
    with open(LIVE / "stats.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(st, ensure_ascii=False) + "\n")
    print(f">>> batch {batch_id} | {len(baru)} file | vitals {st['vit_in']}->{st['vit_ok']} | "
          f"notes {st['not_in']}->{st['not_ok']} | pasien {st['pat_in']}->{st['pat_ok']} | "
          f"latency {st['latency']} dtk")