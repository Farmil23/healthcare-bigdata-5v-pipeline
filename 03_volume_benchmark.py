"""
LANGKAH 3 - Bukti VOLUME (ukuran data vs kompleksitas struktur vs waktu proses).

Dua percobaan, hasilnya ke data/processed/benchmark.json (dibaca tab Volume di Streamlit):

 [1] SKALA : pekerjaan teks yang sama (bersihkan -> ekstrak kata kunci -> hitung) dengan
             Python serial (1 core) vs PySpark paralel, pada ukuran data yang bertambah.
 [2] STRUKTUR : Operasi A (agregasi sederhana, 1 dataset) vs Operasi B (join 3 dataset + analisis
             harian) di Spark, sesuai Tabel 8.2 pada soal. Tujuannya memisahkan efek JUMLAH BARIS
             dari efek STRUKTUR/RELASI query.
"""
import contextlib
import io
import json
import re
import statistics
import time
from collections import Counter
from datetime import datetime

from pyspark.sql import functions as F

from config import RAW, BENCH_DIR, PROCESSED, KEYWORDS, VOLUME_COPIES_LIST, CARDIAC_REGEX
from spark_session import get_spark

SRC = RAW / "clinical_notes.txt"
PATTERN = "|".join(KEYWORDS)
rx = re.compile(PATTERN)


# ---------------------------------------------------------------- [1] SKALA
def buat_file(copies: int):
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    f = BENCH_DIR / f"notes_x{copies}.txt"
    data = SRC.read_bytes()
    with open(f, "wb") as out:
        for _ in range(copies):
            out.write(data)
    return f


def jalankan_serial(file):
    t0, hasil, n = time.time(), Counter(), 0
    with open(file, encoding="utf-8") as f:
        for line in f:
            n += 1
            teks = line.split("|")[-1]
            teks = re.sub(r"<[^>]+>", " ", teks)
            teks = re.sub(r"[^a-zA-Z0-9., ]", " ", teks)
            teks = re.sub(r"\s+", " ", teks).strip().lower()
            m = rx.search(teks)
            if m:
                hasil[m.group(0)] += 1
    return time.time() - t0, n, dict(hasil)


def jalankan_spark(spark, file):
    t0 = time.time()
    df = spark.read.text(str(file))
    teks = F.element_at(F.split("value", r"\|"), -1)
    teks = F.regexp_replace(teks, r"<[^>]+>", " ")
    teks = F.regexp_replace(teks, r"[^a-zA-Z0-9., ]", " ")
    teks = F.regexp_replace(teks, r"\s+", " ")
    kw = F.regexp_extract(F.lower(F.trim(teks)), f"({PATTERN})", 1)
    rows = df.select(kw.alias("keyword")).filter("keyword != ''").groupBy("keyword").count().collect()
    return time.time() - t0, {r["keyword"]: r["count"] for r in rows}, df.rdd.getNumPartitions()


# ---------------------------------------------------------------- [2] STRUKTUR
def op_a(spark):
    """Operasi A: agregasi sederhana, satu dataset (jumlah catatan per pasien)."""
    return spark.read.parquet(str(PROCESSED / "notes.parquet")).groupBy("patient_id").count()


def op_b(spark):
    """Operasi B: relasional + temporal. vitals (harian) JOIN notes (harian) JOIN patients, lalu per riwayat & hari."""
    v = spark.read.parquet(str(PROCESSED / "vitals.parquet"))
    n = spark.read.parquet(str(PROCESSED / "notes.parquet"))
    p = spark.read.parquet(str(PROCESSED / "patients.parquet")).select("patient_id", "riwayat_penyakit")
    vd = v.groupBy("patient_id", F.to_date("timestamp").alias("tanggal")).agg(
        F.avg("heart_rate").alias("hr"), F.count("*").alias("n_v"))
    nd = n.groupBy("patient_id", F.to_date("tanggal").alias("tanggal")).agg(
        F.sum(F.col("catatan").rlike(CARDIAC_REGEX).cast("int")).alias("keluhan"))
    j = vd.join(nd, ["patient_id", "tanggal"]).join(p, "patient_id")
    return j.groupBy("riwayat_penyakit", "tanggal").agg(
        F.round(F.avg("hr"), 1).alias("avg_hr"), F.sum("keluhan").alias("keluhan"), F.sum("n_v").alias("n_v"))


def n_exchange(df):
    """Hitung tahap Exchange (pertukaran data antar partisi / shuffle) pada rencana eksekusi."""
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            df.explain()
        return buf.getvalue().count("Exchange")
    except Exception:
        return None


def ukur(fn, spark, runs=3):
    fn(spark).collect()                                   # pemanasan (perencanaan query + JIT) tidak dihitung
    df = fn(spark)
    plan = n_exchange(df)
    waktu = []
    for _ in range(runs):
        t0 = time.time()
        hasil = fn(spark).collect()
        waktu.append(round(time.time() - t0, 3))
    return {"median_s": round(statistics.median(waktu), 3), "semua_s": waktu, "baris_hasil": len(hasil), "exchange": plan}


spark = get_spark("Volume")
spark.range(1).count()                                    # pemanasan JVM
cores = spark.sparkContext.defaultParallelism

hasil = []
for copies in VOLUME_COPIES_LIST:
    file = buat_file(copies)
    print(f"\n--- [1] Skala x{copies} ---")
    waktu_a, n_rows, a = jalankan_serial(file)
    waktu_b, b, partisi = jalankan_spark(spark, file)
    file.unlink()
    hasil.append({"rows": n_rows, "serial_s": round(waktu_a, 2), "spark_s": round(waktu_b, 2),
                  "speedup": round(waktu_a / waktu_b, 2), "partisi": partisi, "hasil_sama": a == b})
    print(f"    {n_rows:,} baris | serial {waktu_a:.2f}s | spark {waktu_b:.2f}s | speed-up {waktu_a / waktu_b:.2f}x")
if BENCH_DIR.exists():
    try:
        BENCH_DIR.rmdir()
    except OSError:
        pass

print("\n--- [2] Struktur: Operasi A vs B ---")
n_pat = spark.read.parquet(str(PROCESSED / "patients.parquet")).count()
n_vit = spark.read.parquet(str(PROCESSED / "vitals.parquet")).count()
n_not = spark.read.parquet(str(PROCESSED / "notes.parquet")).count()
A, B = ukur(op_a, spark), ukur(op_b, spark)
A.update({"nama": "A - Agregasi sederhana", "dataset": 1, "join": 0, "baris_input": n_not,
          "deskripsi": "Hitung catatan per pasien (groupBy pada 1 dataset)."})
B.update({"nama": "B - Relasional + temporal", "dataset": 3, "join": 2, "baris_input": n_pat + n_vit + n_not,
          "deskripsi": "Agregasi harian vitals dan notes, join ke patients, lalu ringkas per riwayat penyakit & tanggal."})
for x in (A, B):
    print(f"    {x['nama']:<28} {x['median_s']:>6}s | input {x['baris_input']:,} | Exchange {x['exchange']}")

(PROCESSED / "benchmark.json").write_text(json.dumps(
    {"dibuat": datetime.now().isoformat(timespec="seconds"), "cores": cores, "hasil": hasil,
     "operasi": {"A": A, "B": B}}, indent=2, ensure_ascii=False), encoding="utf-8")
print("\nHasil disimpan ke data/processed/benchmark.json")
spark.stop()
