"""
LANGKAH 2 - ETL dengan PySpark.   Bukti: VARIETY, VERACITY, dan sebagian VOLUME.

Keluaran:
  data/processed/*.parquet          data bersih (dipakai Spark SQL dan agen AI)
  data/processed/variety_report.json  skema 3 format + cara baca + hasil integrasi
  data/processed/quality_report.json  masalah kualitas: SEBELUM vs SESUDAH + alasan penanganan
  data/processed/volume_report.json   jumlah baris, ukuran file, relasi antar data
Semua laporan dibaca dashboard Streamlit (app.py).
"""
import json
import time
from datetime import datetime

from pyspark.sql import functions as F

from config import RAW, PROCESSED, CARDIAC_REGEX
from spark_session import get_spark, to_pandas

T0 = time.time()
spark = get_spark("ETL")
PROCESSED.mkdir(parents=True, exist_ok=True)


def mb(path) -> float:
    return round(path.stat().st_size / 1e6, 2)


def records(df, n=3):
    return json.loads(to_pandas(df.limit(n)).to_json(orient="records", date_format="iso"))


def schema_of(df):
    return [{"kolom": f.name, "tipe": f.dataType.simpleString()} for f in df.schema.fields]


def dump(name, obj):
    obj = {"dibuat": datetime.now().isoformat(timespec="seconds"), **obj}
    (PROCESSED / name).write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


# =====================================================================
# VARIETY : satu script Spark membaca 3 format yang berbeda
# =====================================================================
print("\n" + "=" * 70 + "\n  VARIETY - membaca CSV, JSON, dan TXT\n" + "=" * 70)
patients = spark.read.csv(str(RAW / "patients.csv"), header=True, inferSchema=True)   # CSV
vitals = spark.read.json(str(RAW / "vitals.json"))                                      # JSON lines
notes_raw = spark.read.text(str(RAW / "clinical_notes.txt"))                            # TXT (1 kolom "value")

parts = F.split(F.col("value"), r"\|")                       # TXT dipecah per "|" menjadi kolom
notes = notes_raw.select(parts[0].alias("patient_id"), parts[1].alias("tanggal"),
                         parts[2].alias("catatan_mentah"))

n_pat_raw, n_vit_raw, n_not_raw = patients.count(), vitals.count(), notes.count()
print(f"CSV {n_pat_raw:,} baris | JSON {n_vit_raw:,} baris | TXT {n_not_raw:,} baris")
patients.show(3, truncate=False); vitals.show(3, truncate=False); notes.show(3, truncate=70)

# =====================================================================
# VERACITY : ukur masalah SEBELUM, bersihkan, ukur lagi SESUDAH
# =====================================================================
print("\n" + "=" * 70 + "\n  VERACITY - pembersihan kualitas data\n" + "=" * 70)


def stat_patients(df):
    r = df.agg(F.count("*").alias("n"), F.countDistinct("patient_id").alias("uniq"),
               F.sum(F.col("umur").isNull().cast("int")).alias("umur_null")).first()
    return {"n": r["n"], "duplikat": r["n"] - r["uniq"], "umur_kosong": r["umur_null"] or 0}


def vitals_flags(df):
    kosong = F.col("heart_rate").isNull() | F.col("spo2").isNull() | F.col("temperature").isNull()
    return (kosong,
            ~kosong & ~F.col("spo2").between(70, 100),
            ~kosong & ~F.col("heart_rate").between(30, 220),
            ~kosong & ~F.col("temperature").between(34, 42))


def stat_vitals(df):
    kosong, spo2_x, hr_x, suhu_x = vitals_flags(df)
    r = df.agg(F.count("*").alias("n"),
               F.sum(kosong.cast("int")).alias("kosong"), F.sum(spo2_x.cast("int")).alias("spo2"),
               F.sum(hr_x.cast("int")).alias("hr"), F.sum(suhu_x.cast("int")).alias("suhu"),
               F.sum((kosong | spo2_x | hr_x | suhu_x).cast("int")).alias("bermasalah"),
               F.countDistinct("patient_id", "timestamp").alias("uniq"),
               F.round(F.avg("spo2"), 1).alias("avg_spo2"), F.round(F.avg("heart_rate"), 1).alias("avg_hr")).first()
    return {"n": r["n"], "kosong": r["kosong"] or 0, "spo2_mustahil": r["spo2"] or 0, "hr_mustahil": r["hr"] or 0,
            "suhu_mustahil": r["suhu"] or 0, "baris_bermasalah": r["bermasalah"] or 0,
            "duplikat": r["n"] - r["uniq"], "avg_spo2": r["avg_spo2"], "avg_hr": r["avg_hr"]}


def stat_notes(df, col):
    r = df.agg(F.count("*").alias("n"),
               F.sum(F.col(col).rlike("<[^>]+>").cast("int")).alias("html"),
               F.sum(F.col(col).rlike("[#@]").cast("int")).alias("khusus"),
               F.sum((F.col(col) != F.lower(F.col(col))).cast("int")).alias("kapital"),
               F.sum(F.col(col).rlike(r"\s{2,}").cast("int")).alias("spasi")).first()
    return {"n": r["n"], "html": r["html"] or 0, "khusus": r["khusus"] or 0,
            "kapital": r["kapital"] or 0, "spasi": r["spasi"] or 0}


pat_b, vit_b, not_b = stat_patients(patients), stat_vitals(vitals), stat_notes(notes, "catatan_mentah")

# --- (a) Pasien: hapus duplikat ID, isi umur kosong dengan rata-rata ---
avg_age = int(patients.agg(F.avg("umur")).first()[0])
patients_clean = patients.dropDuplicates(["patient_id"]).na.fill({"umur": avg_age})

# --- (b) Vitals: buang kosong, duplikat, dan nilai mustahil secara fisiologis ---
vitals_clean = (vitals.dropna().dropDuplicates(["patient_id", "timestamp"])
                .filter(F.col("spo2").between(70, 100))
                .filter(F.col("heart_rate").between(30, 220))
                .filter(F.col("temperature").between(34, 42))
                .withColumn("timestamp", F.to_timestamp("timestamp")))

# --- (c) Catatan klinis: hapus tag HTML, karakter aneh, spasi berlebih, samakan huruf ---
teks = F.regexp_replace("catatan_mentah", r"<[^>]+>", " ")
teks = F.regexp_replace(teks, r"[^a-zA-Z0-9., ]", " ")
teks = F.regexp_replace(teks, r"\s+", " ")
notes_clean = (notes.withColumn("catatan", F.lower(F.trim(teks)))
               .filter(F.col("catatan").isNotNull() & (F.length("catatan") > 0)))

pat_a, vit_a, not_a = stat_patients(patients_clean), stat_vitals(vitals_clean), stat_notes(notes_clean, "catatan")

issues = [
    {"sumber": "patients.csv (CSV)", "masalah": "ID pasien duplikat", "sebelum": pat_b["duplikat"], "sesudah": pat_a["duplikat"],
     "aturan": "dropDuplicates(patient_id)", "alasan": "Satu pasien harus satu baris; duplikat menggandakan bobot pasien itu di setiap rata-rata/hitungan."},
    {"sumber": "patients.csv (CSV)", "masalah": "Umur kosong", "sebelum": pat_b["umur_kosong"], "sesudah": pat_a["umur_kosong"],
     "aturan": f"Isi dengan rata-rata umur ({avg_age} th)", "alasan": "Baris pasien tetap dipertahankan agar join ke sensor tidak hilang. Konsekuensi: umur ini perkiraan, bukan fakta."},
    {"sumber": "vitals.json (JSON)", "masalah": "SpO2 mustahil (di luar 70-100%)", "sebelum": vit_b["spo2_mustahil"], "sesudah": vit_a["spo2_mustahil"],
     "aturan": "filter 70 <= SpO2 <= 100", "alasan": "SpO2 di atas 100% mustahil; nilai seperti 150 atau 999 adalah error sensor dan merusak rata-rata."},
    {"sumber": "vitals.json (JSON)", "masalah": "Detak jantung mustahil (di luar 30-220 bpm)", "sebelum": vit_b["hr_mustahil"], "sesudah": vit_a["hr_mustahil"],
     "aturan": "filter 30 <= HR <= 220", "alasan": "HR 0 atau 300+ adalah sensor terlepas/glitch; dibuang agar tidak menaikkan/menurunkan rata-rata pasien."},
    {"sumber": "vitals.json (JSON)", "masalah": "Pembacaan duplikat (pasien+waktu sama)", "sebelum": vit_b["duplikat"], "sesudah": vit_a["duplikat"],
     "aturan": "dropDuplicates(patient_id, timestamp)", "alasan": "Satu pembacaan sensor tidak boleh terhitung dua kali."},
    {"sumber": "clinical_notes.txt (TXT)", "masalah": "Tag HTML di dalam teks", "sebelum": not_b["html"], "sesudah": not_a["html"],
     "aturan": "regexp_replace(<[^>]+>)", "alasan": "Tag HTML bukan isi medis dan mengacaukan pencarian kata kunci keluhan."},
    {"sumber": "clinical_notes.txt (TXT)", "masalah": "Karakter aneh (#, @)", "sebelum": not_b["khusus"], "sesudah": not_a["khusus"],
     "aturan": "hapus karakter selain huruf/angka/.,", "alasan": "Simbol acak memecah kata sehingga kata kunci tidak terdeteksi."},
    {"sumber": "clinical_notes.txt (TXT)", "masalah": "Huruf kapital tidak seragam", "sebelum": not_b["kapital"], "sesudah": not_a["kapital"],
     "aturan": "lower() + trim()", "alasan": "'NYERI DADA' dan 'nyeri dada' harus dihitung sebagai keluhan yang sama."},
]
for i in issues:
    print(f"  {i['masalah']:<45} {i['sebelum']:>8,} -> {i['sesudah']:>6,}")

# Dampak ke analisis: rata-rata sensor sebelum vs sesudah dibersihkan
def jml_hr_tinggi(df):
    return df.groupBy("patient_id").agg(F.avg("heart_rate").alias("a")).filter("a > 100").count()


dampak = {"avg_spo2_mentah": vit_b["avg_spo2"], "avg_spo2_bersih": vit_a["avg_spo2"],
          "avg_hr_mentah": vit_b["avg_hr"], "avg_hr_bersih": vit_a["avg_hr"],
          "pasien_hr_tinggi_mentah": jml_hr_tinggi(vitals), "pasien_hr_tinggi_bersih": jml_hr_tinggi(vitals_clean)}

contoh = records(notes_clean.filter(F.col("catatan_mentah").rlike("<|#|@")).select("catatan_mentah", "catatan"), 4)

dump("quality_report.json", {
    "baris": {"patients": [pat_b["n"], pat_a["n"]], "vitals": [vit_b["n"], vit_a["n"]], "notes": [not_b["n"], not_a["n"]]},
    "masalah": issues, "dampak": dampak, "contoh_catatan": contoh,
    "baris_vitals_bermasalah_mentah": vit_b["baris_bermasalah"], "baris_vitals_bermasalah_bersih": vit_a["baris_bermasalah"]})

# =====================================================================
# Simpan Parquet bersih. Spark MEMPROSES, hasil ditulis lewat pandas/pyarrow
# (Spark .write() di Windows butuh winutils; cara ini tidak).
# =====================================================================
vitals_out = vitals_clean.withColumn("timestamp", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
to_pandas(patients_clean).to_parquet(PROCESSED / "patients.parquet", index=False)
to_pandas(vitals_out).to_parquet(PROCESSED / "vitals.parquet", index=False)
to_pandas(notes_clean.drop("catatan_mentah")).to_parquet(PROCESSED / "notes.parquet", index=False)

# =====================================================================
# VARIETY (integrasi): CSV + JSON + TXT -> satu tabel per pasien (kunci: patient_id)
# =====================================================================
cardiac = F.col("catatan").rlike(CARDIAC_REGEX)
vit_agg = vitals_clean.groupBy("patient_id").agg(F.round(F.avg("heart_rate"), 1).alias("avg_hr"),
                                                 F.round(F.avg("spo2"), 1).alias("avg_spo2"),
                                                 F.count("*").alias("n_vitals"))
note_agg = notes_clean.groupBy("patient_id").agg(F.count("*").alias("n_notes"),
                                                 F.sum(cardiac.cast("int")).alias("n_keluhan_jantung"))
integrated = patients_clean.join(vit_agg, "patient_id", "left").join(note_agg, "patient_id", "left").orderBy("patient_id")
int_pdf = to_pandas(integrated)
int_pdf.to_parquet(PROCESSED / "integrated.parquet", index=False)
n_int = len(int_pdf)
lengkap = int((int_pdf["n_vitals"].notna() & int_pdf["n_notes"].notna()).sum())

dump("variety_report.json", {
    "sumber": [
        {"nama": "Data pasien", "format": "CSV", "struktur": "Terstruktur", "file": "patients.csv", "ukuran_mb": mb(RAW / "patients.csv"),
         "baris": n_pat_raw, "skema": schema_of(patients), "sampel": records(patients),
         "cara_baca": "spark.read.csv(header=True, inferSchema=True)",
         "tantangan": "Skema tetap, tetapi ada ID ganda dan umur kosong."},
        {"nama": "Sensor IoT", "format": "JSON lines", "struktur": "Semi-terstruktur", "file": "vitals.json", "ukuran_mb": mb(RAW / "vitals.json"),
         "baris": n_vit_raw, "skema": schema_of(vitals), "sampel": records(vitals),
         "cara_baca": "spark.read.json()  (skema disimpulkan dari isi)",
         "tantangan": "Tiap baris objek JSON; nilai bisa hilang atau di luar rentang wajar."},
        {"nama": "Catatan klinis dokter", "format": "TXT (teks bebas)", "struktur": "Tidak terstruktur", "file": "clinical_notes.txt", "ukuran_mb": mb(RAW / "clinical_notes.txt"),
         "baris": n_not_raw, "skema": [{"kolom": "value", "tipe": "string (satu kolom mentah)"}], "sampel": records(notes_raw),
         "cara_baca": "spark.read.text() lalu split('|') menjadi patient_id, tanggal, catatan",
         "tantangan": "Tidak ada skema; isi kalimat bebas, ada tag HTML dan simbol acak."}],
    "integrasi": {
        "kunci": "patient_id",
        "langkah": ["CSV: bersihkan duplikat/umur kosong", "JSON: buang nilai mustahil lalu agregasi per pasien (rata-rata HR, SpO2, jumlah pembacaan)",
                    "TXT: bersihkan teks, deteksi kata kunci keluhan jantung, hitung per pasien",
                    "LEFT JOIN ketiganya pada patient_id -> satu baris per pasien (integrated.parquet)"],
        "baris_terintegrasi": n_int, "pasien_punya_ketiga_sumber": lengkap,
        "sampel": json.loads(int_pdf.head(5).to_json(orient="records"))}})

# =====================================================================
# VOLUME (ukuran & relasi)
# =====================================================================
fan = vit_agg.join(note_agg, "patient_id").agg(F.sum(F.col("n_vitals") * F.col("n_notes")).alias("x")).first()["x"]
dump("volume_report.json", {
    "dataset": [
        {"nama": "patients", "format": "CSV", "baris": n_pat_raw, "mb_mentah": mb(RAW / "patients.csv"), "mb_parquet": mb(PROCESSED / "patients.parquet")},
        {"nama": "vitals", "format": "JSON", "baris": n_vit_raw, "mb_mentah": mb(RAW / "vitals.json"), "mb_parquet": mb(PROCESSED / "vitals.parquet")},
        {"nama": "notes", "format": "TXT", "baris": n_not_raw, "mb_mentah": mb(RAW / "clinical_notes.txt"), "mb_parquet": mb(PROCESSED / "notes.parquet")}],
    "relasi": {"kunci": "patient_id", "vitals_per_pasien": round(vit_a["n"] / pat_a["n"], 1),
               "notes_per_pasien": round(not_a["n"] / pat_a["n"], 1),
               "baris_join_naif_vitals_x_notes": int(fan or 0), "baris_input_vitals_notes": vit_a["n"] + not_a["n"]},
    "etl_detik": round(time.time() - T0, 1)})

print(f"\nSelesai dalam {time.time() - T0:.1f} detik. Parquet dan laporan JSON di {PROCESSED}")
spark.stop()
