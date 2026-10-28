"""
LANGKAH 6 - Bukti VALUE: pertanyaan klinis dijawab dengan Spark SQL pada data bersih + terintegrasi.

Pertanyaan: "Pasien mana yang berisiko tinggi penyakit jantung, dan apa cirinya?"
Aturan risiko memakai TIGA sumber sekaligus (sensor JSON + catatan TXT + pasien CSV), ambang ada di config.py.
Hasil (SQL + tabel) disimpan ke data/processed/value_report.json untuk tab Value di Streamlit.
"""
import json
import time
from datetime import datetime

import pandas as pd

from config import RAW, PROCESSED, RISK_HR, RISK_SPO2, RISK_RASIO
from spark_session import get_spark, to_pandas

spark = get_spark("Value")
for name in ["patients", "vitals", "notes", "integrated"]:
    spark.read.parquet(str(PROCESSED / f"{name}.parquet")).createOrReplaceTempView(name)

TIER_SQL = f"""
SELECT i.*,
       ROUND(i.n_keluhan_jantung / i.n_notes, 3) AS rasio_keluhan,
       CASE
         WHEN i.avg_hr IS NULL OR i.n_notes IS NULL THEN 'Data tidak lengkap'
         WHEN i.avg_hr > {RISK_HR} AND i.avg_spo2 < {RISK_SPO2} AND i.n_keluhan_jantung / i.n_notes >= {RISK_RASIO} THEN 'Tinggi'
         WHEN (i.avg_hr > {RISK_HR} AND i.avg_spo2 < {RISK_SPO2}) OR i.n_keluhan_jantung / i.n_notes >= {RISK_RASIO} THEN 'Sedang'
         ELSE 'Rendah'
       END AS tier
FROM integrated i
"""
spark.sql(TIER_SQL).createOrReplaceTempView("tiered")

QUERIES = [
    {"id": "q1", "judul": "Q1. Profil tiap kelompok risiko",
     "tujuan": "Apakah kelompok 'Tinggi' benar-benar berbeda dari yang lain pada detak jantung, SpO2, dan keluhan?",
     "sql": """SELECT tier, COUNT(*) AS n_pasien, ROUND(AVG(avg_hr),1) AS rata_hr, ROUND(AVG(avg_spo2),1) AS rata_spo2,
       ROUND(AVG(rasio_keluhan),3) AS rata_rasio_keluhan, ROUND(AVG(umur),1) AS rata_umur
FROM tiered GROUP BY tier ORDER BY n_pasien DESC"""},
    {"id": "q2", "judul": "Q2. Riwayat penyakit di tiap kelompok",
     "tujuan": "Riwayat penyakit apa yang paling banyak pada pasien berisiko?",
     "sql": """SELECT riwayat_penyakit, tier, COUNT(*) AS n_pasien
FROM tiered GROUP BY riwayat_penyakit, tier ORDER BY riwayat_penyakit, tier"""},
    {"id": "q3", "judul": "Q3. Pasien prioritas (10 teratas)",
     "tujuan": "Siapa yang perlu ditinjau dokter lebih dulu?",
     "sql": """SELECT patient_id, umur, riwayat_penyakit, avg_hr, avg_spo2, n_keluhan_jantung, rasio_keluhan
FROM tiered WHERE tier = 'Tinggi' ORDER BY avg_hr DESC LIMIT 10"""},
    {"id": "q4", "judul": "Q4. Tren harian detak jantung: Tinggi vs Rendah",
     "tujuan": "Apakah perbedaan kelompok stabil dari hari ke hari atau hanya kebetulan satu hari?",
     "sql": """SELECT to_date(v.timestamp) AS tanggal, t.tier, ROUND(AVG(v.heart_rate),1) AS rata_hr
FROM vitals v JOIN tiered t ON v.patient_id = t.patient_id
WHERE t.tier IN ('Tinggi','Rendah') GROUP BY to_date(v.timestamp), t.tier ORDER BY tanggal"""},
]

hasil = []
for q in QUERIES:
    t0 = time.time()
    pdf = to_pandas(spark.sql(q["sql"]))
    detik = round(time.time() - t0, 2)
    print(f"{q['judul']}: {len(pdf)} baris, {detik}s")
    hasil.append({**q, "detik": detik, "baris": json.loads(pdf.to_json(orient="records", date_format="iso"))})

# Evaluasi terhadap kunci jawaban (hanya mungkin karena data sintetik)
evaluasi = None
gt_file = RAW / "_ground_truth.csv"
if gt_file.exists():
    gt = pd.read_csv(gt_file)
    pred = to_pandas(spark.sql("SELECT patient_id, tier FROM tiered")).merge(gt, on="patient_id")

    def nilai(flag):
        tp = int(((flag) & (pred["risky"] == 1)).sum()); fp = int(((flag) & (pred["risky"] == 0)).sum())
        fn = int(((~flag) & (pred["risky"] == 1)).sum()); tn = int(((~flag) & (pred["risky"] == 0)).sum())
        return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
                "precision": round(tp / (tp + fp), 3) if tp + fp else None,
                "recall": round(tp / (tp + fn), 3) if tp + fn else None}
    evaluasi = {"hanya_tinggi": nilai(pred["tier"] == "Tinggi"), "tinggi_dan_sedang": nilai(pred["tier"].isin(["Tinggi", "Sedang"])),
                "total_risiko_sebenarnya": int(pred["risky"].sum()), "total_pasien": len(pred)}

(PROCESSED / "value_report.json").write_text(json.dumps({
    "dibuat": datetime.now().isoformat(timespec="seconds"),
    "pertanyaan": "Pasien mana yang berisiko tinggi penyakit jantung, dan apa cirinya?",
    "aturan": {"hr": RISK_HR, "spo2": RISK_SPO2, "rasio": RISK_RASIO}, "tier_sql": TIER_SQL.strip(),
    "queries": hasil, "evaluasi": evaluasi}, indent=2, ensure_ascii=False), encoding="utf-8")
print("Tersimpan: data/processed/value_report.json")
spark.stop()
