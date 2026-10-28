"""Konfigurasi bersama semua script. Kecilkan angka (atau set variabel lingkungan) kalau laptop terasa berat."""
import os
from pathlib import Path

BASE = Path(__file__).parent
DATA = BASE / "data"
RAW = DATA / "raw"                    # data mentah hasil generate (3 format)
PROCESSED = DATA / "processed"        # Parquet bersih + laporan bukti 5V (*.json)
INCOMING = DATA / "incoming"          # folder "kotak masuk" untuk simulasi data baru (Velocity)
LIVE = DATA / "live"                  # hasil ingest worker + statistik per batch
BENCH_DIR = DATA / "bench_tmp"        # file sementara benchmark (dihapus otomatis)

DAYS = 30                                              # rentang waktu data sintetik
N_PATIENTS = int(os.getenv("N_PATIENTS", 2_000))
N_VITALS = int(os.getenv("N_VITALS", 200_000))
N_NOTES = int(os.getenv("N_NOTES", 500_000))
# Benchmark Volume: file catatan disalin berkali-kali -> ukuran data bertambah (0,5 jt / 2 jt / 5 jt baris).
# Set BENCH_QUICK=1 kalau laptop berat (hanya ukuran terkecil).
VOLUME_COPIES_LIST = [1] if os.getenv("BENCH_QUICK") else [1, 4, 10]

KEYWORDS = ["nyeri dada", "sesak napas", "berdebar", "pusing", "mual",
            "batuk", "demam", "pucat", "keringat dingin"]
CARDIAC_KEYWORDS = ["nyeri dada", "sesak napas", "berdebar", "keringat dingin"]
CARDIAC_REGEX = "|".join(CARDIAC_KEYWORDS)

# Aturan klasifikasi risiko (Value). Ini ambang DEMO, bukan standar klinis.
RISK_HR = 100        # rata-rata detak jantung > 100 bpm
RISK_SPO2 = 96       # rata-rata SpO2 < 96 %
RISK_RASIO = 0.30    # >= 30% catatan klinis pasien berisi keluhan jantung

SPARK_MASTER = f"local[{min(4, os.cpu_count() or 2)}]"
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
