"""
LANGKAH 1 - Generate data sintetik (3 format) + noise sengaja untuk bukti VERACITY.

  data/raw/patients.csv         -> terstruktur   (CSV)
  data/raw/vitals.json          -> semi-terstruktur (JSON lines, sensor IoT)
  data/raw/clinical_notes.txt   -> tidak terstruktur (teks bebas, 1 catatan per baris)

15% pasien dibuat "berisiko jantung": detak jantung tinggi + sering ada keluhan nyeri dada,
supaya agen AI nanti punya sesuatu yang bermakna untuk ditemukan.
"""
import csv
import json
import random
from datetime import datetime, timedelta

from config import RAW, DAYS, N_PATIENTS, N_VITALS, N_NOTES

random.seed(42)
RAW.mkdir(parents=True, exist_ok=True)
now = datetime.now()

pid = lambda i: f"P{i:05d}"
risky = set(random.sample(range(1, N_PATIENTS + 1), int(N_PATIENTS * 0.15)))

# Label "kunci jawaban" (hanya ada karena datanya sintetik). TIDAK dipakai pipeline;
# hanya dipakai di tab Value untuk menguji seberapa tepat aturan risiko menemukan pasien yang benar.
with open(RAW / "_ground_truth.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["patient_id", "risky"])
    for i in range(1, N_PATIENTS + 1):
        w.writerow([pid(i), int(i in risky)])

# ---------- patients.csv (CSV) ----------
with open(RAW / "patients.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["patient_id", "umur", "gender", "riwayat_penyakit"])
    for i in range(1, N_PATIENTS + 1):
        umur = random.randint(55, 85) if i in risky else random.randint(18, 80)
        riwayat = (random.choice(["hipertensi", "jantung koroner", "diabetes"]) if i in risky
                   else random.choice(["hipertensi", "diabetes", "asma", "tidak ada"]))
        if random.random() < 0.02:
            umur = ""                        # NOISE: umur kosong
        row = [pid(i), umur, random.choice(["L", "P"]), riwayat]
        w.writerow(row)
        if random.random() < 0.02:
            w.writerow(row)                  # NOISE: baris duplikat

# ---------- vitals.json (JSON lines) ----------
with open(RAW / "vitals.json", "w", encoding="utf-8") as f:
    for _ in range(N_VITALS):
        i = random.randint(1, N_PATIENTS)
        is_risky = i in risky
        ts = now - timedelta(seconds=random.randint(0, DAYS * 86400))
        rec = {
            "patient_id": pid(i),
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "heart_rate": int(random.gauss(112 if is_risky else 76, 10)),
            "temperature": round(random.gauss(36.8, 0.4), 1),
            "spo2": min(100, int(random.gauss(94 if is_risky else 97, 1.5))),
        }
        if random.random() < 0.03:
            rec["spo2"] = random.choice([150, 0, -5, 999])      # NOISE: SpO2 mustahil
        if random.random() < 0.01:
            rec["heart_rate"] = random.choice([0, 300, 999])    # NOISE: detak mustahil
        f.write(json.dumps(rec) + "\n")

# ---------- clinical_notes.txt (teks bebas) ----------
KELUHAN_JANTUNG = [
    "pasien mengeluh nyeri dada sebelah kiri yang menjalar ke lengan",
    "sesak napas saat aktivitas ringan",
    "jantung terasa berdebar disertai keringat dingin",
]
KELUHAN_RINGAN = ["pusing ringan sejak pagi", "mual tanpa muntah", "batuk kering dua hari",
                  "demam ringan", "tidak ada keluhan berarti"]
OBSERVASI = ["terlihat pucat", "tampak lemas", "kesadaran compos mentis", "tampak tenang"]


def noisy(text: str) -> str:
    """NOISE: tag HTML, karakter aneh, spasi berlebih, huruf kapital acak."""
    if random.random() < 0.15:
        text = f"<p>{text}</p><br>"
    if random.random() < 0.20:
        text = text.upper()
    if random.random() < 0.20:
        text = "   " + text.replace(" ", "   ", 2) + " ### @@"
    return text


with open(RAW / "clinical_notes.txt", "w", encoding="utf-8") as f:
    for _ in range(N_NOTES):
        i = random.randint(1, N_PATIENTS)
        p_jantung = 0.6 if i in risky else 0.02
        keluhan = random.choice(KELUHAN_JANTUNG if random.random() < p_jantung else KELUHAN_RINGAN)
        teks = f"{keluhan}, {random.choice(OBSERVASI)}"
        tgl = (now - timedelta(days=random.randint(0, DAYS - 1))).strftime("%Y-%m-%d")
        f.write(f"{pid(i)}|{tgl}|{noisy(teks)}\n")

print(f"Selesai: {N_PATIENTS} pasien, {N_VITALS} vitals, {N_NOTES} catatan -> {RAW}")