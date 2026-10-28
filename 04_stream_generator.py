"""
LANGKAH 4a - Simulator DATA BARU yang masuk ke sistem (jalankan di TERMINAL TERPISAH).

Setiap 3 detik menulis file baru ke data/incoming/ dengan 3 tipe data berbeda:
  vitals_*.json    sensor IoT. Sebagian sengaja kotor (SpO2 150, duplikat)      -> Veracity
  notes_*.txt      catatan dokter. Sebagian berisi tag HTML / karakter aneh      -> Veracity
  patients_*.csv   pasien BARU (tiap ~15 detik). Tiga pasien baru pertama
                   "memburuk": detak jantung tinggi + keluhan nyeri dada        -> Value

Tekan Ctrl+C untuk berhenti.
"""
import json
import random
import shutil
import time
import uuid
from datetime import datetime

from config import INCOMING, N_PATIENTS

shutil.rmtree(INCOMING, ignore_errors=True)
INCOMING.mkdir(parents=True, exist_ok=True)

KELUHAN_JANTUNG = [
    "pasien mengeluh nyeri dada sebelah kiri yang menjalar ke lengan",
    "sesak napas saat aktivitas ringan",
    "jantung terasa berdebar disertai keringat dingin",
]
KELUHAN_RINGAN = ["pusing ringan sejak pagi", "mual tanpa muntah", "batuk kering dua hari",
                  "demam ringan", "tidak ada keluhan berarti"]
OBSERVASI = ["terlihat pucat", "tampak lemas", "kesadaran compos mentis", "tampak tenang"]


def pid(i: int) -> str:
    return f"P{i:05d}"


def tulis(nama: str, isi: str):
    """Tulis ke file sementara lalu rename, agar Spark tidak membaca file setengah jadi."""
    tmp = INCOMING / f".{nama}.tmp"
    tmp.write_text(isi, encoding="utf-8")
    tmp.replace(INCOMING / nama)


def noisy(text: str) -> str:
    if random.random() < 0.15:
        text = f"<p>{text}</p><br>"
    if random.random() < 0.20:
        text = text.upper()
    if random.random() < 0.20:
        text = "   " + text.replace(" ", "   ", 2) + " ### @@"
    return text


alert, next_id, tick = [], N_PATIENTS + 1, 0
print(f"Menulis data baru ke {INCOMING} setiap 3 detik ... (Ctrl+C untuk berhenti)")

while True:
    tick += 1
    now = datetime.now()
    info = []

    # 1) CSV: pasien baru (tick 1, 6, 11, ...)
    if tick % 5 == 1:
        i, next_id = next_id, next_id + 1
        if len(alert) < 3:
            alert.append(i)                      # pasien baru yang kondisinya memburuk
        umur = "" if random.random() < 0.10 else random.randint(55, 85)      # NOISE: umur kosong
        baris = [f"{pid(i)},{umur},{random.choice(['L', 'P'])},{random.choice(['hipertensi', 'jantung koroner', 'diabetes'])}"]
        if random.random() < 0.20:
            baris.append(baris[0])                                            # NOISE: duplikat
        tulis(f"patients_{uuid.uuid4().hex[:8]}.csv",
              "patient_id,umur,gender,riwayat_penyakit\n" + "\n".join(baris))
        info.append(f"pasien baru {pid(i)}")

    # 2) JSON: 20 pembacaan sensor
    recs = []
    for k in range(20):
        if alert and k < 6:                      # sebagian data berasal dari pasien yang memburuk
            i, hr, spo2 = random.choice(alert), random.gauss(125, 8), random.gauss(92, 1.5)
        else:
            i, hr, spo2 = random.randint(1, N_PATIENTS), random.gauss(80, 15), random.gauss(97, 1.5)
        rec = {"patient_id": pid(i), "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S"),
               "heart_rate": int(hr), "temperature": round(random.gauss(36.8, 0.4), 1),
               "spo2": min(100, int(spo2))}
        if random.random() < 0.07:
            rec["spo2"] = random.choice([150, 0, 999])        # NOISE: SpO2 mustahil
        if random.random() < 0.03:
            rec["heart_rate"] = random.choice([0, 300])       # NOISE: detak mustahil
        recs.append(rec)
        if random.random() < 0.05:
            recs.append(dict(rec))                            # NOISE: duplikat
    tulis(f"vitals_{uuid.uuid4().hex[:8]}.json", "\n".join(json.dumps(r) for r in recs))

    # 3) TXT: 6 catatan dokter
    baris = []
    for k in range(6):
        if alert and k < 2:
            i, keluhan = random.choice(alert), random.choice(KELUHAN_JANTUNG)
        else:
            i, keluhan = random.randint(1, N_PATIENTS), random.choice(KELUHAN_RINGAN)
        teks = f"{keluhan}, {random.choice(OBSERVASI)}"
        baris.append(f"{pid(i)}|{now:%Y-%m-%d}|{noisy(teks)}")
    tulis(f"notes_{uuid.uuid4().hex[:8]}.txt", "\n".join(baris))

    print(f"{now:%H:%M:%S}  tick {tick}: {len(recs)} sensor JSON, 6 catatan TXT" + (f", {', '.join(info)} (CSV)" if info else ""))
    time.sleep(3)