# Clinical Decision Support: 5V Big Data (PySpark + Streamlit + LangGraph)

Studi kasus: **IoT-Based Patient Monitoring + Longitudinal EHR** (data sintetik).
Pertanyaan klinis: *pasien mana yang berisiko tinggi penyakit jantung, dan apa cirinya?*

## Cara menjalankan

```
pip install -r requirements.txt
python run_all.py          # generate data -> ETL -> benchmark -> query Value (beberapa menit)
streamlit run app.py       # dashboard 7 tab
```

Velocity: buka tab **Velocity** lalu klik **Mulai simulasi** (menyalakan simulator + worker Spark otomatis).
Alternatif manual, dua terminal: `python 05_ingest_worker.py` lalu `python 04_stream_generator.py`.

Laptop berat / RAM kecil? Perkecil sebelum menjalankan (PowerShell: `$env:N_NOTES=100000`):
`N_VITALS`, `N_NOTES`, `SPARK_DRIVER_MEMORY` (default 2g), dan `BENCH_QUICK=1` (benchmark hanya ukuran terkecil).

## Peta 5V: apa dibuktikan di mana

| V | Script | Tab dashboard | Bukti yang tampil | Segmen video |
|---|--------|---------------|-------------------|--------------|
| Volume | `02`, `03` | Volume | jumlah baris, ukuran CSV/JSON vs Parquet, relasi dan ledakan join, Python vs Spark, Operasi A vs B | 2, 4 |
| Velocity | `04`, `05` | Velocity (live) | jeda kedatangan, latency (antre + proses), throughput, window 1 menit, peringatan pasien memburuk | 2, 4 |
| Variety | `02` | Variety | skema + contoh 3 format, cara baca, diagram integrasi, tabel terintegrasi | 3 |
| Veracity | `02` | Veracity | 8 masalah: sebelum/sesudah, alasan aturan, dampak ke analisis, contoh teks | 3, 5 |
| Value | `06`, `agent.py` | Value | 4 query Spark SQL + interpretasi, evaluasi vs kunci jawaban, batasan, agen LangGraph | 5 |

Tab **Ringkasan** berisi panduan tab mana yang ditampilkan di tiap segmen video; tab **Arsitektur** berisi diagram untuk halaman 3 PDF.

## Perubahan dari versi sebelumnya

- `envloader.py` isinya log error Streamlit (bukan kode) -> dihapus. `agent.py` sudah punya pembaca `.env` sendiri.
- `config.py` tidak punya `INCOMING`, `LIVE`, `BENCH_DIR`, `VOLUME_COPIES_LIST` padahal dipakai script 03/04/04b -> dilengkapi.
- Simulator menulis ke `data/incoming/` tetapi `05_velocity_microbatch.py` memantau folder lain -> versi lama dihapus; satu worker (`05_ingest_worker.py`, dulu `04b_test.py`).
- Setiap tahap kini **menyimpan buktinya ke JSON** (`data/processed/*.json`) sehingga Streamlit menampilkan angka asli, bukan teks ketikan.
- Benchmark kini juga membandingkan Operasi A vs B (Tabel 8.2 soal) untuk memisahkan efek ukuran vs struktur.
- `.env` berisi API key dan log JVM/Streamlit tidak lagi disertakan.

## Catatan jujur untuk presentasi

1. **Volume:** datanya ratusan ribu baris. Pada ukuran kecil Spark lokal bisa lebih lambat dari Python (overhead JVM); dashboard melaporkan apa adanya.
2. **Velocity:** kedatangan data disimulasikan; ini micro-batch manual, bukan Structured Streaming (`05b_...` untuk WSL2/Linux/Colab).
3. **Value:** precision/recall sempurna karena data sintetik dibuat terpisah jelas; ambang risiko adalah ambang demo, bukan standar klinis.
4. **RAG:** TF-IDF (pencocokan kata), bukan embedding semantik. **HDFS** tidak dipakai (1 mesin); ganti path ke `hdfs://` bila ada cluster.
5. Angka di video harus dari hasil jalan di laptop kalian sendiri, dan kalian harus bisa menjelaskan tiap bagian kode (aturan integritas akademik).
