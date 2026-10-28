"""Jalankan seluruh pipeline batch berurutan: generate -> ETL -> benchmark -> query Value.
Pemakaian: python run_all.py     (laptop berat? set BENCH_QUICK=1 atau perkecil N_VITALS / N_NOTES, lihat README)
Velocity tidak termasuk: nyalakan dari tombol di tab Velocity pada dashboard."""
import subprocess
import sys
import time

STEPS = [("01_generate_data.py", "Membuat data sintetik 3 format"),
         ("02_etl_variety_veracity.py", "ETL: Variety + Veracity + ukuran data"),
         ("03_volume_benchmark.py", "Benchmark Volume"),
         ("06_value_queries.py", "Query Spark SQL untuk Value")]

for i, (script, ket) in enumerate(STEPS, 1):
    print(f"\n[{i}/{len(STEPS)}] {ket}  ({script})")
    t0 = time.time()
    if subprocess.call([sys.executable, script]) != 0:
        sys.exit(f"\nGagal di {script}. Perbaiki error di atas, lalu jalankan ulang.")
    print(f"    selesai dalam {time.time() - t0:.0f} detik")
print("\nSemua bukti siap. Jalankan:  streamlit run app.py")
