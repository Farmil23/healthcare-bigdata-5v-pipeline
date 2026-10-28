"""
Dashboard Streamlit: membuktikan 5V Big Data pada pipeline data kesehatan.
Jalankan: streamlit run app.py

Setiap tab V memakai pola yang sama: Apa itu V -> Mengapa penting di kasus ini -> Bukti (angka/grafik)
-> Kesimpulan -> Batasan. Semua angka dibaca dari laporan JSON hasil pipeline (data/processed/*.json),
jadi tidak ada angka yang diketik manual.
"""
import json
import os
import shutil
import subprocess
import sys
import time

import pandas as pd
import streamlit as st

from config import BASE, DATA, PROCESSED, LIVE, RISK_HR, RISK_SPO2, RISK_RASIO

st.set_page_config(page_title="5V Big Data - Clinical Decision Support", page_icon="🫀", layout="wide")


# ====================================================================== util
def load_json(name):
    try:
        return json.loads((PROCESSED / name).read_text(encoding="utf-8"))
    except Exception:
        return None


def fmt(n):
    return f"{int(n):,}".replace(",", ".")


def judul_v(ikon, nama, definisi, alasan):
    st.header(f"{ikon} {nama}")
    a, b = st.columns(2)
    a.info(f"**Apa itu {nama}?**\n\n{definisi}")
    b.success(f"**Mengapa penting di kasus ini?**\n\n{alasan}")


def belum_ada(script):
    st.warning(f"Bukti untuk tab ini belum ada. Jalankan `python {script}` (atau `python run_all.py`), lalu muat ulang halaman.")


def batasan(items):
    with st.expander("⚠️ Batasan dan catatan jujur"):
        for i in items:
            st.markdown(f"- {i}")


# ====================================================================== proses simulasi (Velocity)
@st.cache_resource
def _procs():
    return {}


def _hidup(k):
    p = _procs().get(k)
    return p is not None and p.poll() is None


def hentikan_simulasi():
    for k, p in list(_procs().items()):
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=8)
            except Exception:
                p.kill()
    _procs().clear()


def mulai_simulasi():
    hentikan_simulasi()
    shutil.rmtree(LIVE, ignore_errors=True)
    LIVE.mkdir(parents=True, exist_ok=True)
    logs = DATA / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}

    def jalan(nama, script):
        _procs()[nama] = subprocess.Popen([sys.executable, str(BASE / script)], cwd=BASE, env=env,
                                          stdout=open(logs / f"{nama}.log", "w", encoding="utf-8"), stderr=subprocess.STDOUT)

    with st.spinner("Menyalakan Spark worker (±10-30 detik, pemanasan JVM) ..."):
        jalan("worker", "05_ingest_worker.py")
        t0 = time.time()
        while not (LIVE / "ready.flag").exists() and time.time() - t0 < 120:
            if _procs()["worker"].poll() is not None:
                break
            time.sleep(1)
    if not _hidup("worker"):
        st.error("Worker gagal berjalan. Lihat data/logs/worker.log")
        return
    jalan("gen", "04_stream_generator.py")


def baca_stats():
    rows = []
    try:
        for line in (LIVE / "stats.jsonl").read_text(encoding="utf-8").splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass                                   # baris terakhir bisa belum selesai ditulis
    except OSError:
        pass
    return rows


# ====================================================================== diagram
ARSITEKTUR = r'''
digraph G {
  rankdir=LR; compound=true; nodesep=0.35; ranksep=0.55;
  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, margin="0.12,0.07"];
  edge [fontname="Helvetica", fontsize=9, color="#475569"];

  subgraph cluster_src { label="1. SUMBER DATA  (Variety)"; style="rounded,dashed"; color="#2563eb"; fontname="Helvetica-Bold";
    csv  [label="patients.csv\nCSV · terstruktur", fillcolor="#dbeafe"];
    json [label="vitals.json\nJSON · semi-terstruktur\n(sensor IoT)", fillcolor="#dbeafe"];
    txt  [label="clinical_notes.txt\nTeks bebas · tidak terstruktur", fillcolor="#dbeafe"]; }

  subgraph cluster_ing { label="2. INGESTION  (Velocity)"; style="rounded,dashed"; color="#16a34a"; fontname="Helvetica-Bold";
    gen [label="Simulator sensor\nfile baru tiap 3 detik", fillcolor="#dcfce7"];
    inc [label="data/incoming/", shape=folder, fillcolor="#dcfce7"];
    wrk [label="Ingest worker\nmicro-batch + ukur latency", fillcolor="#dcfce7"]; }

  subgraph cluster_proc { label="3. PEMROSESAN PySpark  (Volume + Veracity)"; style="rounded,dashed"; color="#d97706"; fontname="Helvetica-Bold";
    etl [label="ETL batch\nbaca 3 format · validasi\nbersihkan · integrasi (join)", fillcolor="#fef3c7"];
    bnc [label="Benchmark\nPython vs Spark\nOperasi A vs B", fillcolor="#fef3c7"]; }

  subgraph cluster_sto { label="4. PENYIMPANAN"; style="rounded,dashed"; color="#7c3aed"; fontname="Helvetica-Bold";
    pq   [label="Parquet bersih\npatients · vitals · notes\nintegrated", shape=cylinder, fillcolor="#ede9fe"];
    live [label="Live store\nCSV append + stats.jsonl", shape=cylinder, fillcolor="#ede9fe"]; }

  subgraph cluster_ana { label="5. ANALISIS  (Value)"; style="rounded,dashed"; color="#db2777"; fontname="Helvetica-Bold";
    sql [label="Spark SQL\nkelompok risiko Tinggi/Sedang/Rendah", fillcolor="#fce7f3"];
    agt [label="Agen LangGraph\nrouter → SQL → RAG → answer", fillcolor="#fce7f3"]; }

  out [label="Dashboard Streamlit\n(tab 5V + tanya agen)", shape=component, fillcolor="#fee2e2"];

  csv -> etl; json -> etl; txt -> etl;
  gen -> inc [label="file baru"]; inc -> wrk; wrk -> live;
  etl -> pq [label="Parquet"]; json -> bnc [style=invis]; pq -> bnc [style=dashed];
  pq -> sql; pq -> agt; sql -> out; agt -> out; live -> out [label="latency, window"]; bnc -> out;
}
'''


def dot_agen(trace):
    dikunjungi = {t.split(" ")[0] for t in trace}

    def n(nama, label):
        w = "#86efac" if nama in dikunjungi else "#e5e7eb"
        return f'{nama} [label="{label}", fillcolor="{w}"];'
    return ('digraph A { rankdir=LR; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11]; edge [fontsize=9];'
            + n("router", "router\\n(pilih jalur)") + n("sql_agent", "sql_agent\\n(Spark SQL)")
            + n("rag_agent", "rag_agent\\n(cari catatan)") + n("answer", "answer\\n(ringkasan)")
            + 'START [shape=circle, fillcolor="#bfdbfe"]; END [shape=doublecircle, fillcolor="#bfdbfe"];'
            + 'START -> router; router -> sql_agent [label="angka"]; router -> rag_agent [label="teks saja"];'
            + 'sql_agent -> rag_agent [label="perlu teks"]; sql_agent -> answer [label="angka saja"];'
            + 'rag_agent -> answer; answer -> END; }')


# ====================================================================== data
VOL, VAR, VER = load_json("volume_report.json"), load_json("variety_report.json"), load_json("quality_report.json")
VAL, BEN = load_json("value_report.json"), load_json("benchmark.json")



# ====================================================================== BAGIAN: TENTANG PROYEK (detail)
def _tier(r):
    if r.get("avg_hr") is None or not r.get("n_notes"):
        return "Data tidak lengkap"
    rasio = r["n_keluhan_jantung"] / r["n_notes"]
    sensor = r["avg_hr"] > RISK_HR and r["avg_spo2"] < RISK_SPO2
    teks = rasio >= RISK_RASIO
    return "Tinggi" if sensor and teks else "Sedang" if sensor or teks else "Rendah"


def bagian_proyek():
    st.subheader("📘 Tentang proyek ini, lebih dalam")
    t = st.tabs(["Masalah dan tujuan", "Data dan kamus data", "Cara kerja langkah demi langkah", "Hasil utama", "Peran tim", "Glosarium"])

    with t[0]:
        a, b = st.columns(2)
        a.markdown("""
**Latar belakang**

Seorang pasien jantung bisa memburuk dalam hitungan jam. Informasi tentang dia tersebar di tiga tempat: **data pasien** (tabel),
**sensor** yang mengirim detak jantung dan SpO2 terus-menerus, dan **catatan dokter** berupa kalimat bebas.
Tidak ada dokter yang sanggup membuka ketiganya untuk ribuan pasien setiap hari.

**Masalah yang diselesaikan**

1. Datanya berbeda bentuk, sehingga tidak bisa langsung dibandingkan.
2. Datanya kotor (sensor error, ID ganda, sisa HTML di catatan).
3. Datanya terus datang dan jumlahnya besar.
4. Dokter butuh jawaban singkat: *siapa yang perlu dilihat lebih dulu?*
""")
        b.markdown("""
**Tujuan**

- Menggabungkan tiga sumber menjadi **satu gambaran per pasien**.
- Membersihkan data dengan aturan yang **terukur** dan bisa dijelaskan.
- Memproses data yang terus datang dengan **latency** yang diukur.
- Menghasilkan **kelompok risiko** (Tinggi / Sedang / Rendah) dan daftar pasien prioritas.
- Menunjukkan seberapa layak hasilnya dan **apa batasannya**.

**Siapa penggunanya dan apa keluarannya**

Dokter, perawat, atau manajer layanan melihat kelompok risiko, daftar prioritas tinjauan, dan ringkasan dari agen AI.
Keluaran ini adalah **alat bantu memilih prioritas, bukan diagnosis**; keputusan klinis tetap pada dokter.
""")

    with t[1]:
        st.markdown("**Kamus data: arti tiap kolom**")
        st.dataframe(pd.DataFrame([
            ["patients.csv (CSV)", "patient_id", "ID unik pasien, mis. P00001", "Kunci penghubung ke dua sumber lain"],
            ["patients.csv (CSV)", "umur", "Umur dalam tahun", "Bisa kosong"],
            ["patients.csv (CSV)", "gender", "L atau P", ""],
            ["patients.csv (CSV)", "riwayat_penyakit", "hipertensi, jantung koroner, diabetes, asma, tidak ada", ""],
            ["vitals.json (JSON)", "timestamp", "Waktu pembacaan sensor", "Satu pasien punya banyak pembacaan"],
            ["vitals.json (JSON)", "heart_rate", "Detak jantung (bpm)", "Wajar 30-220 di proyek ini; istirahat dewasa sehat ±60-100"],
            ["vitals.json (JSON)", "spo2", "Saturasi oksigen (%)", "Wajar 70-100; ≥95 umumnya normal"],
            ["vitals.json (JSON)", "temperature", "Suhu tubuh (°C)", "Wajar 34-42"],
            ["clinical_notes.txt (TXT)", "patient_id | tanggal | catatan", "Satu baris per catatan dokter, dipisah tanda |", "Kalimat bebas, mis. 'nyeri dada sebelah kiri'"]],
            columns=["Sumber", "Kolom", "Arti", "Catatan"]), hide_index=True)
        st.markdown("""
**Dari mana datanya?** Semuanya **sintetik** (dibuat `01_generate_data.py`), sehingga tidak ada data pasien asli. Dua rancangan penting:

- **15% pasien sengaja dibuat "berisiko jantung"**: detak jantung lebih tinggi, SpO2 lebih rendah, umur lebih tua, dan 60% catatannya berisi keluhan jantung. Ini yang membuat ada sesuatu yang bermakna untuk ditemukan.
- **Kotoran disuntikkan sengaja** supaya Veracity bisa dibuktikan: SpO2 mustahil (±3% pembacaan), detak jantung mustahil (±1%), pasien duplikat (±2%), umur kosong (±2%), serta tag HTML, simbol aneh, dan huruf kapital acak pada catatan.
""")
        if VOL:
            d = VOL["dataset"]
            st.caption(f"Pada jalan terakhir: {fmt(d[0]['baris'])} pasien, {fmt(d[1]['baris'])} pembacaan sensor, {fmt(d[2]['baris'])} catatan klinis.")

    with t[2]:
        st.dataframe(pd.DataFrame([
            ["1", "Membuat data", "—", "3 file mentah di data/raw/", "01_generate_data.py", "Tidak memakai data pasien asli (aman dan bisa diulang)."],
            ["2", "Membaca dan mengukur kualitas", "3 file mentah", "Angka masalah (sebelum)", "02", "Masalah harus terukur dulu sebelum dibersihkan."],
            ["3", "Membersihkan", "Data mentah", "Data bersih + angka sesudah", "02", "Setiap aturan punya alasan; hasilnya dicek ulang."],
            ["4", "Mengintegrasikan", "3 sumber bersih", "integrated.parquet (1 baris per pasien)", "02", "Agregasi dulu, baru join, agar baris tidak meledak."],
            ["5", "Mengukur skala dan struktur", "Parquet bersih", "benchmark.json", "03", "Memisahkan efek jumlah baris dari efek struktur query."],
            ["6", "Mensimulasikan data baru", "File baru tiap ±3 detik", "Statistik latency + window", "04, 05", "Mengukur apakah sistem mengejar kedatangan data."],
            ["7", "Menjawab pertanyaan klinis", "Data terintegrasi", "Kelompok risiko + 4 query + evaluasi", "06", "Mengubah data jadi keputusan prioritas."],
            ["8", "Tanya jawab bebas", "Pertanyaan dokter", "Ringkasan klinis", "agent.py", "Menggabungkan angka sensor dan teks catatan."],
            ["9", "Menampilkan", "Semua laporan JSON", "Dashboard ini", "app.py", "Bukti dibaca dari hasil, bukan diketik."]],
            columns=["#", "Langkah", "Masukan", "Keluaran", "File", "Mengapa"]), hide_index=True)

        st.markdown("**Contoh perjalanan satu pasien**")
        if VAR:
            smp = VAR["integrasi"]["sampel"]
            r = next((x for x in smp if _tier(x) == "Tinggi"), smp[0])
            rasio = (r["n_keluhan_jantung"] / r["n_notes"]) if r.get("n_notes") else 0
            st.markdown(f"""
Ambil pasien **{r['patient_id']}** dari tabel terintegrasi:

1. **CSV**: umur {r['umur']}, gender {r['gender']}, riwayat *{r['riwayat_penyakit']}*.
2. **JSON**: {r['n_vitals']} pembacaan sensor yang valid, dengan rata-rata detak jantung **{r['avg_hr']} bpm** dan SpO2 **{r['avg_spo2']}%**.
3. **TXT**: {r['n_notes']} catatan dokter, {r['n_keluhan_jantung']} di antaranya menyebut keluhan jantung, yaitu **{rasio * 100:.0f}%**.
4. **Aturan risiko**: HR > {RISK_HR}? {'ya' if r['avg_hr'] > RISK_HR else 'tidak'} · SpO2 < {RISK_SPO2}? {'ya' if r['avg_spo2'] < RISK_SPO2 else 'tidak'} · keluhan ≥ {RISK_RASIO * 100:.0f}%? {'ya' if rasio >= RISK_RASIO else 'tidak'}
   → kelompok **{_tier(r)}**.
""")
        else:
            st.caption("Jalankan `python 02_etl_variety_veracity.py` untuk menampilkan contoh pasien nyata.")

    with t[3]:
        pts = []
        if VOL:
            d = VOL["dataset"]
            pts.append(f"**Data:** {fmt(sum(x['baris'] for x in d))} baris dari tiga format diproses; ukuran {sum(x['mb_mentah'] for x in d):.0f} MB mentah menjadi {sum(x['mb_parquet'] for x in d):.0f} MB Parquet.")
        if VER:
            dm = VER["dampak"]
            pts.append(f"**Kualitas:** {fmt(sum(i['sebelum'] for i in VER['masalah']))} temuan masalah berkurang menjadi {fmt(sum(i['sesudah'] for i in VER['masalah']))}. "
                       f"Tanpa pembersihan, rata-rata SpO2 tampak {dm['avg_spo2_mentah']}% (mustahil) dan {fmt(dm['pasien_hr_tinggi_mentah'])} pasien tampak ber-HR tinggi, padahal yang sebenarnya {fmt(dm['pasien_hr_tinggi_bersih'])}.")
        if BEN:
            A, B = BEN["operasi"]["A"], BEN["operasi"]["B"]
            pts.append(f"**Kinerja:** query relasional (Operasi B) {B['median_s'] / max(A['median_s'], 1e-9):.1f}× lebih lama dari agregasi sederhana (A) pada input {B['baris_input'] / A['baris_input']:.1f}× lebih banyak, yaitu pengaruh struktur dan bukan hanya jumlah baris.")
        st_ = baca_stats()
        if st_:
            pts.append(f"**Kecepatan:** latency median pemrosesan data baru {pd.DataFrame(st_)['latency'].median():.1f} detik (hasil simulasi terakhir).")
        if VAL:
            q1 = pd.DataFrame(VAL["queries"][0]["baris"])
            ringkas = ", ".join(f"{r.tier}: {int(r.n_pasien)}" for r in q1.itertuples())
            pts.append(f"**Temuan klinis:** pembagian pasien per kelompok risiko → {ringkas}. Kelompok Tinggi punya detak jantung lebih tinggi, SpO2 lebih rendah, dan jauh lebih banyak keluhan jantung (detail di tab Value).")
        if pts:
            for p in pts:
                st.markdown(f"- {p}")
        else:
            st.info("Jalankan `python run_all.py` agar ringkasan hasil muncul di sini.")
        st.markdown("""
**Yang belum/tidak diklaim**

- Hasil tidak membuktikan efektivitas klinis: datanya sintetik dan ambang risikonya ambang demo.
- Bukan sistem real-time sungguhan: kedatangan data disimulasikan.
- **Pengembangan lanjutan:** data de-identifikasi nyata (mis. MIMIC), Structured Streaming di cluster, embedding semantik untuk RAG, ambang risiko dari pedoman klinis.
""")

    with t[4]:
        st.caption("Tiap anggota harus bisa menjelaskan bagiannya dan menjawab pertanyaan penguji, bukan hanya membacakan skrip.")
        st.dataframe(pd.DataFrame([
            ["Project Lead / Analyst", "M1", "1", "Ringkasan (halaman ini)", "Mengapa masalah ini cocok untuk Big Data? Apa bedanya ukuran data dengan kompleksitasnya?"],
            ["System Engineer", "M2", "2", "Arsitektur", "Mengapa Parquet dan PySpark? Apa yang berubah bila dijalankan di cluster HDFS?"],
            ["Data Engineer", "M3-M4", "3", "Variety, Veracity · 02", "Mengapa umur kosong diisi rata-rata? Apa risikonya? Mengapa LEFT JOIN?"],
            ["Big Data Developer", "M5-M6", "4", "Volume, Velocity · 03, 05", "Mengapa Spark bisa lebih lambat di data kecil? Apa arti latency dan Exchange?"],
            ["Data Analyst", "M7", "5", "Value · 06, agent.py", "Mengapa precision sempurna tidak boleh dipercaya begitu saja? Apa batasan aturan risiko?"]],
            columns=["Peran", "Modul", "Segmen video", "Bagian dashboard dan file", "Contoh pertanyaan yang mungkin ditanyakan"]), hide_index=True)

    with t[5]:
        st.dataframe(pd.DataFrame([
            ["Big Data 5V", "Lima ciri data besar: Volume, Velocity, Variety, Veracity, Value."],
            ["Terstruktur / semi / tidak terstruktur", "Tabel baku (CSV) / punya kunci tetapi fleksibel (JSON) / teks bebas (TXT)."],
            ["ETL", "Extract-Transform-Load: ambil data, ubah/bersihkan, simpan hasilnya."],
            ["PySpark / DataFrame", "Spark lewat Python; DataFrame adalah tabel terdistribusi yang diproses paralel."],
            ["Parquet", "Format simpan kolumnar dan terkompres; lebih kecil dan cepat dibaca daripada CSV."],
            ["Partisi", "Potongan data yang diproses paralel oleh tiap core."],
            ["Shuffle / Exchange", "Pemindahan data antar partisi (mis. saat join/groupBy); salah satu langkah termahal."],
            ["Join / LEFT JOIN", "Menggabungkan tabel pada kunci; LEFT JOIN menjaga semua baris tabel kiri meski tidak ada pasangan."],
            ["Micro-batch", "Memproses data yang datang dalam kelompok kecil secara berkala."],
            ["Latency / Throughput", "Waktu dari data tiba sampai selesai diproses / jumlah record yang diproses per detik."],
            ["Window", "Pengelompokan per rentang waktu (di sini 1 menit)."],
            ["Imputasi", "Mengisi nilai kosong dengan perkiraan (di sini rata-rata umur)."],
            ["RAG / TF-IDF", "Mengambil catatan relevan sebelum menjawab / pencocokan kata berbobot (bukan pemahaman makna)."],
            ["LangGraph", "Kerangka alur agen: node-node dengan jalur bersyarat."],
            ["Precision / Recall", "Dari yang diprediksi berisiko, berapa yang benar / dari yang sebenarnya berisiko, berapa yang tertangkap."],
            ["SpO2 / bpm", "Saturasi oksigen darah (%) / denyut per menit."],
            ["Sintetik", "Data buatan yang meniru pola nyata; aman dari sisi privasi tetapi bukan fakta nyata."]],
            columns=["Istilah", "Arti singkat"]), hide_index=True)


# ====================================================================== TAB: RINGKASAN
def tab_ringkasan():
    st.header("🫀 Dari data mentah menjadi keputusan klinis: 5V dalam satu pipeline")
    st.markdown("> **Pertanyaan sentral:** bagaimana 5V memengaruhi desain dan implementasi pipeline data kesehatan, "
                "dan informasi kesehatan bermakna apa yang bisa diperoleh darinya?")
    st.markdown("**Kasus:** *IoT-Based Patient Monitoring* dipadukan dengan *Longitudinal EHR* (data sintetik). "
                "**Pertanyaan klinis:** *pasien mana yang berisiko tinggi penyakit jantung, dan apa cirinya?*")

    stats = baca_stats()
    c = st.columns(5)
    if VOL:
        total = sum(d["baris"] for d in VOL["dataset"])
        mb_r, mb_p = sum(d["mb_mentah"] for d in VOL["dataset"]), sum(d["mb_parquet"] for d in VOL["dataset"])
        c[0].metric("📦 Volume", f"{fmt(total)} baris", f"{mb_r:.0f} MB → {mb_p:.0f} MB Parquet", delta_color="off")
    else:
        c[0].metric("📦 Volume", "—")
    if stats:
        lat = pd.DataFrame(stats)["latency"].median()
        c[1].metric("⚡ Velocity", f"{lat:.1f} dtk", "median latency (live)", delta_color="off")
    else:
        c[1].metric("⚡ Velocity", "—", "jalankan simulasi", delta_color="off")
    if VAR:
        c[2].metric("🧩 Variety", "3 → 1", "CSV + JSON + TXT → 1 tabel", delta_color="off")
    else:
        c[2].metric("🧩 Variety", "—")
    if VER:
        sb, sa = sum(i["sebelum"] for i in VER["masalah"]), sum(i["sesudah"] for i in VER["masalah"])
        c[3].metric("🛡️ Veracity", f"{fmt(sb)} → {fmt(sa)}", "temuan masalah sebelum → sesudah", delta_color="off")
    else:
        c[3].metric("🛡️ Veracity", "—")
    if VAL:
        q1 = pd.DataFrame(VAL["queries"][0]["baris"])
        n_t = int(q1.loc[q1["tier"] == "Tinggi", "n_pasien"].sum()) if len(q1) else 0
        c[4].metric("💎 Value", f"{fmt(n_t)} pasien", "kelompok risiko Tinggi", delta_color="off")
    else:
        c[4].metric("💎 Value", "—")

    st.divider()
    bagian_proyek()
    st.divider()
    st.subheader("Peta 5V: pertanyaan → bukti → file")
    st.dataframe(pd.DataFrame([
        ["📦 Volume", "Seberapa banyak data, bagaimana relasinya, dan apa yang membuat pemrosesan berat?", "Tab Volume", "02, 03", "2, 4"],
        ["⚡ Velocity", "Seberapa sering data tiba, dan apakah sistem memprosesnya tepat waktu?", "Tab Velocity (live)", "04, 05", "2, 4"],
        ["🧩 Variety", "Bagaimana tiga format berbeda digabung menjadi satu?", "Tab Variety", "02", "3"],
        ["🛡️ Veracity", "Seburuk apa kualitas data, apa yang dilakukan, dan apa dampaknya?", "Tab Veracity", "02", "3, 5"],
        ["💎 Value", "Apa jawaban klinisnya, dan seberapa layak dipercaya?", "Tab Value", "06, agent.py", "5"]],
        columns=["V", "Pertanyaan yang dijawab", "Lihat di", "Script", "Segmen video"]), hide_index=True)

    st.subheader("Status bukti")
    ok = {"📦 Volume": VOL and BEN, "⚡ Velocity": bool(baca_stats()), "🧩 Variety": VAR, "🛡️ Veracity": VER, "💎 Value": VAL}
    st.write("  ".join(f"{'✅' if v else '⬜'} {k}" for k, v in ok.items()))
    if not all(ok.values()):
        st.caption("⬜ = belum ada bukti. Jalankan `python run_all.py`; Velocity dinyalakan dari tombol di tab Velocity.")

    with st.expander("🎬 Panduan video 5 menit: tab mana ditampilkan di segmen mana"):
        st.dataframe(pd.DataFrame([
            ["1", "0:45", "Project Lead / Analyst (M1)", "Ringkasan", "Masalah klinis, dataset, dan mengapa datanya menantang di kelima V"],
            ["2", "0:45", "System Engineer (M2)", "Arsitektur", "Alur data, mengapa PySpark + Parquet + micro-batch"],
            ["3", "1:00", "Data Engineer (M3-M4)", "Variety lalu Veracity", "3 format → 1 tabel; masalah kualitas sebelum/sesudah"],
            ["4", "1:15", "Big Data Developer (M5-M6)", "Volume lalu Velocity", "Benchmark, Operasi A vs B; simulasi live + latency"],
            ["5", "1:15", "Data Analyst (M7)", "Value", "Query Spark SQL, interpretasi, evaluasi, batasan; demo agen"]],
            columns=["Segmen", "Durasi", "Peran", "Tab", "Isi singkat"]), hide_index=True)


# ====================================================================== TAB: ARSITEKTUR
def tab_arsitektur():
    st.header("🏗️ Arsitektur dan alur data")
    st.caption("Diagram ini juga bisa di-screenshot untuk halaman 3 PDF. Warna kotak = V yang dibuktikan di tahap itu.")
    st.graphviz_chart(ARSITEKTUR)

    st.subheader("Mengapa arsitektur ini? (justifikasi tiap keputusan)")
    st.dataframe(pd.DataFrame([
        ["Pemrosesan", "PySpark (mode lokal)", "API sama dengan cluster: data diperbesar tanpa mengubah logika. Di satu laptop paralelisme hanya antar-core.", "Volume"],
        ["Penyimpanan", "Parquet (bukan CSV)", "Kolumnar dan terkompres: lebih kecil dan lebih cepat dibaca ulang (lihat rasio ukuran di tab Volume). HDFS tidak dipakai karena hanya 1 mesin; ganti path ke hdfs:// bila ada cluster.", "Volume"],
        ["Ingestion", "Micro-batch pantau folder", "Mensimulasikan data tiba berkala dan memungkinkan pengukuran latency. Bukan Structured Streaming karena itu butuh winutils di Windows (versi aslinya ada di 05b).", "Velocity"],
        ["Integrasi", "Pra-agregasi lalu join pada patient_id", "Join mentah vitals × notes menghasilkan puluhan kali lipat baris input (lihat tab Volume); agregasi dulu menekan ledakan itu.", "Variety · Volume"],
        ["Kualitas", "Aturan eksplisit + hitung sebelum/sesudah", "Setiap pembersihan harus terukur dan beralasan; bukan sekadar klaim 'sudah bersih'.", "Veracity"],
        ["Analitik", "Spark SQL", "Deklaratif, mudah ditunjukkan di video, dan menjalankan join + agregasi pada data bersih.", "Value"],
        ["Agen AI", "LangGraph: router → SQL → RAG → answer", "Pertanyaan dokter bisa butuh angka (sensor), teks (catatan), atau keduanya. RAG memakai TF-IDF (pencocokan kata, bukan makna).", "Value"]],
        columns=["Aspek", "Pilihan", "Alasan", "V terkait"]), hide_index=True)

    st.subheader("Urutan alur data")
    st.markdown("""
1. **Generate** data sintetik tiga format (dengan *noise* sengaja untuk Veracity).
2. **ETL PySpark** membaca ketiganya, mengukur masalah, membersihkan, lalu menggabungkan jadi tabel per pasien → **Parquet**.
3. **Benchmark** membandingkan Python vs Spark (skala) dan Operasi A vs B (struktur).
4. **Simulator** menulis file baru tiap 3 detik → **worker** memproses micro-batch dan mencatat latency → **live store**.
5. **Spark SQL** menjawab pertanyaan klinis; **agen LangGraph** menjawab pertanyaan bebas dokter.
6. **Streamlit** menampilkan semuanya.
""")


# ====================================================================== TAB: VOLUME
def tab_volume():
    judul_v("📦", "Volume",
            "Jumlah data yang dihasilkan, disimpan, dan diproses. Struktur relasi dan beban komputasi memengaruhi proses, "
            "tetapi *bukan* ukuran Volume itu sendiri, jadi ketiganya dipisahkan di sini.",
            "Rekam medis memiliki banyak pasien × banyak pembacaan sensor × banyak catatan. Data terus bertambah, dan "
            "menggabungkannya per pasien bisa jauh lebih berat daripada sekadar menghitung baris.")
    if not VOL:
        return belum_ada("02_etl_variety_veracity.py")

    st.subheader("1) Seberapa banyak data?")
    d = pd.DataFrame(VOL["dataset"])
    c = st.columns(4)
    c[0].metric("Pasien", fmt(d.loc[0, "baris"]))
    c[1].metric("Pembacaan sensor", fmt(d.loc[1, "baris"]))
    c[2].metric("Catatan klinis", fmt(d.loc[2, "baris"]))
    c[3].metric("Ukuran mentah → Parquet", f"{d['mb_mentah'].sum():.0f} → {d['mb_parquet'].sum():.0f} MB")
    d2 = d.rename(columns={"nama": "Dataset", "format": "Format", "baris": "Baris", "mb_mentah": "Ukuran mentah (MB)", "mb_parquet": "Parquet bersih (MB)"})
    st.dataframe(d2, hide_index=True)
    st.caption("Catatan: ini kisaran ratusan ribu baris (skala demo), bukan terabyte. Poin tugasnya adalah menjelaskan tantangan data dan "
               "membuktikan penanganannya, bukan sekadar besar.")

    st.subheader("2) Bagaimana relasinya?")
    r = VOL["relasi"]
    cA, cB = st.columns([1, 1])
    with cA:
        st.graphviz_chart(f'''digraph R {{ rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#fef3c7", fontname="Helvetica"]; edge [fontname="Helvetica", fontsize=10];
          p [label="patients\\n{fmt(d.loc[0, 'baris'])} baris"]; v [label="vitals\\n{fmt(d.loc[1, 'baris'])} baris"]; n [label="notes\\n{fmt(d.loc[2, 'baris'])} baris"];
          p -> v [label="1 : N  (±{r['vitals_per_pasien']} per pasien)"]; p -> n [label="1 : N  (±{r['notes_per_pasien']} per pasien)"]; }}''')
    with cB:
        x = r["baris_join_naif_vitals_x_notes"] / max(r["baris_input_vitals_notes"], 1)
        st.metric("Baris hasil join langsung vitals × notes", fmt(r["baris_join_naif_vitals_x_notes"]),
                  f"{x:.0f}× lebih banyak dari input ({fmt(r['baris_input_vitals_notes'])} baris)", delta_color="inverse")
        st.markdown("Jika dua tabel yang sama-sama banyak baris per pasien digabung langsung, hasilnya meledak. "
                    "Karena itu pipeline **mengagregasi dulu per pasien (atau per pasien-hari), baru join**.")

    if not BEN:
        return belum_ada("03_volume_benchmark.py")

    st.subheader("3) Berapa waktu proses saat data membesar? (Python 1 core vs PySpark)")
    h = pd.DataFrame(BEN["hasil"])
    chart = h.assign(ukuran=h["rows"].map(lambda v: f"{fmt(v)} baris")).set_index("ukuran")[["serial_s", "spark_s"]]
    chart.columns = ["Python serial (detik)", "PySpark (detik)"]
    cA, cB = st.columns([3, 2])
    cA.bar_chart(chart, stack=False)
    cB.dataframe(h.rename(columns={"rows": "Baris", "serial_s": "Serial (s)", "spark_s": "Spark (s)", "speedup": "Speed-up (×)", "partisi": "Partisi", "hasil_sama": "Hasil sama"}), hide_index=True)
    cross = next((x for x in BEN["hasil"] if x["speedup"] > 1), None)
    if cross:
        st.success(f"**Kesimpulan:** Spark mulai lebih cepat pada ≥ {fmt(cross['rows'])} baris (speed-up {cross['speedup']}×). "
                   "Di bawah itu, biaya start-up JVM, penjadwalan task, dan shuffle lebih besar daripada manfaat paralelisme.")
    else:
        last = BEN["hasil"][-1]
        st.warning(f"**Kesimpulan jujur:** pada ukuran terbesar yang diuji ({fmt(last['rows'])} baris) Spark belum lebih cepat ({last['speedup']}×). "
                   "Ini wajar: pada satu laptop dan data sekecil ini overhead Spark mendominasi. Keunggulan paralel baru terlihat pada data lebih besar.")
    st.caption(f"Spark berjalan dengan {BEN['cores']} core. Kedua cara menghasilkan hitungan kata kunci yang sama (kolom 'Hasil sama'), jadi perbandingannya adil.")

    st.subheader("4) Ukuran data vs kompleksitas struktur (Operasi A vs B)")
    A, B = BEN["operasi"]["A"], BEN["operasi"]["B"]
    st.dataframe(pd.DataFrame({
        "Aspek": ["Deskripsi", "Jumlah dataset", "Jumlah join", "Baris input", "Tahap Exchange (shuffle) di rencana eksekusi", "Waktu median (3 kali jalan)", "Baris hasil"],
        "Operasi A: agregasi sederhana": [A["deskripsi"], str(A["dataset"]), str(A["join"]), fmt(A["baris_input"]), str(A["exchange"]), f"{A['median_s']} s", fmt(A["baris_hasil"])],
        "Operasi B: relasional + temporal": [B["deskripsi"], str(B["dataset"]), str(B["join"]), fmt(B["baris_input"]), str(B["exchange"]), f"{B['median_s']} s", fmt(B["baris_hasil"])]}),
        hide_index=True)
    sr, tr = B["baris_input"] / A["baris_input"], B["median_s"] / max(A["median_s"], 1e-9)
    c = st.columns(3)
    c[0].metric("Baris input B ÷ A", f"{sr:.1f}×")
    c[1].metric("Waktu B ÷ A", f"{tr:.1f}×")
    c[2].metric("Exchange B vs A", f"{B['exchange']} vs {A['exchange']}")
    if tr > 1.5 * sr:
        st.success(f"**Kesimpulan:** B {tr:.1f}× lebih lama padahal inputnya hanya {sr:.1f}× lebih banyak. Selisih terutama berasal dari "
                   "**struktur dan desain query** (join, agregasi bertahap, lebih banyak shuffle), bukan dari jumlah baris.")
    else:
        st.info(f"**Kesimpulan:** waktu B ({tr:.1f}×) sebanding dengan selisih ukuran input ({sr:.1f}×), jadi pada skala ini pengaruh struktur belum menonjol.")
    batasan(["Dataset sintetik dan relatif kecil; tujuan benchmark adalah memisahkan efek ukuran, struktur, dan desain query.",
             "Waktu bergantung pada spesifikasi laptop dan beban saat pengukuran. Angka pada video harus berasal dari hasil jalan sendiri.",
             "Mode lokal: tidak ada paralelisme antar-mesin dan tidak ada HDFS."])


# ====================================================================== TAB: VELOCITY
def tab_velocity():
    judul_v("⚡", "Velocity",
            "Seberapa cepat data tiba, dan apakah pemrosesannya selesai cukup cepat dibanding laju kedatangan.",
            "Sensor pasien mengirim pembacaan tiap beberapa detik. Kalau pemrosesan lebih lambat dari kedatangan, antrean menumpuk "
            "dan peringatan untuk pasien yang memburuk datang terlambat.")
    c1, c2, c3 = st.columns([1, 1, 3])
    if c1.button("▶️ Mulai simulasi", type="primary"):
        mulai_simulasi()
    if c2.button("⏹️ Hentikan"):
        hentikan_simulasi()
    c3.markdown(f"Status: **{'🟢 berjalan (simulator + worker)' if _hidup('worker') and _hidup('gen') else '⚪ berhenti'}**  ·  "
                "simulator menulis file baru tiap ±3 detik; worker Spark memproses tiap batch.")
    st.caption("Alternatif lewat terminal: `python 05_ingest_worker.py` lalu `python 04_stream_generator.py` di terminal lain.")
    panel_live()
    batasan(["Kedatangan data **disimulasikan** (bukan sensor sungguhan), sesuai ketentuan tugas.",
             "Ini *micro-batch manual* dengan DataFrame Spark, **bukan** API Structured Streaming (butuh winutils di Windows). Versi aslinya ada di `05b_velocity_structured_streaming.py` untuk WSL2/Linux/Colab.",
             "Latency = waktu antara file ditulis simulator sampai Spark selesai memprosesnya (antre + proses). Batch pertama bisa lebih lambat karena pemanasan JVM."])


@st.fragment(run_every="3s")
def panel_live():
    rows = baca_stats()
    if not rows:
        st.info("Belum ada data live. Klik **Mulai simulasi**; grafik di bawah muncul dan memperbarui diri tiap 3 detik.")
        return
    df = pd.DataFrame(rows)
    for col in ["latency", "queue_s", "spark_s", "vit_in", "vit_ok", "not_in", "pat_in"]:
        if col not in df:
            df[col] = 0
    lat_med, lat_p95 = df["latency"].median(), df["latency"].quantile(0.95)
    interval = df["arr_interval"].dropna().iloc[-1] if "arr_interval" in df and df["arr_interval"].notna().any() else None
    rec = int((df["vit_in"] + df["not_in"] + df["pat_in"]).sum())
    thr = rec / max(df["spark_s"].sum(), 1e-9)
    ditolak = 1 - df["vit_ok"].sum() / max(df["vit_in"].sum(), 1)

    c = st.columns(5)
    c[0].metric("Batch diproses", fmt(len(df)))
    c[1].metric("Jeda kedatangan (median)", f"{interval:.1f} dtk" if interval else "—")
    c[2].metric("Latency median / p95", f"{lat_med:.1f} / {lat_p95:.1f} dtk")
    c[3].metric("Throughput", f"{thr:.0f} record/dtk")
    c[4].metric("Sensor ditolak (Veracity live)", f"{ditolak * 100:.1f}%")

    if interval:
        if lat_med < interval:
            st.success(f"**Sistem mengejar kedatangan:** latency median {lat_med:.1f} dtk < jeda antar file {interval:.1f} dtk, jadi tidak ada antrean menumpuk.")
        else:
            st.warning(f"**Sistem tertinggal:** latency median {lat_med:.1f} dtk ≥ jeda antar file {interval:.1f} dtk, sehingga antrean bisa menumpuk. "
                       "Kecilkan beban batch atau tambah sumber daya.")

    a, b = st.columns(2)
    a.markdown("**Latency per batch** (antre + proses Spark)")
    a.line_chart(df.set_index("batch")[["latency", "queue_s", "spark_s"]].rename(
        columns={"latency": "total latency (s)", "queue_s": "menunggu giliran (s)", "spark_s": "proses Spark (s)"}))
    b.markdown("**Rata-rata detak jantung per window 1 menit** (Spark `window`)")
    try:
        w = pd.DataFrame(json.loads((LIVE / "window.json").read_text(encoding="utf-8")))
        b.bar_chart(w.set_index("jendela")["avg_hr"])
    except Exception:
        b.caption("Window pertama sedang dihitung ...")

    st.markdown("**Peringatan dari data live:** pasien dengan rata-rata detak jantung tertinggi pada data yang baru masuk")
    try:
        lv = pd.read_csv(LIVE / "vitals.csv")
        top = (lv.groupby("patient_id").agg(rata_hr=("heart_rate", "mean"), rata_spo2=("spo2", "mean"), n=("heart_rate", "size"))
               .query("n >= 3").sort_values("rata_hr", ascending=False).head(5).round(1))
        st.dataframe(top)
        st.caption("Inilah gunanya Velocity: simulator sengaja membuat beberapa pasien baru memburuk (HR tinggi, SpO2 rendah), dan mereka muncul di sini dalam hitungan detik.")
    except Exception:
        st.caption("Menunggu data live ...")
    with st.expander("Detail 10 batch terakhir"):
        cols = [c for c in ["batch", "ts", "files", "vit_in", "vit_ok", "not_in", "pat_in", "queue_s", "spark_s", "latency"] if c in df]
        st.dataframe(df[cols].tail(10), hide_index=True)


# ====================================================================== TAB: VARIETY
def tab_variety():
    judul_v("🧩", "Variety",
            "Perbedaan sumber, format, struktur, dan representasi data.",
            "Satu pasien tersebar di tiga bentuk: tabel (CSV), pembacaan sensor (JSON), dan kalimat dokter (TXT). "
            "Keputusan klinis butuh ketiganya, jadi harus digabung, bukan sekadar didaftar.")
    if not VAR:
        return belum_ada("02_etl_variety_veracity.py")

    st.subheader("1) Tiga sumber, tiga struktur")
    for col, s in zip(st.columns(3), VAR["sumber"]):
        with col:
            st.markdown(f"#### {s['format']}")
            st.caption(f"{s['nama']} · **{s['struktur']}** · {fmt(s['baris'])} baris · {s['ukuran_mb']} MB")
            st.code(s["cara_baca"], language="python")
            st.markdown("**Skema**")
            st.dataframe(pd.DataFrame(s["skema"]), hide_index=True)
            st.markdown("**Contoh isi mentah**")
            st.dataframe(pd.DataFrame(s["sampel"]), hide_index=True)
            st.warning(s["tantangan"])

    st.subheader("2) Integrasi: tiga bentuk menjadi satu baris per pasien")
    ig = VAR["integrasi"]
    st.graphviz_chart('''digraph I { rankdir=LR; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11]; edge [fontsize=9];
      a [label="CSV pasien\\numur · gender · riwayat", fillcolor="#dbeafe"]; b [label="JSON sensor\\n→ agregasi per pasien\\n(avg HR, avg SpO2)", fillcolor="#dbeafe"];
      c [label="TXT catatan\\n→ bersihkan + deteksi keluhan\\n(jumlah keluhan jantung)", fillcolor="#dbeafe"];
      k [label="LEFT JOIN\\nkunci: patient_id", shape=diamond, fillcolor="#fef3c7"]; o [label="integrated.parquet\\n1 baris / pasien", shape=cylinder, fillcolor="#ede9fe"];
      a -> k; b -> k; c -> k; k -> o; }''')
    for i, langkah in enumerate(ig["langkah"], 1):
        st.markdown(f"{i}. {langkah}")
    m = st.columns(3)
    m[0].metric("Baris terintegrasi", fmt(ig["baris_terintegrasi"]))
    m[1].metric("Pasien dengan ketiga sumber", fmt(ig["pasien_punya_ketiga_sumber"]))
    m[2].metric("Cakupan integrasi", f"{ig['pasien_punya_ketiga_sumber'] / ig['baris_terintegrasi'] * 100:.1f}%")
    st.markdown("**Hasil integrasi (5 pasien pertama):**")
    st.dataframe(pd.DataFrame(ig["sampel"]), hide_index=True)
    st.success("**Kesimpulan:** tiga format yang tadinya tidak bisa dibandingkan langsung kini berbagi satu kunci dan satu skema. "
               "Satu tabel inilah yang memungkinkan pertanyaan seperti 'HR tinggi + SpO2 rendah + banyak keluhan jantung'.")
    batasan(["Integrasi bergantung pada kualitas `patient_id`: ID salah ketik akan menghasilkan pasien tanpa data pendamping (karena itu dipakai LEFT JOIN dan cakupan dilaporkan).",
             "Teks bebas hanya direduksi menjadi hitungan kata kunci; nuansa kalimat (negasi, tingkat keparahan) tidak tertangkap."])


# ====================================================================== TAB: VERACITY
def tab_veracity():
    judul_v("🛡️", "Veracity",
            "Akurasi, kelengkapan, konsistensi, dan keandalan data.",
            "Sensor bisa error (SpO2 150%), catatan bisa berisi sisa HTML, ID bisa ganda. Jika dibiarkan, rata-rata dan kelompok risiko "
            "yang dihitung menjadi salah, dan keputusan klinis ikut salah.")
    if not VER:
        return belum_ada("02_etl_variety_veracity.py")

    iss = pd.DataFrame(VER["masalah"])
    st.subheader("1) Masalah yang ditemukan, aturan penanganan, dan hasil ukurnya")
    tampil = iss.rename(columns={"sumber": "Sumber", "masalah": "Masalah", "sebelum": "Sebelum", "aturan": "Aturan penanganan",
                                 "sesudah": "Sesudah", "alasan": "Alasan keputusan"})[["Sumber", "Masalah", "Sebelum", "Aturan penanganan", "Sesudah", "Alasan keputusan"]]
    st.dataframe(tampil, hide_index=True)

    b = VER["baris"]
    denom = lambda s: b["patients"][0] if s.startswith("patients") else b["vitals"][0] if s.startswith("vitals") else b["notes"][0]
    iss["pct"] = [round(r.sebelum / denom(r.sumber) * 100, 2) for r in iss.itertuples()]
    a, c = st.columns([3, 2])
    a.markdown("**Persentase baris terdampak sebelum pembersihan** (setelah pembersihan semuanya 0)")
    a.bar_chart(iss.set_index("masalah")["pct"], horizontal=True)
    c.markdown("**Jumlah baris sebelum → sesudah**")
    c.dataframe(pd.DataFrame([[k, fmt(v[0]), fmt(v[1]), fmt(v[0] - v[1])] for k, v in b.items()],
                             columns=["Data", "Sebelum", "Sesudah", "Dibuang"]), hide_index=True)
    c.caption(f"Pembacaan sensor yang melanggar ≥1 aturan: {fmt(VER['baris_vitals_bermasalah_mentah'])} → {fmt(VER['baris_vitals_bermasalah_bersih'])}.")

    st.subheader("2) Mengapa ini penting? Dampak pada analisis")
    d = VER["dampak"]
    m = st.columns(3)
    m[0].metric("Rata-rata SpO2", f"{d['avg_spo2_bersih']}%", f"mentah {d['avg_spo2_mentah']}% (mustahil >100%)", delta_color="off")
    m[1].metric("Rata-rata detak jantung", f"{d['avg_hr_bersih']} bpm", f"mentah {d['avg_hr_mentah']} bpm", delta_color="off")
    m[2].metric("Pasien dengan rata-rata HR > 100", fmt(d["pasien_hr_tinggi_bersih"]), f"mentah {fmt(d['pasien_hr_tinggi_mentah'])} (positif palsu)", delta_color="off")
    if d["avg_spo2_mentah"] > 100:
        st.error(f"Pada data mentah, rata-rata SpO2 = **{d['avg_spo2_mentah']}%**, angka yang secara fisiologis tidak mungkin. Inilah bukti bahwa pembersihan wajib.")
    st.caption("Pasien yang rata-rata HR-nya tampak > 100 hanya karena sebuah pembacaan error (mis. 300 atau 999) akan salah dikira berisiko.")

    st.subheader("3) Contoh teks catatan: sebelum vs sesudah")
    st.dataframe(pd.DataFrame(VER["contoh_catatan"]).rename(columns={"catatan_mentah": "Sebelum (mentah)", "catatan": "Sesudah (bersih)"}), hide_index=True)
    st.success("**Kesimpulan:** kualitas data bukan klaim, melainkan angka. Setiap masalah diukur sebelum, ditangani dengan aturan yang bisa dijelaskan, lalu diukur lagi (0 pelanggaran tersisa).")
    batasan(["Rentang wajar (SpO2 70-100, HR 30-220, suhu 34-42) adalah ambang demo; klinisi bisa menetapkan batas lain.",
             "Umur kosong diisi **rata-rata**: umur itu perkiraan. Alternatif: tandai sebagai 'tidak diketahui' atau hapus baris, dengan konsekuensi berbeda.",
             "Noise pada data ini sengaja disuntikkan (sintetik). Pada data nyata, jenis dan proporsi masalahnya akan berbeda."])


# ====================================================================== TAB: VALUE
def tab_value():
    judul_v("💎", "Value",
            "Kegunaan informasi yang dihasilkan: apakah pemrosesan menjawab pertanyaan nyata dan hasilnya bisa dipercaya.",
            "Tujuan akhirnya bukan pipeline, tetapi membantu dokter menentukan pasien mana yang perlu ditinjau lebih dulu.")
    if not VAL:
        return belum_ada("06_value_queries.py")

    st.markdown(f"**Pertanyaan klinis:** *{VAL['pertanyaan']}*")
    r = VAL["aturan"]
    st.markdown(f"**Aturan kelompok (menggabungkan 3 sumber):** 🔴 **Tinggi** = rata-rata HR > {r['hr']} bpm **dan** SpO2 < {r['spo2']}% **dan** ≥ {r['rasio'] * 100:.0f}% catatannya memuat keluhan jantung · "
                f"🟠 **Sedang** = memenuhi sebagian · 🟢 **Rendah** = lainnya. *Ambang ini adalah ambang demo, bukan standar klinis.*")
    Q = {q["id"]: q for q in VAL["queries"]}

    def blok(q):
        st.subheader(q["judul"])
        st.caption(q["tujuan"])
        st.code(q["sql"], language="sql")
        st.caption(f"Dijalankan Spark SQL dalam {q['detik']} detik")
        return pd.DataFrame(q["baris"])

    q1 = blok(Q["q1"])
    c1, c2 = st.columns([3, 2])
    c1.dataframe(q1, hide_index=True)
    c2.bar_chart(q1.set_index("tier")[["rata_hr"]])
    t, rd = q1[q1["tier"] == "Tinggi"], q1[q1["tier"] == "Rendah"]
    if len(t) and len(rd):
        t, rd = t.iloc[0], rd.iloc[0]
        tot = int(q1["n_pasien"].sum())
        st.success(f"**Interpretasi:** {int(t.n_pasien)} dari {fmt(tot)} pasien ({t.n_pasien / tot * 100:.1f}%) masuk kelompok **Tinggi**. "
                   f"Rata-rata HR {t.rata_hr} vs {rd.rata_hr} bpm (selisih {t.rata_hr - rd.rata_hr:.1f}), SpO2 {t.rata_spo2}% vs {rd.rata_spo2}%, "
                   f"dan {t.rata_rasio_keluhan * 100:.0f}% catatan mereka memuat keluhan jantung vs {rd.rata_rasio_keluhan * 100:.0f}%. Usia rata-rata {t.rata_umur:.0f} vs {rd.rata_umur:.0f} tahun.")

    q2 = blok(Q["q2"])
    pv = q2.pivot_table(index="riwayat_penyakit", columns="tier", values="n_pasien", fill_value=0)
    c1, c2 = st.columns(2)
    c1.dataframe(pv)
    c2.bar_chart(pv)
    if "Tinggi" in pv:
        top = pv["Tinggi"].sort_values(ascending=False)
        top = top[top > 0]
        st.success("**Interpretasi:** pada kelompok Tinggi, riwayat penyakit terbanyak adalah "
                   + ", ".join(f"**{k}** ({int(v)})" for k, v in top.items())
                   + (f". Riwayat yang tidak muncul di kelompok Tinggi: {', '.join(pv.index.difference(top.index))}. (Pola ini memang dirancang di generator data sintetik, jadi bukan temuan klinis.)" if len(pv.index.difference(top.index)) else "."))

    q3 = blok(Q["q3"])
    st.dataframe(q3, hide_index=True)
    if len(q3):
        st.success(f"**Interpretasi:** pasien dengan rata-rata HR tertinggi adalah **{q3.iloc[0].patient_id}** ({q3.iloc[0].avg_hr} bpm, SpO2 {q3.iloc[0].avg_spo2}%). "
                   "Daftar ini adalah *antrean tinjauan* untuk dokter, bukan diagnosis.")

    q4 = blok(Q["q4"])
    pv4 = q4.pivot_table(index="tanggal", columns="tier", values="rata_hr")
    st.line_chart(pv4)
    if {"Tinggi", "Rendah"} <= set(pv4.columns):
        sel = (pv4["Tinggi"] - pv4["Rendah"]).dropna()
        stabil = (sel.max() - sel.min()) < 0.25 * sel.mean()
        st.success(f"**Interpretasi:** selisih HR harian kelompok Tinggi dibanding Rendah berkisar {sel.min():.0f}-{sel.max():.0f} bpm. "
                   + ("Selisihnya konsisten setiap hari, jadi perbedaan ini bukan kebetulan satu hari. Garis yang datar berarti data sintetik tidak memiliki tren naik/turun alami."
                      if stabil else "Selisihnya berubah-ubah antarhari, jadi perbedaan kelompok perlu diperiksa lebih lanjut sebelum disimpulkan."))

    st.subheader("Seberapa bisa dipercaya? Uji terhadap kunci jawaban")
    ev = VAL.get("evaluasi")
    if ev:
        e = ev["hanya_tinggi"]
        c = st.columns(4)
        c[0].metric("Precision (Tinggi)", e["precision"])
        c[1].metric("Recall (Tinggi)", e["recall"])
        c[2].metric("Pasien berisiko sebenarnya", fmt(ev["total_risiko_sebenarnya"]))
        c[3].metric("Terdeteksi benar (TP) / salah (FP)", f"{e['tp']} / {e['fp']}")
        st.dataframe(pd.DataFrame([[e["tn"], e["fp"]], [e["fn"], e["tp"]]], index=["Sebenarnya tidak berisiko", "Sebenarnya berisiko"],
                                  columns=["Diprediksi bukan Tinggi", "Diprediksi Tinggi"]))
        st.warning("**Jangan salah baca:** skor sebagus ini terjadi karena data sintetik dibuat dengan dua kelompok yang sangat terpisah. "
                   "Pada data nyata, kelompok saling tumpang tindih dan nilainya akan jauh lebih rendah. Bagian ini hanya membuktikan bahwa pipeline "
                   "(integrasi, pembersihan, aturan) bekerja benar, **bukan** bahwa aturan ini valid secara klinis.")
    else:
        st.info("Kunci jawaban tidak ditemukan (data/raw/_ground_truth.csv), jadi evaluasi dilewati.")
    batasan(["Data sintetik: temuan menggambarkan cara kerja pipeline, bukan fakta epidemiologi.",
             "Ambang risiko adalah ambang demo. Alat ini membantu memprioritaskan tinjauan, bukan mendiagnosis; keputusan akhir pada dokter.",
             "Korelasi dalam data tidak membuktikan sebab-akibat.",
             "Pasien tanpa sensor atau catatan masuk 'Data tidak lengkap' dan tidak dinilai.",
             "RAG di agen memakai TF-IDF (pencocokan kata), bukan pemahaman makna."])

    st.divider()
    agen()


def agen():
    st.subheader("🤖 Tanya agen AI (LangGraph + Spark SQL + RAG)")
    q = st.text_area("Pertanyaan dokter",
                     "Berdasarkan tren data IoT terbaru dan catatan klinis minggu ini, kelompokkan pasien yang memiliki risiko tinggi terkena penyakit jantung, dan berikan ringkasan kondisinya.",
                     height=90)
    if st.button("Analisis dengan agen"):
        try:
            from agent import ask
        except Exception as e:
            st.error(f"Agen tidak bisa dimuat: {type(e).__name__}: {e}")
        else:
            with st.spinner("Agen bekerja (Spark pertama kali butuh beberapa detik) ..."):
                try:
                    st.session_state["agen"] = ask(q)
                except Exception as e:
                    st.session_state["agen"] = None
                    st.error(f"Agen gagal: {type(e).__name__}: {e}. Pastikan `python 02_etl_variety_veracity.py` sudah dijalankan.")
    res = st.session_state.get("agen")
    if not res:
        return
    st.markdown("**Jalur yang dilalui agen** (hijau = node yang dijalankan)")
    st.graphviz_chart(dot_agen(res["trace"]))
    st.caption(" ➜ ".join(res["trace"]))
    if res.get("patients"):
        df = pd.DataFrame(res["patients"])
        a, b = st.columns(2)
        a.markdown("**Data sensor (Spark SQL)**")
        a.dataframe(df, hide_index=True)
        b.markdown(f"**Rata-rata detak jantung {res['days']} hari terakhir**")
        b.bar_chart(df.set_index("patient_id")["avg_hr"])
    if res.get("evidence"):
        st.markdown("**Bukti dari catatan klinis (RAG)**")
        st.dataframe(pd.DataFrame(res["evidence"]), hide_index=True)
    st.markdown("**Ringkasan klinis**")
    st.write(res["answer"])
    mode = res.get("llm_mode", "")
    if mode.startswith("Template"):
        st.warning(f"Mode jawaban: {mode}")
    else:
        st.caption(f"Mode jawaban: {mode}")


# ====================================================================== LAYOUT
st.title("5V Big Data pada Pipeline Data Kesehatan")
st.caption("PySpark · Parquet · Spark SQL · LangGraph · Streamlit  |  IFB-451 Big Data, Itenas")

with st.sidebar:
    st.markdown("### Cara menjalankan")
    st.code("python run_all.py\nstreamlit run app.py", language="bash")
    st.caption("`run_all.py` membuat data, ETL, benchmark, dan query Value. Velocity dinyalakan dari tombol di tab Velocity.")
    if st.button("🔄 Muat ulang laporan"):
        st.rerun()

T = st.tabs(["🏠 Ringkasan", "🏗️ Arsitektur", "📦 Volume", "⚡ Velocity", "🧩 Variety", "🛡️ Veracity", "💎 Value"])
for tab, fn in zip(T, [tab_ringkasan, tab_arsitektur, tab_volume, tab_velocity, tab_variety, tab_veracity, tab_value]):
    with tab:
        fn()