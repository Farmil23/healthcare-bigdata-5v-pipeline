"""
OPSIONAL - V2 VELOCITY dengan API Spark Structured Streaming asli.

TIDAK dipakai di Windows tanpa winutils: checkpoint streaming butuh Hadoop. Jalankan di WSL2/Linux/Colab.
Untuk Windows pakai 05_velocity_microbatch.py.

Jalankan SETELAH 04_stream_generator.py aktif di terminal lain.
Dua hal ditampilkan real-time di konsol:
  1. Rata-rata detak jantung per tumbling window 1 menit
  2. Latency: selisih waktu data dibuat sensor vs saat Spark memprosesnya
"""
from datetime import datetime

from pyspark.sql import functions as F

from config import INCOMING as STREAM_DIR
from spark_session import get_spark

spark = get_spark("Velocity")
STREAM_DIR.mkdir(parents=True, exist_ok=True)

schema = "patient_id STRING, timestamp TIMESTAMP, heart_rate INT, temperature DOUBLE, spo2 INT"
stream = spark.readStream.schema(schema).option("pathGlobFilter", "vitals_*.json").json(str(STREAM_DIR))   # Spark memantau folder ini

# --- 1. Tumbling window 1 menit: rata-rata detak jantung ---
window_avg = (
    stream.withWatermark("timestamp", "1 minute")
    .groupBy(F.window("timestamp", "1 minute"))
    .agg(F.round(F.avg("heart_rate"), 1).alias("avg_heart_rate"),
         F.count("*").alias("jumlah_record"))
)
q1 = (window_avg.writeStream.outputMode("update").format("console")
      .option("truncate", False).trigger(processingTime="3 seconds").start())


# --- 2. Latency tiap micro-batch ---
def tampilkan_latency(df, batch_id):
    if df.isEmpty():
        return
    s = df.agg(F.count("*").alias("n"), F.max("timestamp").alias("terbaru")).first()
    latency = (datetime.now() - s["terbaru"]).total_seconds()
    print(f">>> batch {batch_id}: {s['n']} record baru | latency ~ {latency:.1f} detik")


q2 = stream.writeStream.foreachBatch(tampilkan_latency).trigger(processingTime="3 seconds").start()

spark.streams.awaitAnyTermination()
