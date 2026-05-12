"""
Archivo: 04_streaming_spark_kafka.py
Proyecto: IoT Médico - Monitoreo de Pacientes Crónicos

Objetivo:
Leer eventos de signos vitales desde Kafka usando Spark Structured Streaming,
procesarlos en micro-batches y generar alertas clínicas en tiempo real.

Topic Kafka:
- iot-medico-events

Salidas:
- output/streaming/events/          (todos los eventos por batch)
- output/streaming/alertas/         (solo eventos críticos)
- output/streaming/resumen_tipo.csv (resumen por tipo de evento)
- output/streaming/resumen_distrito.csv

Reglas de alerta implementadas:
1. FC > 110 bpm sostenida O SpO2 < 93% → alerta urgente
2. risk_score >= 0.70 → evento de alto riesgo

Comando:
docker compose exec spark python src/04_streaming_spark_kafka.py --duration 180
"""

from pathlib import Path
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType
)


# ============================================================
# 1. Configuración
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

STREAMING_DIR  = BASE_DIR / "output" / "streaming"
EVENTS_DIR     = STREAMING_DIR / "events"
ALERTAS_DIR    = STREAMING_DIR / "alertas"
CHECKPOINT_DIR = BASE_DIR / "data" / "checkpoints" / "iot_medico_streaming"

for d in [STREAMING_DIR, EVENTS_DIR, ALERTAS_DIR, CHECKPOINT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

KAFKA_TOPIC             = "iot-medico-events"
KAFKA_BOOTSTRAP_SERVERS = "broker:19092"
KAFKA_SPARK_PACKAGE     = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"


# ============================================================
# 2. Schema del evento JSON
# ============================================================

event_schema = StructType([
    StructField("event_id",               StringType(),  True),
    StructField("id_paciente",            StringType(),  True),
    StructField("event_type",             StringType(),  True),
    StructField("wearable",               StringType(),  True),
    StructField("distrito",               StringType(),  True),
    StructField("diagnostico_principal",  StringType(),  True),
    StructField("especialidad",           StringType(),  True),
    StructField("nivel_riesgo_historico", StringType(),  True),
    StructField("frecuencia_cardiaca_bpm",IntegerType(), True),
    StructField("spo2_porcentaje",        IntegerType(), True),
    StructField("temperatura_corporal_c", DoubleType(),  True),
    StructField("nivel_estres",           StringType(),  True),
    StructField("pasos_ultimo_registro",  IntegerType(), True),
    StructField("risk_score",             DoubleType(),  True),
    StructField("es_critico",             BooleanType(), True),
    StructField("requiere_atencion",      BooleanType(), True),
    StructField("event_timestamp",        StringType(),  True),
])


# ============================================================
# 3. Sesión Spark
# ============================================================

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("IoTMedicoKafkaStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# 4. Guardar CSV acumulativo
# ============================================================

def append_csv(pdf, output_file: Path) -> None:
    write_header = not output_file.exists()
    pdf.to_csv(output_file, mode="a", header=write_header,
               index=False, encoding="utf-8")


# ============================================================
# 5. Procesar cada micro-batch
# ============================================================

def process_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"  Batch {batch_id}: sin eventos")
        return

    print("\n" + "=" * 70)
    print(f"  Batch streaming #{batch_id}")
    print("=" * 70)

    batch_df.cache()
    total = batch_df.count()
    print(f"  Eventos recibidos: {total}")

    # Vista previa
    print("\n  Muestra de eventos:")
    batch_df.select(
        "event_id","id_paciente","event_type","frecuencia_cardiaca_bpm",
        "spo2_porcentaje","risk_score","es_critico"
    ).show(8, truncate=False)

    # ── Resumen por tipo de evento ────────────────────────────
    resumen_tipo = (
        batch_df
        .groupBy("event_type")
        .agg(
            F.count("*").alias("total_eventos"),
            F.round(F.avg("risk_score"), 2).alias("risk_promedio"),
            F.sum(F.col("es_critico").cast("int")).alias("eventos_criticos"),
            F.round(F.avg("frecuencia_cardiaca_bpm"), 1).alias("fc_promedio"),
            F.round(F.avg("spo2_porcentaje"), 1).alias("spo2_promedio"),
        )
        .orderBy(F.desc("total_eventos"))
    )
    print("\n  Resumen por tipo de evento:")
    resumen_tipo.show(truncate=False)

    # ── Resumen por distrito ──────────────────────────────────
    resumen_distrito = (
        batch_df
        .groupBy("distrito")
        .agg(
            F.count("*").alias("total_eventos"),
            F.sum(F.col("es_critico").cast("int")).alias("criticos"),
            F.round(F.avg("risk_score"), 2).alias("risk_promedio"),
            F.round(F.avg("frecuencia_cardiaca_bpm"), 1).alias("fc_promedio"),
        )
        .orderBy(F.desc("criticos"))
    )
    print("\n  Resumen por distrito:")
    resumen_distrito.show(10, truncate=False)

    # ── REGLA DE ALERTA 1: FC > 110 O SpO2 < 93 ─────────────
    # ── REGLA DE ALERTA 2: risk_score >= 0.70    ─────────────
    alertas_df = (
        batch_df
        .filter(
            (F.col("frecuencia_cardiaca_bpm") > 110) |
            (F.col("spo2_porcentaje") < 93)          |
            (F.col("risk_score") >= 0.70)
        )
        .select(
            "event_id","id_paciente","event_type",
            "distrito","diagnostico_principal","especialidad",
            "nivel_riesgo_historico","wearable",
            "frecuencia_cardiaca_bpm","spo2_porcentaje",
            "temperatura_corporal_c","risk_score",
            "es_critico","requiere_atencion","event_timestamp"
        )
        .orderBy(F.desc("risk_score"))
    )

    total_alertas = alertas_df.count()
    print(f"\n  Alertas clínicas detectadas: {total_alertas}")

    if total_alertas > 0:
        alertas_df.show(10, truncate=False)

    # ── Guardar archivos del batch ────────────────────────────
    eventos_pdf = batch_df.toPandas()
    eventos_pdf["batch_id"] = batch_id
    eventos_pdf.to_csv(
        EVENTS_DIR / f"events_batch_{batch_id}.csv",
        index=False, encoding="utf-8"
    )

    if total_alertas > 0:
        alertas_pdf = alertas_df.toPandas()
        alertas_pdf["batch_id"] = batch_id
        alertas_pdf.to_csv(
            ALERTAS_DIR / f"alertas_batch_{batch_id}.csv",
            index=False, encoding="utf-8"
        )

    resumen_tipo_pdf = resumen_tipo.toPandas()
    resumen_tipo_pdf["batch_id"] = batch_id
    append_csv(resumen_tipo_pdf, STREAMING_DIR / "resumen_tipo.csv")

    resumen_dist_pdf = resumen_distrito.toPandas()
    resumen_dist_pdf["batch_id"] = batch_id
    append_csv(resumen_dist_pdf, STREAMING_DIR / "resumen_distrito.csv")

    batch_df.unpersist()

    print(f"\n  Archivos guardados para batch {batch_id}:")
    print(f"  - output/streaming/events/events_batch_{batch_id}.csv")
    if total_alertas > 0:
        print(f"  - output/streaming/alertas/alertas_batch_{batch_id}.csv")
    print("  - output/streaming/resumen_tipo.csv  (acumulativo)")
    print("  - output/streaming/resumen_distrito.csv  (acumulativo)")


# ============================================================
# 6. Función principal
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Streaming - IoT Médico")
    parser.add_argument("--duration", type=int, default=180,
                        help="Duración del streaming en segundos")
    args = parser.parse_args()

    print("=" * 70)
    print("  Spark Structured Streaming - IoT Médico")
    print("=" * 70)
    print(f"  Topic  : {KAFKA_TOPIC}")
    print(f"  Broker : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Duración: {args.duration} segundos")
    print("  Nota: primera ejecución puede demorar (descarga conector Kafka)")
    print("=" * 70)

    spark = create_spark_session()

    # Leer stream desde Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parsear JSON
    parsed_df = (
        kafka_df
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("message_value"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("json_data", F.from_json(F.col("message_value"), event_schema))
        .select("message_key","kafka_timestamp","json_data.*")
        .withColumn("event_timestamp",      F.to_timestamp("event_timestamp"))
        .withColumn("processing_timestamp", F.current_timestamp())
        .withColumn(
            "categoria_riesgo",
            F.when(F.col("risk_score") >= 0.70, "CRITICO")
             .when(F.col("risk_score") >= 0.40, "MODERADO")
             .otherwise("BAJO")
        )
    )

    # Iniciar query de streaming
    query = (
        parsed_df
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\n  Streaming iniciado.")
    print("  Ejecuta el productor en otra terminal:")
    print("  docker compose exec spark python src/02_generate_streaming_events.py --events 600 --delay 0.1")
    print()

    query.awaitTermination(args.duration)
    query.stop()

    print("=" * 70)
    print("  Streaming finalizado correctamente")
    print("  Resultados en: output/streaming/")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
