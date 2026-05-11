"""
Archivo: 03_batch_etl_spark.py
Proyecto: IoT Médico - Monitoreo de Pacientes Crónicos

Objetivo:
Procesar datos históricos clínicos usando Apache Spark.

ETL:
1. Extract  → lee CSV, JSON desde data/raw/
2. Transform → limpia, transforma, enriquece y une fuentes
3. Load     → guarda Parquet en data/processed/ y KPIs en output/kpis/

Temas trabajados:
- RDD, DataFrames, Spark SQL
- Joins entre 5 fuentes
- 6 reportes KPI exportados en CSV

Comando:
docker compose exec spark python src/03_batch_etl_spark.py
"""

from pathlib import Path
import json
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType
)


# ============================================================
# 1. Rutas
# ============================================================

BASE_DIR      = Path(_file_).resolve().parents[1]
RAW_DIR       = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
KPI_DIR       = BASE_DIR / "output" / "kpis"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
KPI_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. Sesión Spark
# ============================================================

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("IoTMedicoBatchETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# 3. Funciones auxiliares
# ============================================================

def read_csv(spark, filename: str):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .csv(str(RAW_DIR / filename))
    )


def write_single_csv(df, filename: str) -> None:
    """Guarda un DataFrame Spark como un único CSV en output/kpis/."""
    final_path = KPI_DIR / filename
    temp_dir   = KPI_DIR / f"tmp{filename.replace('.csv','')}"

    if final_path.exists(): final_path.unlink()
    if temp_dir.exists():   shutil.rmtree(temp_dir)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))

    parts = list(temp_dir.glob("part-*.csv"))
    if not parts:
        raise FileNotFoundError(f"No se generó part en {temp_dir}")

    shutil.move(str(parts[0]), str(final_path))
    shutil.rmtree(temp_dir)
    print(f"  KPI guardado: output/kpis/{filename}")


def info(name: str, df) -> None:
    print(f"\n  [{name}] {df.count():,} registros | {len(df.columns)} columnas")


# ============================================================
# 4. Schema del JSON de signos vitales
# ============================================================

signos_schema = StructType([
    StructField("id_lectura",             StringType(),  True),
    StructField("id_paciente",            StringType(),  True),
    StructField("timestamp",              StringType(),  True),
    StructField("frecuencia_cardiaca_bpm",IntegerType(), True),
    StructField("spo2_porcentaje",        IntegerType(), True),
    StructField("temperatura_corporal_c", DoubleType(),  True),
    StructField("pasos_diarios",          IntegerType(), True),
    StructField("calorias_quemadas",      IntegerType(), True),
    StructField("nivel_estres",           StringType(),  True),
    StructField("horas_sueno",            DoubleType(),  True),
    StructField("alerta_generada",        BooleanType(), True),
    StructField("detalle_alerta",         StringType(),  True),
    StructField("dispositivo", StructType([
        StructField("marca",    StringType(), True),
        StructField("firmware", StringType(), True),
    ]), True),
])


# ============================================================
# 5. Proceso principal
# ============================================================

def main() -> None:
    print("=" * 70)
    print("  ETL Batch - IoT Médico con Apache Spark")
    print("=" * 70)

    spark = create_spark_session()

    # ─────────────────────────────────────────────────────────
    # EXTRACT: leer las 5 fuentes
    # ─────────────────────────────────────────────────────────
    print("\n  [EXTRACT] Leyendo archivos desde data/raw/ ...")

    pacientes_df    = read_csv(spark, "pacientes.csv")
    dispositivos_df = read_csv(spark, "dispositivos.csv")
    medicamentos_df = read_csv(spark, "medicamentos.csv")
    alertas_df      = read_csv(spark, "alertas_clinicas.csv")

    # JSON anidado con multiLine
    signos_df_raw = (
        spark.read
        .option("multiLine", True)
        .schema(signos_schema)
        .json(str(RAW_DIR / "signos_vitales.json"))
    )

    # Aplanar campo anidado dispositivo
    signos_df = signos_df_raw.select(
        "id_lectura","id_paciente","timestamp",
        "frecuencia_cardiaca_bpm","spo2_porcentaje",
        "temperatura_corporal_c","pasos_diarios","calorias_quemadas",
        "nivel_estres","horas_sueno","alerta_generada","detalle_alerta",
        F.col("dispositivo.marca").alias("marca_dispositivo"),
        F.col("dispositivo.firmware").alias("firmware"),
    )

    info("pacientes_df",    pacientes_df)
    info("dispositivos_df", dispositivos_df)
    info("signos_df",       signos_df)
    info("medicamentos_df", medicamentos_df)
    info("alertas_df",      alertas_df)

    print("\n  Schema de signos_df:")
    signos_df.printSchema()

    # ─────────────────────────────────────────────────────────
    # TRANSFORM: limpiar y enriquecer signos vitales
    # ─────────────────────────────────────────────────────────
    print("\n  [TRANSFORM] Limpiando y enriqueciendo datos ...")

    signos_clean = (
        signos_df
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("fecha",     F.to_date("timestamp"))
        .withColumn("hora",      F.hour("timestamp"))
        .withColumn("alerta_int",F.col("alerta_generada").cast("int"))
        .withColumn(
            "clasificacion_fc",
            F.when(F.col("frecuencia_cardiaca_bpm") < 60, "bradicardia")
             .when(F.col("frecuencia_cardiaca_bpm") <= 100, "normal")
             .when(F.col("frecuencia_cardiaca_bpm") <= 120, "taquicardia_leve")
             .otherwise("taquicardia_severa")
        )
        .withColumn(
            "clasificacion_spo2",
            F.when(F.col("spo2_porcentaje") >= 95, "normal")
             .when(F.col("spo2_porcentaje") >= 90, "hipoxemia_leve")
             .otherwise("hipoxemia_severa")
        )
        .withColumn(
            "turno",
            F.when((F.col("hora") >= 6)  & (F.col("hora") < 14), "mañana")
             .when((F.col("hora") >= 14) & (F.col("hora") < 22), "tarde")
             .otherwise("noche")
        )
    )

    # Enriquecer con datos del paciente
    signos_enriquecidos = (
        signos_clean
        .join(
            pacientes_df.select(
                "id_paciente","diagnostico_principal","nivel_riesgo",
                "especialidad","distrito","seguro_salud","edad","sexo"
            ),
            on="id_paciente", how="left"
        )
    )

    print("\n  Vista previa signos enriquecidos:")
    signos_enriquecidos.select(
        "id_lectura","id_paciente","frecuencia_cardiaca_bpm",
        "spo2_porcentaje","clasificacion_fc","clasificacion_spo2",
        "nivel_riesgo","alerta_generada"
    ).show(8, truncate=False)

    # ─────────────────────────────────────────────────────────
    # LOAD: guardar Parquet
    # ─────────────────────────────────────────────────────────
    print("\n  [LOAD] Guardando dataset procesado en Parquet ...")

    parquet_path = PROCESSED_DIR / "signos_enriquecidos.parquet"
    if parquet_path.exists(): shutil.rmtree(parquet_path)

    signos_enriquecidos.write.mode("overwrite").parquet(str(parquet_path))
    print("  Parquet guardado: data/processed/signos_enriquecidos.parquet")

    # ─────────────────────────────────────────────────────────
    # SPARK SQL: vistas temporales
    # ─────────────────────────────────────────────────────────
    print("\n  [SPARK SQL] Creando vistas temporales ...")

    signos_enriquecidos.createOrReplaceTempView("signos")
    pacientes_df.createOrReplaceTempView("pacientes")
    medicamentos_df.createOrReplaceTempView("medicamentos")
    alertas_df.createOrReplaceTempView("alertas")

    # ─────────────────────────────────────────────────────────
    # KPI 1: Alertas por nivel de riesgo
    # ─────────────────────────────────────────────────────────
    kpi1 = spark.sql("""
        SELECT
            nivel_riesgo,
            COUNT(*)                              AS total_lecturas,
            SUM(alerta_int)                       AS total_alertas,
            ROUND(AVG(frecuencia_cardiaca_bpm),1) AS fc_promedio,
            ROUND(AVG(spo2_porcentaje),1)         AS spo2_promedio,
            ROUND(AVG(alerta_int)*100,2)          AS pct_alertas
        FROM signos
        GROUP BY nivel_riesgo
        ORDER BY total_alertas DESC
    """)
    print("\n  KPI 1: Alertas por nivel de riesgo")
    kpi1.show()
    write_single_csv(kpi1, "kpi1_alertas_por_riesgo.csv")

    # ─────────────────────────────────────────────────────────
    # KPI 2: Signos vitales por diagnóstico
    # ─────────────────────────────────────────────────────────
    kpi2 = spark.sql("""
        SELECT
            diagnostico_principal,
            COUNT(*)                              AS total_lecturas,
            ROUND(AVG(frecuencia_cardiaca_bpm),1) AS fc_promedio,
            ROUND(AVG(spo2_porcentaje),1)         AS spo2_promedio,
            ROUND(AVG(temperatura_corporal_c),2)  AS temp_promedio,
            SUM(alerta_int)                       AS total_alertas
        FROM signos
        GROUP BY diagnostico_principal
        ORDER BY total_alertas DESC
    """)
    print("\n  KPI 2: Signos vitales por diagnóstico")
    kpi2.show(truncate=False)
    write_single_csv(kpi2, "kpi2_signos_por_diagnostico.csv")

    # ─────────────────────────────────────────────────────────
    # KPI 3: Alertas por distrito
    # ─────────────────────────────────────────────────────────
    kpi3 = spark.sql("""
        SELECT
            distrito,
            COUNT(*)       AS total_lecturas,
            SUM(alerta_int)AS total_alertas,
            ROUND(SUM(alerta_int)100.0/COUNT(),2) AS pct_alertas
        FROM signos
        GROUP BY distrito
        ORDER BY total_alertas DESC
        LIMIT 15
    """)
    print("\n  KPI 3: Alertas por distrito")
    kpi3.show(truncate=False)
    write_single_csv(kpi3, "kpi3_alertas_por_distrito.csv")

    # ─────────────────────────────────────────────────────────
    # KPI 4: Medicamentos más recetados y adherencia
    # ─────────────────────────────────────────────────────────
    kpi4 = spark.sql("""
        SELECT
            nombre_generico,
            COUNT(*)                        AS total_recetas,
            ROUND(AVG(costo_total),2)       AS costo_promedio,
            SUM(CASE WHEN nivel_adherencia='alta'    THEN 1 ELSE 0 END) AS adherencia_alta,
            SUM(CASE WHEN nivel_adherencia='baja'    THEN 1 ELSE 0 END) AS adherencia_baja,
            SUM(CASE WHEN cubierto_seguro='SI'       THEN 1 ELSE 0 END) AS con_cobertura
        FROM medicamentos
        GROUP BY nombre_generico
        ORDER BY total_recetas DESC
        LIMIT 10
    """)
    print("\n  KPI 4: Top medicamentos recetados")
    kpi4.show(truncate=False)
    write_single_csv(kpi4, "kpi4_medicamentos.csv")

    # ─────────────────────────────────────────────────────────
    # KPI 5: Tipos de alerta clínica
    # ─────────────────────────────────────────────────────────
    kpi5 = spark.sql("""
        SELECT
            tipo_alerta,
            nivel_criticidad,
            COUNT(*)  AS total,
            SUM(CASE WHEN estado_alerta='resuelta'    THEN 1 ELSE 0 END) AS resueltas,
            SUM(CASE WHEN estado_alerta='pendiente'   THEN 1 ELSE 0 END) AS pendientes,
            ROUND(AVG(tiempo_respuesta_min),1)        AS tiempo_resp_promedio
        FROM alertas
        GROUP BY tipo_alerta, nivel_criticidad
        ORDER BY total DESC
    """)
    print("\n  KPI 5: Tipos de alerta clínica")
    kpi5.show(truncate=False)
    write_single_csv(kpi5, "kpi5_tipos_alerta.csv")

    # ─────────────────────────────────────────────────────────
    # KPI 6: Pacientes críticos con más alertas
    # ─────────────────────────────────────────────────────────
    kpi6 = spark.sql("""
        SELECT
            s.id_paciente,
            p.diagnostico_principal,
            p.nivel_riesgo,
            p.distrito,
            COUNT(s.id_lectura)  AS total_lecturas,
            SUM(s.alerta_int)    AS total_alertas,
            ROUND(AVG(s.frecuencia_cardiaca_bpm),1) AS fc_promedio,
            ROUND(AVG(s.spo2_porcentaje),1)         AS spo2_promedio
        FROM signos s
        JOIN pacientes p ON s.id_paciente = p.id_paciente
        WHERE p.nivel_riesgo IN ('alto','crítico')
        GROUP BY s.id_paciente, p.diagnostico_principal, p.nivel_riesgo, p.distrito
        HAVING total_alertas >= 3
        ORDER BY total_alertas DESC
        LIMIT 20
    """)
    print("\n  KPI 6: Pacientes críticos con más alertas")
    kpi6.show(truncate=False)
    write_single_csv(kpi6, "kpi6_pacientes_criticos.csv")

    # ─────────────────────────────────────────────────────────
    # RDD: resumen de clasificación de FC
    # ─────────────────────────────────────────────────────────
    print("\n  [RDD] Procesando clasificación de FC desde CSV ...")

    rdd_raw = spark.sparkContext.textFile(str(RAW_DIR / "signos_vitales.json"))

    # En el JSON cada lectura tiene "frecuencia_cardiaca_bpm"
    # Usamos RDD para extraer y clasificar ese campo
    def clasificar_fc(linea: str) -> tuple:
        if '"frecuencia_cardiaca_bpm"' not in linea:
            return None
        try:
            val = int(linea.strip().split(":")[1].strip().rstrip(","))
            if val < 60:   return ("bradicardia",    1)
            if val <= 100: return ("normal",          1)
            if val <= 120: return ("taquicardia_leve",1)
            return              ("taquicardia_severa",1)
        except Exception:
            return None

    rdd_fc = (
        rdd_raw
        .map(clasificar_fc)
        .filter(lambda x: x is not None)
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    rdd_df = spark.createDataFrame(
        rdd_fc, ["clasificacion_fc", "total_lecturas"]
    ).orderBy(F.desc("total_lecturas"))

    print("\n  RDD: Distribución de clasificación de FC")
    rdd_df.show()
    write_single_csv(rdd_df, "rdd_clasificacion_fc.csv")

    # ─────────────────────────────────────────────────────────
    # Cierre
    # ─────────────────────────────────────────────────────────
    print("=" * 70)
    print("  ETL batch finalizado correctamente")
    print("  Datos procesados : data/processed/")
    print("  KPIs generados   : output/kpis/")
    print("=" * 70)

    spark.stop()


if _name_ == "_main_":
    main()