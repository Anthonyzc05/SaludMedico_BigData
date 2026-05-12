"""
Archivo: 02_generate_streaming_events.py
Proyecto: IoT Médico - Monitoreo de Pacientes Crónicos

Objetivo:
Generar eventos simulados de signos vitales en tiempo real
y enviarlos a Kafka como productor.

Topic usado:
- iot-medico-events

Tipos de eventos:
- lectura_normal       → lectura rutinaria del wearable sin anomalías
- lectura_anomala      → FC o SpO2 fuera de rango
- alerta_fc_alta       → frecuencia cardíaca crítica
- alerta_spo2_bajo     → saturación de oxígeno crítica
- alerta_temperatura   → temperatura corporal elevada
- evento_critico       → múltiples parámetros críticos simultáneos

Comando:
docker compose exec spark python src/02_generate_streaming_events.py --events 600 --delay 0.1
"""

from pathlib import Path
from datetime import datetime
import argparse
import json
import random
import time

import pandas as pd
from confluent_kafka import Producer


# ============================================================
# 1. Configuración
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "data" / "raw"

KAFKA_TOPIC             = "iot-medico-events"
KAFKA_BOOTSTRAP_SERVERS = "broker:19092"

EVENT_TYPES = [
    "lectura_normal",
    "lectura_anomala",
    "alerta_fc_alta",
    "alerta_spo2_bajo",
    "alerta_temperatura",
    "evento_critico",
]

EVENT_WEIGHTS = [0.40, 0.25, 0.13, 0.10, 0.07, 0.05]

NIVELES_ESTRES = ["bajo", "medio", "alto", "muy_alto"]

WEARABLES = [
    "Fitbit Sense 2", "Apple Watch Series 9",
    "Samsung Galaxy Watch 6", "Garmin Venu 3",
    "Fitbit Charge 6", "Xiaomi Mi Band 8",
]


# ============================================================
# 2. Cargar datos históricos de referencia
# ============================================================

def load_reference_data() -> dict:
    """
    Carga pacientes.csv y dispositivos.csv generados por 01_.
    Los eventos streaming serán consistentes con los datos batch.
    """
    pacientes_path    = RAW_DIR / "pacientes.csv"
    dispositivos_path = RAW_DIR / "dispositivos.csv"
    alertas_path      = RAW_DIR / "alertas_clinicas.csv"

    for p in [pacientes_path, dispositivos_path, alertas_path]:
        if not p.exists():
            raise FileNotFoundError(
                f"No se encontró {p}. "
                "Primero ejecuta src/01_generate_batch_data.py"
            )

    pacientes_df    = pd.read_csv(pacientes_path)
    dispositivos_df = pd.read_csv(dispositivos_path)
    alertas_df      = pd.read_csv(alertas_path)

    # Solo pacientes con wearable activo
    pids_con_wear = dispositivos_df[
        dispositivos_df["estado_dispositivo"] == "activo"
    ]["id_paciente"].tolist()

    riesgo_map     = dict(zip(pacientes_df["id_paciente"], pacientes_df["nivel_riesgo"]))
    diagnostico_map= dict(zip(pacientes_df["id_paciente"], pacientes_df["diagnostico_principal"]))
    especialidad_map=dict(zip(pacientes_df["id_paciente"], pacientes_df["especialidad"]))
    wearable_map   = dict(zip(pacientes_df["id_paciente"], pacientes_df["modelo_wearable"]))
    distrito_map   = dict(zip(pacientes_df["id_paciente"], pacientes_df["distrito"]))

    return {
        "pids":             pids_con_wear,
        "riesgo_map":       riesgo_map,
        "diagnostico_map":  diagnostico_map,
        "especialidad_map": especialidad_map,
        "wearable_map":     wearable_map,
        "distrito_map":     distrito_map,
    }


# ============================================================
# 3. Callback de confirmación Kafka
# ============================================================

def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"  Error enviando evento: {err}")


# ============================================================
# 4. Crear un evento de signos vitales
# ============================================================

def create_event(event_number: int, ref: dict) -> dict:
    """
    Genera un evento JSON de signos vitales para un paciente con wearable.
    Los valores se ajustan según el nivel de riesgo del paciente,
    haciendo el streaming clínicamente coherente con el batch.
    """
    pid    = random.choice(ref["pids"])
    riesgo = ref["riesgo_map"].get(pid, "moderado")

    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Valores de FC y SpO2 según riesgo y tipo de evento
    if event_type in ["alerta_fc_alta", "evento_critico"]:
        fc   = random.randint(110, 160)
        spo2 = random.randint(86, 93)
        temp = round(random.uniform(37.4, 39.2), 1)
    elif event_type == "alerta_spo2_bajo":
        fc   = random.randint(90, 125)
        spo2 = random.randint(84, 93)
        temp = round(random.uniform(36.5, 37.8), 1)
    elif event_type == "alerta_temperatura":
        fc   = random.randint(85, 115)
        spo2 = random.randint(93, 97)
        temp = round(random.uniform(37.5, 39.5), 1)
    elif event_type == "lectura_anomala":
        if riesgo in ["alto","crítico"]:
            fc   = random.randint(95, 130)
            spo2 = random.randint(90, 95)
        else:
            fc   = random.randint(88, 110)
            spo2 = random.randint(93, 96)
        temp = round(random.uniform(36.5, 37.6), 1)
    else:  # lectura_normal
        fc   = random.randint(58, 95)
        spo2 = random.randint(95, 99)
        temp = round(random.uniform(36.1, 37.2), 1)

    # Risk score clínico
    risk_score = 0.0
    if fc   > 110: risk_score += 0.35
    if fc   > 130: risk_score += 0.20
    if spo2 < 94:  risk_score += 0.30
    if spo2 < 90:  risk_score += 0.20
    if temp > 37.5:risk_score += 0.15
    risk_score = round(min(risk_score + random.uniform(0, 0.10), 1.0), 2)

    es_critico = (
        risk_score >= 0.70
        or event_type == "evento_critico"
        or (fc > 130 and spo2 < 90)
    )

    event = {
        "event_id":               f"EVT-{event_number:07d}",
        "id_paciente":            pid,
        "event_type":             event_type,
        "wearable":               ref["wearable_map"].get(pid, "Desconocido"),
        "distrito":               ref["distrito_map"].get(pid, "Lima"),
        "diagnostico_principal":  ref["diagnostico_map"].get(pid, ""),
        "especialidad":           ref["especialidad_map"].get(pid, ""),
        "nivel_riesgo_historico": riesgo,
        "frecuencia_cardiaca_bpm":fc,
        "spo2_porcentaje":        spo2,
        "temperatura_corporal_c": temp,
        "nivel_estres":           random.choice(NIVELES_ESTRES),
        "pasos_ultimo_registro":  random.randint(0, 3000),
        "risk_score":             risk_score,
        "es_critico":             es_critico,
        "requiere_atencion":      es_critico or event_type in ["alerta_fc_alta","alerta_spo2_bajo","evento_critico"],
        "event_timestamp":        datetime.now().isoformat(timespec="seconds"),
    }

    return event


# ============================================================
# 5. Función principal
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Productor Kafka - IoT Médico"
    )
    parser.add_argument("--events", type=int,   default=600,  help="Cantidad de eventos a enviar")
    parser.add_argument("--delay",  type=float, default=0.1,  help="Delay entre eventos (segundos)")
    args = parser.parse_args()

    print("=" * 70)
    print("  Productor Kafka - IoT Médico")
    print("=" * 70)
    print(f"  Topic        : {KAFKA_TOPIC}")
    print(f"  Broker       : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Eventos      : {args.events}")
    print(f"  Delay        : {args.delay}s")
    print("=" * 70)

    ref = load_reference_data()
    print(f"  Pacientes con wearable activo: {len(ref['pids'])}")
    print("-" * 70)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    for n in range(1, args.events + 1):
        event = create_event(n, ref)
        msg   = json.dumps(event, ensure_ascii=False)

        producer.produce(
            topic    = KAFKA_TOPIC,
            key      = event["id_paciente"],
            value    = msg,
            callback = delivery_report,
        )
        producer.poll(0)

        if n <= 5 or n % 100 == 0:
            critico = "⚠ CRÍTICO" if event["es_critico"] else ""
            print(f"  Evento {n:04d} | {event['event_type']:<22} | "
                  f"FC={event['frecuencia_cardiaca_bpm']} SpO2={event['spo2_porcentaje']}% "
                  f"risk={event['risk_score']} {critico}")

        time.sleep(args.delay)

    producer.flush()

    print("=" * 70)
    print("  Envío de eventos finalizado correctamente")
    print("=" * 70)


if __name__ == "__main__":
    main()
