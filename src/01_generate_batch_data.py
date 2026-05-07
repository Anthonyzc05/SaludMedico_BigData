"""
Archivo: 01_generate_batch_data.py
Proyecto: IoT Médico - Monitoreo de Pacientes Crónicos

Objetivo:
Generar datos históricos ficticios pero realistas para un hospital
que integra datos clínicos con dispositivos wearables de pacientes crónicos.

Archivos generados:
- data/raw/pacientes.csv          (archivo principal - 10,000 registros)
- data/raw/dispositivos.csv
- data/raw/signos_vitales.json    (lectura con json_normalize)
- data/raw/medicamentos.csv
- data/raw/alertas_clinicas.csv

Comando:
docker compose exec spark python src/01_generate_batch_data.py
"""

from pathlib import Path
from datetime import datetime, timedelta
import random
import json

import numpy as np
import pandas as pd
from faker import Faker


# ============================================================
# 1. Configuración general
# ============================================================

SEED = 2026
random.seed(SEED)
np.random.seed(SEED)

fake = Faker("es_ES")
Faker.seed(SEED)

N_PACIENTES      = 10_000
N_DISPOSITIVOS   = 6_500
N_SIGNOS         = 50_000
N_MEDICAMENTOS   = 15_000
N_ALERTAS        = 8_000

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. Funciones auxiliares
# ============================================================

def random_datetime(start: datetime, end: datetime) -> datetime:
    total = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, total))


def save_csv(df: pd.DataFrame, filename: str) -> None:
    path = RAW_DIR / filename
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"  {filename}: {len(df):,} registros")


def save_json(data: list, filename: str) -> None:
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  {filename}: {len(data):,} registros")


# ============================================================
# 3. Datos de referencia
# ============================================================

DIAGNOSTICOS = [
    ("Diabetes Mellitus Tipo 2",              "E11", "Endocrinología"),
    ("Diabetes Mellitus Tipo 1",              "E10", "Endocrinología"),
    ("Hipertensión Arterial Esencial",        "I10", "Cardiología"),
    ("Insuficiencia Cardíaca Congestiva",     "I50", "Cardiología"),
    ("Fibrilación Auricular",                 "I48", "Cardiología"),
    ("Enfermedad Pulmonar Obstructiva Crónica","J44","Neumología"),
    ("Asma Bronquial",                        "J45", "Neumología"),
    ("Enfermedad Renal Crónica",              "N18", "Nefrología"),
    ("Nefropatía Diabética",                  "N08", "Nefrología"),
    ("Hipotiroidismo",                        "E03", "Endocrinología"),
    ("Dislipidemia Mixta",                    "E78", "Cardiología"),
    ("Angina Estable",                        "I20", "Cardiología"),
    ("Lupus Eritematoso Sistémico",           "M32", "Reumatología"),
]

DIAG_SEC = [
    ("Hipertensión Arterial",    "I10"),
    ("Dislipidemia",             "E78"),
    ("Anemia Crónica",           "D63"),
    ("Retinopatía Diabética",    "H36"),
    ("Neuropatía Periférica",    "G60"),
    ("Obesidad",                 "E66"),
    ("Apnea Obstructiva Sueño", "G47"),
    ("Depresión Mayor",          "F32"),
    ("Osteoporosis",             "M81"),
    ("Artritis Reumatoide",      "M05"),
]

MEDICOS = [
    "Dra. Rosa Valverde",   "Dr. Luis Paredes",
    "Dra. Ana Ramos",       "Dr. Jorge Castillo",
    "Dra. Carmen Polo",     "Dr. Roberto Huanca",
    "Dra. Patricia Vega",   "Dr. Mario Quispe",
    "Dra. Silvia Torres",   "Dr. Andrés Mamani",
]

WEARABLES = [
    ("Fitbit Sense 2",          "FB-S2"),
    ("Apple Watch Series 9",    "AW-S9"),
    ("Samsung Galaxy Watch 6",  "SG-W6"),
    ("Garmin Venu 3",           "GR-V3"),
    ("Fitbit Charge 6",         "FB-C6"),
    ("Xiaomi Mi Band 8",        "XM-B8"),
    ("Huawei Watch GT4",        "HW-G4"),
    ("Amazfit GTR 4",           "AZ-G4"),
]

DISTRITOS = [
    "San Borja","Miraflores","San Isidro","Pueblo Libre","Barranco",
    "Jesús María","Lince","Cercado de Lima","Surco","La Molina",
    "San Miguel","Magdalena","Chorrillos","Surquillo","San Luis",
    "Ate","Santa Anita","El Agustino","Rímac","Los Olivos",
]

SEGUROS  = ["EsSalud","SIS","Rimac","Pacifico","Mapfre","La Positiva"]
RIESGOS  = ["bajo","moderado","alto","crítico"]
RIESGO_W = [0.25, 0.35, 0.25, 0.15]

MEDICAMENTOS = [
    ("Metformina",          "1000 mg","Tableta","Oral"),
    ("Enalapril",           "20 mg",  "Tableta","Oral"),
    ("Amlodipino",          "5 mg",   "Tableta","Oral"),
    ("Furosemida",          "40 mg",  "Tableta","Oral"),
    ("Losartán",            "50 mg",  "Tableta","Oral"),
    ("Atorvastatina",       "40 mg",  "Tableta","Oral"),
    ("Tiotropio",           "18 mcg", "Inhalador","Inhalatoria"),
    ("Salbutamol",          "100 mcg","Inhalador","Inhalatoria"),
    ("Insulina Glargina",   "100 UI/mL","Inyectable","Subcutánea"),
    ("Insulina Aspart",     "100 UI/mL","Inyectable","Subcutánea"),
    ("Carvedilol",          "25 mg",  "Tableta","Oral"),
    ("Espironolactona",     "25 mg",  "Tableta","Oral"),
    ("Levotiroxina",        "100 mcg","Tableta","Oral"),
    ("Prednisona",          "5 mg",   "Tableta","Oral"),
    ("Bisoprolol",          "5 mg",   "Tableta","Oral"),
]

FARMACIAS = [
    "Farmacia EsSalud","Farmacia SIS","Rimac Farmacia",
    "Pacifico Farmacia","Inkafarma","Mifarma","Botica Arcángel",
]

NIVEL_ADHERENCIA = ["alta","moderada","baja","nueva"]

TIPOS_ALERTA = [
    "FC_ELEVADA",
    "SPO2_BAJO",
    "TEMPERATURA_ALTA",
    "FC_Y_SPO2_CRITICO",
    "HIPOGLUCEMIA_NOCTURNA",
    "DESCOMPENSACION_CARDIACA",
]

NIVELES_ESTRES = ["bajo","medio","alto","muy_alto"]


# ============================================================
# 4. Generar pacientes  (ARCHIVO PRINCIPAL - 10,000 registros)
# ============================================================

def generate_pacientes() -> pd.DataFrame:
    rows = []
    for i in range(1, N_PACIENTES + 1):
        sexo  = random.choice(["M","F"])
        edad  = random.randint(30, 85)
        anio  = 2025 - edad
        diag  = random.choice(DIAGNOSTICOS)
        diag_s= random.choice(DIAG_SEC)
        wear  = random.choice(WEARABLES) if random.random() < 0.65 else None
        peso  = round(random.uniform(50, 115), 1)
        talla = random.randint(148, 188)
        fname = fake.first_name_male() if sexo=="M" else fake.first_name_female()

        rows.append({
            "id_paciente":          f"PAC-{i:06d}",
            "nombre_completo":      f"{fname} {fake.last_name()} {fake.last_name()}",
            "fecha_nacimiento":     f"{anio}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "edad":                 edad,
            "sexo":                 sexo,
            "dni":                  str(random.randint(10000000, 99999999)),
            "telefono":             f"9{random.randint(10000000,99999999)}",
            "correo":               f"pac{i}@hospital.test",
            "distrito":             random.choice(DISTRITOS),
            "medico_asignado":      random.choice(MEDICOS),
            "especialidad":         diag[2],
            "seguro_salud":         random.choice(SEGUROS),
            "diagnostico_principal":diag[0],
            "codigo_cie10":         diag[1],
            "diagnostico_secundario":diag_s[0],
            "codigo_cie10_sec":     diag_s[1],
            "peso_kg":              peso,
            "talla_cm":             talla,
            "imc":                  round(peso / ((talla/100)**2), 1),
            "grupo_sanguineo":      random.choice(["A+","A-","B+","B-","AB+","AB-","O+","O-"]),
            "nivel_riesgo":         random.choices(RIESGOS, weights=RIESGO_W)[0],
            "tiene_wearable":       "SI" if wear else "NO",
            "modelo_wearable":      wear[0] if wear else "",
            "segmento_wearable":    wear[1] if wear else "",
            "fecha_registro":       fake.date_between(start_date="-5y", end_date="-1m"),
            "ultima_consulta":      fake.date_between(start_date="-3m", end_date="today"),
            "frecuencia_visitas":   random.choice(["semanal","quincenal","mensual","trimestral"]),
            "estado_paciente":      random.choices(["activo","activo","activo","seguimiento","alta_temporal"],
                                                   weights=[0.6,0.1,0.1,0.15,0.05])[0],
        })
    return pd.DataFrame(rows)


# ============================================================
# 5. Generar dispositivos wearable
# ============================================================

def generate_dispositivos(pacientes_df: pd.DataFrame) -> pd.DataFrame:
    con_wear = pacientes_df[pacientes_df["tiene_wearable"] == "SI"].copy()
    firmware_map = {
        "Fitbit Sense 2": "2.3.1", "Apple Watch Series 9": "10.1",
        "Samsung Galaxy Watch 6": "5.0.2", "Garmin Venu 3": "14.20",
        "Fitbit Charge 6": "1.8.0", "Xiaomi Mi Band 8": "3.5.7",
        "Huawei Watch GT4": "4.0.3", "Amazfit GTR 4": "2.1.9",
    }
    rows = []
    for idx, row in con_wear.iterrows():
        seg = row["segmento_wearable"]
        uid = f"{seg}-{random.randint(1000,9999):04X}"
        modelo = row["modelo_wearable"]
        rows.append({
            "id_dispositivo":  f"DISP-{idx+1:06d}",
            "id_paciente":     row["id_paciente"],
            "marca":           modelo,
            "modelo_codigo":   uid,
            "firmware":        firmware_map.get(modelo, "1.0.0"),
            "fecha_activacion":fake.date_between(start_date="-2y", end_date="-1m"),
            "estado_dispositivo": random.choices(["activo","mantenimiento","inactivo"],
                                                 weights=[0.90,0.06,0.04])[0],
            "intervalo_sync_min": random.choice([5,10,15,30]),
            "bateria_promedio_pct": random.randint(40,100),
        })
    return pd.DataFrame(rows)


# ============================================================
# 6. Generar signos vitales  (JSON - archivo enriquecido)
# ============================================================

def generate_signos_vitales(pacientes_df: pd.DataFrame) -> list:
    con_wear = pacientes_df[pacientes_df["tiene_wearable"] == "SI"]["id_paciente"].tolist()
    riesgo_map = dict(zip(pacientes_df["id_paciente"], pacientes_df["nivel_riesgo"]))
    start = datetime(2025, 1, 1)
    end   = datetime(2025, 10, 31)
    records = []
    for i in range(1, N_SIGNOS + 1):
        pid    = random.choice(con_wear)
        riesgo = riesgo_map.get(pid, "moderado")

        if riesgo == "crítico":
            fc   = random.randint(95, 145)
            spo2 = random.randint(88, 95)
        elif riesgo == "alto":
            fc   = random.randint(85, 120)
            spo2 = random.randint(92, 97)
        elif riesgo == "moderado":
            fc   = random.randint(68, 100)
            spo2 = random.randint(94, 98)
        else:
            fc   = random.randint(58, 88)
            spo2 = random.randint(96, 99)

        temp   = round(random.uniform(36.1, 37.7), 1)
        pasos  = random.randint(200, 13000)
        estres_w = {"bajo":[0.6,0.3,0.08,0.02],"moderado":[0.3,0.4,0.25,0.05],
                    "alto":[0.1,0.3,0.45,0.15],"crítico":[0.05,0.15,0.45,0.35]}
        estres = random.choices(NIVELES_ESTRES, weights=estres_w[riesgo])[0]
        sueno  = round(random.uniform(3.5, 9.2), 1) if random.random() > 0.25 else None
        alerta = fc > 105 or spo2 < 94 or temp > 37.4

        rec = {
            "id_lectura":             f"LEC-{i:07d}",
            "id_paciente":            pid,
            "dispositivo": {
                "marca":              pacientes_df.loc[pacientes_df["id_paciente"]==pid,"modelo_wearable"].values[0],
                "firmware":           "2.3.1",
            },
            "timestamp":              random_datetime(start, end).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "frecuencia_cardiaca_bpm":fc,
            "spo2_porcentaje":        spo2,
            "temperatura_corporal_c": temp,
            "pasos_diarios":          pasos,
            "calorias_quemadas":      int(pasos * random.uniform(0.28, 0.42)),
            "nivel_estres":           estres,
            "horas_sueno":            sueno,
            "alerta_generada":        alerta,
        }
        if alerta:
            msgs = []
            if fc   > 105: msgs.append(f"FC elevada {fc} bpm")
            if spo2 < 94:  msgs.append(f"SpO2 bajo {spo2}%")
            if temp > 37.4: msgs.append(f"Temperatura {temp}°C")
            rec["detalle_alerta"] = " + ".join(msgs)
        records.append(rec)
    return records


# ============================================================
# 7. Generar medicamentos / recetas
# ============================================================

def generate_medicamentos(pacientes_df: pd.DataFrame) -> pd.DataFrame:
    pids = pacientes_df["id_paciente"].tolist()
    start = datetime(2025, 1, 1)
    end   = datetime(2025, 10, 31)
    rows  = []
    for i in range(1, N_MEDICAMENTOS + 1):
        med = random.choice(MEDICAMENTOS)
        pid = random.choice(pids)
        dt  = random_datetime(start, end)
        dur_dias = random.choice([30,60,90,180])
        cant = random.randint(20, 180)
        cu   = round(random.uniform(0.25, 200.0), 2)
        rows.append({
            "id_receta":           f"REC-{i:06d}",
            "id_paciente":         pid,
            "nombre_generico":     med[0],
            "concentracion":       med[1],
            "forma_farmaceutica":  med[2],
            "via_administracion":  med[3],
            "frecuencia":          random.choice(["1 vez/día","2 veces/día","3 veces/día","cada 12h"]),
            "duracion_dias":       dur_dias,
            "fecha_prescripcion":  dt.strftime("%Y-%m-%d"),
            "fecha_vencimiento":   (dt + timedelta(days=dur_dias)).strftime("%Y-%m-%d"),
            "estado_receta":       random.choices(["activa","activa","activa","vencida","suspendida"],
                                                  weights=[0.6,0.1,0.1,0.15,0.05])[0],
            "nivel_adherencia":    random.choice(NIVEL_ADHERENCIA),
            "farmacia":            random.choice(FARMACIAS),
            "cantidad_unidades":   cant,
            "costo_unitario":      cu,
            "costo_total":         round(cu * cant, 2),
            "cubierto_seguro":     random.choice(["SI","SI","SI","NO"]),
        })
    return pd.DataFrame(rows)


# ============================================================
# 8. Generar alertas clínicas
# ============================================================

def generate_alertas(pacientes_df: pd.DataFrame) -> pd.DataFrame:
    pids_riesgo = pacientes_df[
        pacientes_df["nivel_riesgo"].isin(["alto","crítico"])
    ]["id_paciente"].tolist()
    todos = pacientes_df["id_paciente"].tolist()
    start = datetime(2025, 1, 1)
    end   = datetime(2025, 10, 31)
    rows  = []
    for i in range(1, N_ALERTAS + 1):
        pid  = random.choice(pids_riesgo) if random.random() < 0.75 else random.choice(todos)
        tipo = random.choice(TIPOS_ALERTA)
        dt   = random_datetime(start, end)
        rows.append({
            "id_alerta":          f"ALT-{i:06d}",
            "id_paciente":        pid,
            "tipo_alerta":        tipo,
            "fecha_alerta":       dt.strftime("%Y-%m-%d"),
            "hora_alerta":        dt.strftime("%H:%M:%S"),
            "valor_fc":           random.randint(90, 160) if "FC" in tipo else None,
            "valor_spo2":         random.randint(85, 93)  if "SPO2" in tipo or "CRITICO" in tipo else None,
            "valor_temperatura":  round(random.uniform(37.4, 39.0), 1) if "TEMPERATURA" in tipo else None,
            "nivel_criticidad":   random.choices(["media","alta","critica"],weights=[0.4,0.35,0.25])[0],
            "estado_alerta":      random.choices(["resuelta","pendiente","en_seguimiento"],
                                                 weights=[0.55,0.25,0.20])[0],
            "medico_notificado":  random.choice(MEDICOS),
            "canal_notificacion": random.choice(["app_movil","sms","llamada","email"]),
            "tiempo_respuesta_min":random.randint(1, 120),
        })
    return pd.DataFrame(rows)


# ============================================================
# 9. Función principal
# ============================================================

def main() -> None:
    print("=" * 70)
    print("  Generando datos históricos - IoT Médico")
    print("=" * 70)
    print(f"  Carpeta de salida: {RAW_DIR}")
    print("-" * 70)

    pacientes_df = generate_pacientes()
    save_csv(pacientes_df, "pacientes.csv")

    dispositivos_df = generate_dispositivos(pacientes_df)
    save_csv(dispositivos_df, "dispositivos.csv")

    signos = generate_signos_vitales(pacientes_df)
    save_json(signos, "signos_vitales.json")

    medicamentos_df = generate_medicamentos(pacientes_df)
    save_csv(medicamentos_df, "medicamentos.csv")

    alertas_df = generate_alertas(pacientes_df)
    save_csv(alertas_df, "alertas_clinicas.csv")

    print("-" * 70)
    print("  Proceso finalizado correctamente.")
    print("  Archivos generados en data/raw/")
    print("-" * 70)

    print("\n  Vista previa de pacientes.csv:")
    print(pacientes_df[["id_paciente","nombre_completo","edad","diagnostico_principal",
                         "nivel_riesgo","tiene_wearable"]].head(5).to_string(index=False))

    print("\n  Distribución por nivel de riesgo:")
    print(pacientes_df["nivel_riesgo"].value_counts().to_string())

    print("\n  Pacientes con wearable:")
    print(pacientes_df["tiene_wearable"].value_counts().to_string())


# ============================================================
# 10. Cargar a MongoDB
# ============================================================

def cargar_a_mongodb() -> None:
    """
    Después de generar los archivos, los carga directamente a MongoDB.
    Colecciones creadas: pacientes, dispositivos, medicamentos, alertas_clinicas
    El JSON de signos_vitales se carga desde archivo porque es grande.
    """
    try:
        from pymongo import MongoClient
        import json

        print("\n" + "=" * 70)
        print("  Cargando datos generados a MongoDB ...")

        # host.docker.internal conecta al MongoDB de tu PC desde Docker
        for host in ["host.docker.internal", "localhost"]:
            try:
                client = MongoClient(f"mongodb://{host}:27017/", serverSelectionTimeoutMS=2000)
                client.admin.command("ping")
                print(f"  Conectado a mongodb://{host}:27017/")
                break
            except Exception:
                continue
        else:
            print("  No se pudo conectar a MongoDB.")
            print("  Asegurate de que MongoDB este corriendo en tu PC.")
            return

        db = client["iot_medico"]

        # Cargar cada CSV generado
        archivos = [
            ("pacientes",       RAW_DIR / "pacientes.csv"),
            ("dispositivos",    RAW_DIR / "dispositivos.csv"),
            ("medicamentos",    RAW_DIR / "medicamentos.csv"),
            ("alertas_clinicas",RAW_DIR / "alertas_clinicas.csv"),
        ]

        for nombre_col, path in archivos:
            if path.exists():
                import pandas as pd
                df = pd.read_csv(path, encoding="utf-8")
                df = df.where(pd.notnull(df), None)
                registros = df.to_dict("records")
                db[nombre_col].drop()
                db[nombre_col].insert_many(registros)
                print(f"  [OK] {nombre_col}: {len(registros):,} documentos")

        # Cargar JSON de signos vitales
        json_path = RAW_DIR / "signos_vitales.json"
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                signos = json.load(f)
            db["signos_vitales"].drop()
            db["signos_vitales"].insert_many(signos)
            print(f"  [OK] signos_vitales: {len(signos):,} documentos")

        print("  MongoDB cargado. Abre Compass → mongodb://localhost:27017 → iot_medico")
        client.close()

    except ImportError:
        print("  pymongo no instalado. Instala con: pip install pymongo")
    except Exception as e:
        print(f"  Error al cargar MongoDB: {e}")


if __name__ == "__main__":
    main()
    cargar_a_mongodb()