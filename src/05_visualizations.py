"""
Archivo: 05_visualizations.py
Proyecto: IoT Médico - Monitoreo de Pacientes Crónicos

Objetivo:
Generar gráficos a partir de los KPIs creados por 03_batch_etl_spark.py

Gráficos generados:
- chart1_alertas_por_riesgo.png
- chart2_fc_por_diagnostico.png
- chart3_alertas_por_distrito.png
- chart4_medicamentos_top.png
- chart5_tipos_alerta.png

Comando:
docker compose exec spark python src/05_visualizations.py
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")   # modo sin pantalla (necesario en Docker)


# ============================================================
# 1. Rutas
# ============================================================

BASE_DIR   = Path(__file__).resolve().parents[1]
KPI_DIR    = BASE_DIR / "output" / "kpis"
CHARTS_DIR = BASE_DIR / "output" / "charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. Auxiliares
# ============================================================

def read_kpi(filename: str) -> pd.DataFrame:
    path = KPI_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró {path}. "
            "Ejecuta primero 03_batch_etl_spark.py"
        )
    return pd.read_csv(path)


def save(filename: str) -> None:
    out = CHARTS_DIR / filename
    plt.savefig(out, bbox_inches="tight", dpi=140)
    plt.close()
    print(f"  Gráfico creado: output/charts/{filename}")


# ============================================================
# 3. Gráfico 1: Alertas por nivel de riesgo
# ============================================================

def chart_alertas_por_riesgo() -> None:
    """
    ¿En qué nivel de riesgo se concentran más las alertas?
    Barras agrupadas: total lecturas vs total alertas.
    """
    df = read_kpi("kpi1_alertas_por_riesgo.csv")
    df = df.sort_values("total_alertas", ascending=False)

    x      = range(len(df))
    width  = 0.35
    fig, ax = plt.subplots(figsize=(9, 5))

    bars1 = ax.bar([i - width/2 for i in x], df["total_lecturas"], width,
                   label="Total lecturas", color="#4A90D9")
    bars2 = ax.bar([i + width/2 for i in x], df["total_alertas"],  width,
                   label="Total alertas",  color="#E05A4A")

    ax.set_title("Lecturas y Alertas por Nivel de Riesgo del Paciente")
    ax.set_xlabel("Nivel de Riesgo")
    ax.set_ylabel("Cantidad")
    ax.set_xticks(list(x))
    ax.set_xticklabels(df["nivel_riesgo"])
    ax.legend()

    for bar in bars2:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h,
                str(int(h)), ha="center", va="bottom", fontsize=8)

    save("chart1_alertas_por_riesgo.png")


# ============================================================
# 4. Gráfico 2: FC y SpO2 promedio por diagnóstico
# ============================================================

def chart_signos_por_diagnostico() -> None:
    """
    ¿Qué diagnósticos tienen peores signos vitales en promedio?
    Doble eje: FC (barras) y SpO2 (línea).
    """
    df = read_kpi("kpi2_signos_por_diagnostico.csv")
    df = df.sort_values("fc_promedio", ascending=False).head(10)

    # Etiquetas más cortas
    df["diag_corto"] = df["diagnostico_principal"].apply(
        lambda x: x[:22] + "…" if len(x) > 22 else x
    )

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()

    bars = ax1.bar(df["diag_corto"], df["fc_promedio"], color="#4A90D9", alpha=0.8)
    ax2.plot(df["diag_corto"], df["spo2_promedio"], color="#E05A4A",
             marker="o", linewidth=2, label="SpO2 promedio (%)")

    ax1.set_title("FC y SpO2 Promedio por Diagnóstico Principal (Top 10)")
    ax1.set_xlabel("Diagnóstico")
    ax1.set_ylabel("FC Promedio (bpm)", color="#4A90D9")
    ax2.set_ylabel("SpO2 Promedio (%)", color="#E05A4A")
    ax2.set_ylim(90, 100)
    plt.xticks(rotation=40, ha="right")

    lines, labels = ax2.get_legend_handles_labels()
    ax2.legend(lines, labels, loc="lower right")

    save("chart2_fc_spo2_por_diagnostico.png")


# ============================================================
# 5. Gráfico 3: Alertas por distrito
# ============================================================

def chart_alertas_por_distrito() -> None:
    """
    ¿En qué distritos de Lima se generan más alertas clínicas?
    """
    df = read_kpi("kpi3_alertas_por_distrito.csv")
    df = df.sort_values("total_alertas", ascending=False).head(12)

    fig, ax = plt.subplots(figsize=(11, 5))
    bars = ax.bar(df["distrito"], df["total_alertas"], color="#E05A4A")

    ax.set_title("Top Distritos con Mayor Cantidad de Alertas Clínicas")
    ax.set_xlabel("Distrito")
    ax.set_ylabel("Total Alertas")
    plt.xticks(rotation=40, ha="right")

    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h,
                str(int(h)), ha="center", va="bottom", fontsize=8)

    save("chart3_alertas_por_distrito.png")


# ============================================================
# 6. Gráfico 4: Top medicamentos recetados
# ============================================================

def chart_medicamentos() -> None:
    """
    ¿Cuáles son los medicamentos más recetados y cuál es su costo promedio?
    """
    df = read_kpi("kpi4_medicamentos.csv")
    df = df.sort_values("total_recetas", ascending=False).head(10)

    fig, ax = plt.subplots(figsize=(11, 5))
    bars = ax.barh(df["nombre_generico"], df["total_recetas"], color="#4A90D9")

    ax.set_title("Top 10 Medicamentos más Recetados")
    ax.set_xlabel("Total Recetas")
    ax.set_ylabel("Medicamento")
    ax.invert_yaxis()

    for bar in bars:
        w = bar.get_width()
        ax.text(w + 5, bar.get_y() + bar.get_height()/2,
                str(int(w)), va="center", fontsize=8)

    save("chart4_medicamentos_top.png")


# ============================================================
# 7. Gráfico 5: Tipos de alerta clínica por criticidad
# ============================================================

def chart_tipos_alerta() -> None:
    """
    ¿Qué tipos de alerta se generan más y con qué nivel de criticidad?
    Barras apiladas por nivel de criticidad.
    """
    df = read_kpi("kpi5_tipos_alerta.csv")

    pivot = df.pivot_table(
        index="tipo_alerta",
        columns="nivel_criticidad",
        values="total",
        aggfunc="sum",
        fill_value=0
    )

    colores = {"media": "#F4C145", "alta": "#E07B39", "critica": "#C0392B"}
    fig, ax = plt.subplots(figsize=(11, 6))

    bottom = pd.Series([0] * len(pivot), index=pivot.index)
    for nivel in ["media", "alta", "critica"]:
        if nivel in pivot.columns:
            ax.bar(pivot.index, pivot[nivel], bottom=bottom,
                   label=nivel.capitalize(), color=colores[nivel])
            bottom = bottom + pivot[nivel]

    ax.set_title("Tipos de Alerta Clínica por Nivel de Criticidad")
    ax.set_xlabel("Tipo de Alerta")
    ax.set_ylabel("Total Alertas")
    ax.legend(title="Criticidad")
    plt.xticks(rotation=30, ha="right")

    save("chart5_tipos_alerta.png")


# ============================================================
# 8. Función principal
# ============================================================

def main() -> None:
    print("=" * 70)
    print("  Generando visualizaciones - IoT Médico")
    print("=" * 70)

    chart_alertas_por_riesgo()
    chart_signos_por_diagnostico()
    chart_alertas_por_distrito()
    chart_medicamentos()
    chart_tipos_alerta()

    print("=" * 70)
    print("  5 gráficos generados correctamente")
    print("  Carpeta: output/charts/")
    print("=" * 70)


if __name__ == "__main__":
    main()
