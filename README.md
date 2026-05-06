# Rapídex: Plataforma Big Data Para Monitoreo De Pedidos Delivery En Tiempo Real

## 1. Descripción del caso

Rapídex es una empresa ficticia de delivery que opera en varios distritos de Lima.
La empresa necesita analizar pedidos históricos y eventos en tiempo real para detectar demanda, retrasos, cancelaciones y pedidos en riesgo.

## 2. Objetivo general

Construir una solución Big Data usando Apache Spark, Python y Kafka para procesar datos batch y streaming, generar indicadores operativos y preparar una estructura adaptable a MongoDB.

## 3. Tecnologías usadas

- Docker
- Python
- Apache Spark / PySpark
- Spark SQL
- RDD
- DataFrames
- Kafka
- CSV
- JSON
- Parquet
- Matplotlib
- MongoDB como extensión opcional

## 4. Resultados esperados

- Pedidos por distrito
- Tiempo promedio de entrega
- Pedidos retrasados
- Cancelaciones por distrito
- Ranking de repartidores
- Alertas en tiempo real
- Reportes CSV
- Archivos Parquet
- Gráficos PNG

## 5. Estructura del proyecto

```text
rapidex-bigdata-streaming/
├── data/
│   ├── raw/
│   ├── processed/
│   └── checkpoints/
├── docs/
├── notebooks/
├── output/
│   ├── charts/
│   ├── kpis/
│   └── streaming/
├── src/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md