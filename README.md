# Taller Data Engineer Junior Advanced — ETL con PySpark

Proyecto práctico de ingeniería de datos local, construido sobre las seis hojas del Excel del taller de RIWI. Python + PySpark + Spark SQL/DataFrames, sin nube ni Delta Lake.

## Funcionalidades

- Importación de CSV históricos y JSON Lines incrementales.
- Limpieza de tipos y validación de claves, integridad referencial, fechas, precios, cantidades y descuentos.
- Normalización documentada del error tipográfico `deliverd` → `delivered`.
- Cuarentena trazable de filas inválidas, huérfanas y duplicadas en `output/quarantine/`.
- Idempotencia: combina histórico + incremental y selecciona una única fila por `order_id`, priorizando el mayor `updated_at`; detalle único por `line_id`.
- Ventas netas por **mes, país, canal y categoría**, con fórmula `quantity * unit_price * (1 - discount_pct)`.
- Dos controles automáticos: reconciliación de totales y granularidad de líneas únicas. También comprueba importes negativos/nulos.
- Resultados Parquet y evidencia de QA en JSON.

## Requisitos

Python 3.12, JDK 17/21/25 según la guía y PySpark 4.3.0. Comprueba `java -version`.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python src/pipeline.py
```

En Windows: `py -3.12 -m venv .venv`, `.venv\\Scripts\\activate`, luego instalación y ejecución equivalentes.

Para comparar resultados sin lote incremental:

```bash
python src/pipeline.py --no-incremental --output-dir output/historical_only
```

## Estructura

```text
├── datos/
│   ├── customers.csv, products.csv, orders.csv, order_items.csv
│   └── incremental/orders_incremental.jsonl, order_items_incremental.jsonl
├── src/pipeline.py
├── tests/test_pipeline.py
├── docs/decisiones.md
├── requirements.txt
└── output/   # generado al ejecutar; ignorado por Git
```

El Excel original contenía todo el material de datos; se exportaron sus seis hojas a CSV/JSONL para ejecutar Spark sin el ZIP citado en la consigna, que no estaba adjunto.

## Salidas

| Ruta | Contenido |
|---|---|
| `output/sales_detail/` | Una fila por línea válida de pedido comercializable |
| `output/monthly_sales/` | Ventas netas agregadas por mes, país, canal y categoría |
| `output/orders_current/` | Última versión por pedido |
| `output/items_current/` | Detalle vigente sin duplicados |
| `output/quarantine/` | Filas rechazadas separadas por incidencia |
| `output/qa_report.json` | Conciliación, líneas, duplicados, importes inválidos y rechazos |

### Regla comercial

Se reconocen ventas para estados `delivered` y `shipped`; `pending` y `cancelled` se conservan como pedidos, pero no generan ventas. Se usa precio de producto del catálogo actual (no existe precio histórico de línea); esta limitación se documenta en `docs/decisiones.md`.

### Reprocesamiento

La entrada es inmutable y cada ejecución recalcula salidas en modo `overwrite`: reprocesar el mismo lote no suma pedidos de nuevo. Los resultados son reproducibles con el mismo Excel/CSV/JSONL. Si se actualiza la fuente, el nuevo `updated_at` prevalece por pedido.

> Nota: la escritura de Parquet es una operación local, no un `MERGE` transaccional; para producción usar staging/rename atómico o un formato transaccional.
