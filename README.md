# Taller Data Engineer - PySpark

Este es mi ejercicio de procesamiento de ventas de una tienda usando **Python y PySpark**. Trabajé con los datos del Excel del taller, que pasé a CSV y JSONL para poder leerlos con Spark.

## Qué hace el programa

1. Lee clientes, productos, pedidos y detalles.
2. Agrega el lote incremental de pedidos.
3. Cambia los tipos de datos y revisa errores como cantidades negativas, precios inválidos y registros que no tienen una referencia válida.
4. Separa esos registros en `output/errores/` para revisarlos.
5. Une las tablas y calcula `quantity * unit_price * (1 - discount_pct)`.
6. Agrupa las ventas por mes, país, canal y categoría.
7. Comprueba que el total del detalle sea igual al total del resumen y que no haya líneas duplicadas.
8. Guarda los resultados en formato Parquet dentro de `output/`.

## Cómo ejecutarlo

Necesitas Python 3.12, Java y PySpark. Desde la carpeta del proyecto:

```bash
python -m venv .venv
```

En Windows, activa con `.venv\Scripts\activate`. En Linux, con `source .venv/bin/activate`.

```bash
pip install -r requirements.txt
python main.py
```

## Datos

La carpeta `datos/` tiene los cuatro CSV históricos y los dos JSONL incrementales. No necesitas convertir el Excel otra vez.

## Cómo traté los errores

- Si hay un pedido repetido, conservo el que tenga `updated_at` más reciente.
- Si una cantidad es negativa, un descuento sale del rango 0 a 1, o un precio no es válido, separo el registro; no le invento un valor.
- Si un pedido no tiene cliente, o un detalle no tiene pedido o producto, también lo separo.
- Corregí `deliverd` por `delivered` porque era un error de escritura identificable.
- Consideré ventas los pedidos `shipped` y `delivered`; los pendientes y cancelados no se suman.

La salida se sobrescribe en cada ejecución. Por eso procesar otra vez el mismo lote incremental no aumenta los pedidos artificialmente. Como el detalle de pedidos no tiene el precio de cada venta, tomé el precio de la tabla de productos.

**Nota:** Este es un proyecto local para practicar, no un pipeline de producción.
# taller-data-engineer
