"""Taller de PySpark: limpieza de pedidos y resumen de ventas."""

from pathlib import Path
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


BASE = Path(__file__).resolve().parent
DATOS = BASE / "datos"
SALIDA = BASE / "output"

spark = (
    SparkSession.builder
    .appName("Taller Data Engineer")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def leer_csv(nombre):
    return spark.read.option("header", True).csv(str(DATOS / nombre))


def guardar_error(df, nombre):
    """Guardo las filas que no puedo usar para revisar luego qué pasó."""
    cantidad = df.count()
    if cantidad > 0:
        df.write.mode("overwrite").json(str(SALIDA / "errores" / nombre))
    print(f"{nombre}: {cantidad} registros apartados")


def separar_validos(df, condicion, nombre):
    validos = df.filter(F.coalesce(condicion, F.lit(False)))
    errores = df.filter(~F.coalesce(condicion, F.lit(False)))
    guardar_error(errores, nombre)
    return validos


def quitar_duplicados(df, columna, nombre, fecha=None):
    # Para los pedidos me quedo con el último updated_at.
    # Para las otras tablas dejo un registro por identificador.
    if fecha:
        ventana = Window.partitionBy(columna).orderBy(F.col(fecha).desc_nulls_last())
    else:
        ventana = Window.partitionBy(columna).orderBy(columna)
    numerados = df.withColumn("numero", F.row_number().over(ventana))
    guardar_error(numerados.filter("numero > 1").drop("numero"), nombre)
    return numerados.filter("numero = 1").drop("numero")


try:
    # 1. Leer los archivos del taller
    clientes = leer_csv("customers.csv")
    productos = leer_csv("products.csv")
    pedidos = leer_csv("orders.csv")
    detalles = leer_csv("order_items.csv")

    print("Pedidos históricos:", pedidos.count())
    print("Líneas históricas:", detalles.count())

    # 2. Agregar los pedidos nuevos y una actualización tardía
    nuevos_pedidos = spark.read.json(str(DATOS / "incremental" / "orders_incremental.jsonl"))
    nuevos_detalles = spark.read.json(str(DATOS / "incremental" / "order_items_incremental.jsonl"))
    pedidos = pedidos.unionByName(nuevos_pedidos.select(*pedidos.columns))
    detalles = detalles.unionByName(nuevos_detalles.select(*detalles.columns))

    # 3. Cambiar los tipos de datos y corregir un error de escritura conocido
    clientes = clientes.withColumn("signup_date", F.to_date("signup_date"))
    productos = productos.withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
    pedidos = (
        pedidos.withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("status", F.when(F.col("status") == "deliverd", "delivered")
                    .otherwise(F.col("status")))
    )
    detalles = (
        detalles.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("discount_pct", F.col("discount_pct").cast("decimal(8,4)"))
    )

    # 4. Separar registros con problemas
    clientes = separar_validos(
        clientes,
        F.col("customer_id").isNotNull() & F.col("country").isNotNull()
        & F.col("signup_date").isNotNull(),
        "clientes_invalidos",
    )
    clientes = quitar_duplicados(clientes, "customer_id", "clientes_duplicados")

    productos = separar_validos(
        productos,
        F.col("product_id").isNotNull() & F.col("category").isNotNull()
        & (F.col("unit_price") > 0),
        "productos_invalidos",
    )
    productos = quitar_duplicados(productos, "product_id", "productos_duplicados")

    pedidos = separar_validos(
        pedidos,
        F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
        & F.col("order_ts").isNotNull() & F.col("updated_at").isNotNull()
        & F.col("status").isin("delivered", "shipped", "pending", "cancelled")
        & F.col("channel").isin("web", "mobile", "store"),
        "pedidos_invalidos",
    )
    # Validar que cada pedido pertenezca a un cliente que existe
    sin_cliente = pedidos.join(clientes.select("customer_id"), "customer_id", "left_anti")
    guardar_error(sin_cliente, "pedidos_sin_cliente")
    pedidos = pedidos.join(clientes.select("customer_id"), "customer_id", "left_semi")
    pedidos = quitar_duplicados(pedidos, "order_id", "pedidos_duplicados", "updated_at")

    detalles = separar_validos(
        detalles,
        F.col("line_id").isNotNull() & F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull() & (F.col("quantity") > 0)
        & F.col("discount_pct").between(0, 1),
        "detalles_invalidos",
    )
    sin_pedido = detalles.join(pedidos.select("order_id"), "order_id", "left_anti")
    guardar_error(sin_pedido, "detalles_sin_pedido")
    detalles = detalles.join(pedidos.select("order_id"), "order_id", "left_semi")
    sin_producto = detalles.join(productos.select("product_id"), "product_id", "left_anti")
    guardar_error(sin_producto, "detalles_sin_producto")
    detalles = detalles.join(productos.select("product_id"), "product_id", "left_semi")
    detalles = quitar_duplicados(detalles, "line_id", "detalles_duplicados")

    # 5. Unir todo. Una fila de ventas representa UNA línea del pedido.
    # Solo cuento los pedidos enviados o entregados como ventas.
    ventas = (
        detalles.join(
            pedidos.filter(F.col("status").isin("delivered", "shipped"))
            .select("order_id", "customer_id", "order_ts", "channel"), "order_id"
        )
        .join(clientes.select("customer_id", "country"), "customer_id")
        .join(productos.select("product_id", "category", "unit_price"), "product_id")
        .withColumn("mes", F.date_format("order_ts", "yyyy-MM"))
        .withColumn(
            "venta_neta",
            (F.col("quantity") * F.col("unit_price")
             * (1 - F.col("discount_pct"))).cast("decimal(20,4)"),
        )
    )

    # 6. Calcular el resumen mensual
    resumen = (
        ventas.groupBy("mes", "country", "channel", "category")
        .agg(F.sum("venta_neta").alias("total_ventas"))
        .orderBy("mes", "country", "channel", "category")
    )

    # 7. Dos comprobaciones de calidad
    total_detalle = ventas.agg(F.sum("venta_neta")).first()[0] or 0
    total_resumen = resumen.agg(F.sum("total_ventas")).first()[0] or 0
    repetidos = ventas.groupBy("line_id").count().filter("count > 1").count()
    importes_malos = ventas.filter(F.col("venta_neta").isNull() | (F.col("venta_neta") < 0)).count()

    print("Total del detalle:", total_detalle)
    print("Total del resumen:", total_resumen)
    print("Líneas duplicadas:", repetidos)

    assert total_detalle == total_resumen, "Los totales no coinciden"
    assert repetidos == 0, "Se duplicaron líneas en los joins"
    assert importes_malos == 0, "Hay importes inválidos"

    # 8. Guardar resultados. Al sobrescribir, ejecutar dos veces no duplica la salida.
    ventas.write.mode("overwrite").parquet(str(SALIDA / "detalle_ventas"))
    resumen.write.mode("overwrite").parquet(str(SALIDA / "resumen_mensual"))
    pedidos.write.mode("overwrite").parquet(str(SALIDA / "pedidos_actualizados"))

    reporte = {
        "total_detalle": str(total_detalle),
        "total_resumen": str(total_resumen),
        "lineas_duplicadas": repetidos,
        "importes_invalidos": importes_malos,
        "controles_ok": True,
    }
    SALIDA.mkdir(parents=True, exist_ok=True)
    (SALIDA / "control_calidad.json").write_text(
        json.dumps(reporte, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    resumen.show(10, truncate=False)
    print("Listo. Revisa la carpeta output")
finally:
    spark.stop()
