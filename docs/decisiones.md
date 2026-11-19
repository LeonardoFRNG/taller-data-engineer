# Diagnóstico de calidad y decisiones técnicas

## Modelo

- `customers.customer_id` (1 fila por cliente)
- `products.product_id` (1 fila por producto)
- `orders.order_id` (1 fila por pedido y versión vigente)
- `order_items.line_id` (1 fila por línea)
- `orders.customer_id -> customers.customer_id`
- `order_items.order_id -> orders.order_id`
- `order_items.product_id -> products.product_id`

## Anomalías y políticas

1. **Duplicados**: cliente y pedido repetidos. Se retiene una versión por clave; en pedidos vence `updated_at`, con desempate determinista por hash del contenido. Las versiones descartadas quedan en cuarentena.
2. **Cantidades fuera de rango** (`0`, `-2`): se ponen en cuarentena porque no son ventas válidas; nunca se corrigen a 1 arbitrariamente.
3. **Descuentos inválidos** (por ejemplo `1.20`): se ponen en cuarentena; deben estar en `[0, 1]`.
4. **Precios ausentes, inválidos o no positivos**: productos afectados se ponen en cuarentena, y sus líneas relacionadas quedan aisladas como referencias inválidas.
5. **Pedidos incompletos, valores de estado/canal incorrectos y fechas mal formadas**: se ponen en cuarentena para evitar asignaciones erróneas. El estado `deliverd` se corrige de forma explícita a `delivered`.
6. **Referencias huérfanas**: los pedidos sin cliente y las líneas sin pedido/producto se envían a cuarentena, con motivo explícito.

Los informes de cuarentena se guardan por causa para preservar los registros rechazados. Las filas reparadas por normalización se conservan como válidas (no quedan simultáneamente en cuarentena).

## Precisión y granularidad

`unit_price`: decimal(18,2); `discount_pct`: decimal(8,4); `net_amount`: decimal(20,4). Se agregan **líneas individuales**, no totales de pedido antes de unir dimensiones, lo que evita multiplicar importes. `monthly_sales` suma esos valores decimal sin redondeos prematuros.

## Incremental

Se carga el histórico junto al JSONL en cada ejecución. `row_number()` por `order_id` y `updated_at DESC` elige la versión más reciente (incluyendo actualización tardía). No se agregan salidas anteriores a la fuente; el resultado se sobrescribe. Los ítems usan `line_id` para conservar una única versión. No se requiere Delta Lake.

## Consideraciones

- No existe un precio de artículo persistido en `order_items`: se usa `products.unit_price`. Un catálogo con precios históricos requeriría snapshots o Slowly Changing Dimensions.
- Se asume que `shipped` y `delivered` representan ventas. El estado `pending` y `cancelled` no se contabiliza.
- Parquet local con `overwrite` no ofrece atomicidad ante corte eléctrico durante la escritura.
- En modo local la distribución física, tolerancia a fallos y rendimiento no reproducen un clúster real.
- Las entradas provienen de datos sintéticos del Excel proporcionado.
