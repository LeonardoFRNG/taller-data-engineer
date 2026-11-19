"""ETL local de ventas con PySpark: datos, calidad, agregación e incremental idempotente."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = {
    'customers': ['customer_id','first_name','last_name','email','country','signup_date','segment'],
    'products': ['product_id','product_name','category','unit_price','active'],
    'orders': ['order_id','customer_id','order_ts','status','channel','updated_at'],
    'order_items': ['line_id','order_id','product_id','quantity','discount_pct'],
}
KEYS = {'customers':'customer_id','products':'product_id','orders':'order_id','order_items':'line_id'}


def spark_session() -> SparkSession:
    return (SparkSession.builder.master('local[*]').appName('taller-data-engineer')
            .config('spark.sql.session.timeZone','UTC')
            .config('spark.sql.shuffle.partitions','8')
            .getOrCreate())


def read_inputs(spark: SparkSession, folder: Path) -> dict[str,DataFrame]:
    raw = {}
    for name, cols in SCHEMAS.items():
        path=folder / f'{name}.csv'
        if not path.exists(): raise FileNotFoundError(f'Falta {path}')
        frame=spark.read.option('header',True).option('inferSchema',False).csv(str(path))
        if frame.columns != cols: raise ValueError(f'Esquema inesperado en {path}: {frame.columns}')
        raw[name]=frame
    return raw


def read_incremental(spark: SparkSession, folder:Path) -> dict[str,DataFrame]:
    result={}
    for name, filename in [('orders','orders_incremental.jsonl'),('order_items','order_items_incremental.jsonl')]:
        path=folder/'incremental'/filename
        if not path.exists(): raise FileNotFoundError(f'Falta {path}')
        df=spark.read.json(str(path))
        result[name]=df.select(*SCHEMAS[name])
    return result


def deduplicate(df: DataFrame, key: str, updated: bool=False)->tuple[DataFrame,DataFrame]:
    # Un mismo ID conserva la mayor versión temporal. Ante empate, el contenido desempata.
    order=[F.col('updated_at').desc_nulls_last()] if updated else []
    order += [F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)])),256).desc()]
    w=Window.partitionBy(key).orderBy(*order)
    ranked=df.withColumn('_rank',F.row_number().over(w))
    return ranked.filter('_rank=1').drop('_rank'),ranked.filter('_rank>1').drop('_rank')


def tag(df:DataFrame, reason:str, origin:str)->DataFrame:
    return df.withColumn('_reason',F.lit(reason)).withColumn('_source',F.lit(origin))


def partition(df:DataFrame, expression, reason:str, origin:str):
    return df.filter(expression), tag(df.filter(~F.coalesce(expression,F.lit(False))),reason,origin)


def validate_dimensions(raw:dict[str,DataFrame]):
    rejected=[]
    customers=raw['customers'].withColumn('signup_date',F.to_date('signup_date'))
    customers,bad=partition(customers,F.col('customer_id').isNotNull() & F.col('country').isNotNull() & F.col('signup_date').isNotNull(),'customer_invalid','customers'); rejected.append(bad)
    customers,dup=deduplicate(customers,'customer_id');rejected.append(tag(dup,'duplicate_key','customers'))
    products=raw['products'].withColumn('unit_price',F.col('unit_price').cast(T.DecimalType(18,2)))
    products,bad=partition(products,F.col('product_id').isNotNull() & F.col('unit_price').isNotNull() & (F.col('unit_price')>0) & F.col('category').isNotNull(),'product_invalid','products');rejected.append(bad)
    products,dup=deduplicate(products,'product_id');rejected.append(tag(dup,'duplicate_key','products'))
    return customers,products,rejected


def validate_orders(raw:DataFrame, customers:DataFrame):
    df=(raw.withColumn('order_ts',F.to_timestamp('order_ts'))
         .withColumn('updated_at',F.to_timestamp('updated_at'))
         .withColumn('status',F.when(F.col('status')=='deliverd','delivered').otherwise(F.col('status'))))
    cond=(F.col('order_id').isNotNull() & F.col('customer_id').isNotNull()
          & F.col('order_ts').isNotNull() & F.col('updated_at').isNotNull()
          & F.col('status').isin('delivered','shipped','pending','cancelled')
          & F.col('channel').isin('web','mobile','store'))
    good,bad=partition(df,cond,'order_invalid','orders')
    joined=good.join(customers.select('customer_id'),on='customer_id',how='left_anti')
    good=good.join(customers.select('customer_id'),on='customer_id',how='left_semi')
    good,duplicates=deduplicate(good,'order_id',updated=True)
    return good,[bad,tag(joined,'orphan_customer','orders'),tag(duplicates,'duplicate_key','orders')]


def validate_items(raw:DataFrame, orders:DataFrame, products:DataFrame):
    df=(raw.withColumn('quantity',F.col('quantity').cast('int'))
         .withColumn('discount_pct',F.col('discount_pct').cast(T.DecimalType(8,4))))
    cond=(F.col('line_id').isNotNull() & F.col('order_id').isNotNull()
          & F.col('product_id').isNotNull() & (F.col('quantity')>0)
          & F.col('discount_pct').between(0,1))
    good,bad=partition(df,cond,'item_invalid','order_items')
    orphan_orders=good.join(orders.select('order_id'),on='order_id',how='left_anti')
    good=good.join(orders.select('order_id'),on='order_id',how='left_semi')
    orphan_products=good.join(products.select('product_id'),on='product_id',how='left_anti')
    good=good.join(products.select('product_id'),on='product_id',how='left_semi')
    good,duplicates=deduplicate(good,'line_id')
    return good,[bad,tag(orphan_orders,'orphan_order','order_items'),tag(orphan_products,'orphan_product','order_items'),tag(duplicates,'duplicate_key','order_items')]


def build_sales(orders:DataFrame,items:DataFrame,customers:DataFrame,products:DataFrame)->DataFrame:
    # Solo pedidos entregados/enviados constituyen ventas reconocidas en este ejercicio.
    accepted=orders.filter(F.col('status').isin('delivered','shipped'))
    return (items.join(accepted.select('order_id','customer_id','order_ts','status','channel'),on='order_id',how='inner')
        .join(customers.select('customer_id','country'),on='customer_id',how='inner')
        .join(products.select('product_id','category','unit_price'),on='product_id',how='inner')
        .withColumn('net_amount',F.expr('cast(quantity * unit_price * (1 - discount_pct) as decimal(20,4))'))
        .withColumn('month',F.date_format('order_ts','yyyy-MM'))
        .select('line_id','order_id','customer_id','product_id','order_ts','month','country','channel','category','quantity','unit_price','discount_pct','net_amount'))


def monthly_summary(sales:DataFrame)->DataFrame:
    return (sales.groupBy('month','country','channel','category')
            .agg(F.sum('net_amount').cast('decimal(24,4)').alias('net_sales'),F.count('*').alias('line_count'))
            .orderBy('month','country','channel','category'))


def reconciliation(sales:DataFrame,summary:DataFrame)->dict:
    detail=sales.agg(F.sum('net_amount').alias('total'),F.count('*').alias('lines')).first()
    aggregate=summary.agg(F.sum('net_sales').alias('total'),F.sum('line_count').alias('lines')).first()
    a,b=detail.total or 0,aggregate.total or 0
    if a!=b or detail.lines!=aggregate.lines:
        raise AssertionError(f'No reconcilia: detalle={a}/{detail.lines}, agregado={b}/{aggregate.lines}')
    # Dos controles: integridad de granularidad y reconciliación monetaria.
    duplicates=sales.groupBy('line_id').count().filter(F.col('count')>1).count()
    if duplicates: raise AssertionError(f'Líneas duplicadas: {duplicates}')
    invalid=sales.filter(F.col('net_amount').isNull() | (F.col('net_amount')<0)).count()
    if invalid: raise AssertionError(f'Importes inválidos: {invalid}')
    return {'detail_net':str(a),'monthly_net':str(b),'detail_lines':detail.lines,
            'monthly_lines':aggregate.lines,'duplicate_line_ids':duplicates,'invalid_net_amounts':invalid,
            'reconciliation_ok':True}


def main(argv=None):
    parser=argparse.ArgumentParser(description='ETL Spark local de ventas con calidad e incremental')
    parser.add_argument('--data-dir',type=Path,default=ROOT/'datos')
    parser.add_argument('--output-dir',type=Path,default=ROOT/'output')
    parser.add_argument('--no-incremental',action='store_true')
    args=parser.parse_args(argv)
    spark=spark_session();spark.sparkContext.setLogLevel('ERROR')
    try:
        raw=read_inputs(spark,args.data_dir)
        customers,products,dim_rejects=validate_dimensions(raw)
        # Control incremental: sustituir por la versión con updated_at más reciente.
        if not args.no_incremental:
            inc=read_incremental(spark,args.data_dir)
            orders_raw=raw['orders'].unionByName(inc['orders'])
            items_raw=raw['order_items'].unionByName(inc['order_items'])
        else:
            orders_raw=raw['orders'];items_raw=raw['order_items']
        orders,order_rejects=validate_orders(orders_raw,customers)
        items,item_rejects=validate_items(items_raw,orders,products)
        sales=build_sales(orders,items,customers,products)
        summary=monthly_summary(sales)
        checks=reconciliation(sales,summary)
        # Escritura a staging para evitar que una lectura accidental de output/ mezcle runs.
        out=args.output_dir;out.mkdir(parents=True,exist_ok=True)
        sales.write.mode('overwrite').parquet(str(out/'sales_detail'))
        summary.write.mode('overwrite').parquet(str(out/'monthly_sales'))
        orders.write.mode('overwrite').parquet(str(out/'orders_current'))
        items.write.mode('overwrite').parquet(str(out/'items_current'))
        # Diferentes esquemas originales => un directorio por origen y motivo; nada se pierde sin rastro.
        rejects=dim_rejects+order_rejects+item_rejects
        reasons={}
        for i,frame in enumerate(rejects):
            count=frame.count(); key=f'{i:02d}_{frame.select("_source").first()[0] if count else "empty"}_{frame.select("_reason").first()[0] if count else "none"}'
            if count:
                frame.write.mode('overwrite').json(str(out/'quarantine'/key))
                reasons[key]=count
        checks['quarantined_rows_by_issue']=reasons
        checks['orders_current']=orders.count();checks['items_current']=items.count()
        (out/'qa_report.json').write_text(json.dumps(checks,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps(checks,ensure_ascii=False,indent=2))
    finally:spark.stop()


if __name__=='__main__': main()
