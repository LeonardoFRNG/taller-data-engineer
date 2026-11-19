"""Pruebas Spark pequeñas; ejecutar: python -m unittest discover -s tests -v"""
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pyspark.sql import functions as F
from pipeline import spark_session, deduplicate, monthly_summary, reconciliation

class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.spark=spark_session()
    @classmethod
    def tearDownClass(cls):cls.spark.stop()

    def test_latest_update_wins(self):
        rows=[('O1','2026-01-01T00:00:00Z','old'),('O1','2026-02-01T00:00:00Z','new')]
        df=self.spark.createDataFrame(rows,['order_id','updated_at','status']).withColumn('updated_at',F.to_timestamp('updated_at'))
        current,discarded=deduplicate(df,'order_id',True)
        self.assertEqual(current.first().status,'new')
        self.assertEqual(discarded.count(),1)

    def test_reconciliation(self):
        from decimal import Decimal
        rows=[('L1','2026-01','CO','web','books',Decimal('15.0000')),
              ('L2','2026-01','CO','web','books',Decimal('8.2500'))]
        df=self.spark.createDataFrame(rows,['line_id','month','country','channel','category','net_amount'])
        summary=monthly_summary(df)
        report=reconciliation(df,summary)
        self.assertTrue(report['reconciliation_ok'])
        self.assertEqual(report['detail_lines'],2)

if __name__=='__main__': unittest.main()
