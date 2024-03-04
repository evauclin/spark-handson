import unittest

from pyspark.sql.column import Column, _to_java_column, _to_seq

from tests.fr.hymaia.spark_test_case import spark


def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


class ScalaUdf(unittest.TestCase):
    def test_add_category_name(self):
        df = spark.createDataFrame(
            [
                ("1", "2020-01-01", "5", "10"),
                ("2", "2020-01-01", "6", "10"),
                ("3", "2020-01-01", "7", "10"),
                ("4", "2020-01-01", "2", "10"),
            ],
            ["id", "date", "category", "price"],
        )
        df_actual = df.withColumn("category_name", addCategoryName(df["category"]))
        actual_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
            )
            for row in df_actual.collect()
        ]

        df_expected = spark.createDataFrame(
            [
                ("1", "2020-01-01", "5", "10", "food"),
                ("2", "2020-01-01", "6", "10", "furniture"),
                ("3", "2020-01-01", "7", "10", "furniture"),
                ("4", "2020-01-01", "2", "10", "food"),
            ],
            ["id", "date", "category", "price", "category_name"],
        )
        expected_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
            )
            for row in df_expected.collect()
        ]

        self.assertEqual(actual_list, expected_list)
        self.assertEqual(df_actual.printSchema(), df_expected.printSchema())

    ######################################FAILING TESTS############################################

    def test_add_category_name_fail(self):
        df = spark.createDataFrame(
            [
                ("1", "2020-01-01", "5", "10"),
                ("2", "2020-01-01", "6", "10"),
                ("3", "2020-01-01", "7", "10"),
                ("4", "2020-01-01", "2", "10"),
            ],
            ["id", "date", "category", "price"],
        )
        df_actual = df.withColumn("category_name", addCategoryName(df["category"]))
        actual_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
            )
            for row in df_actual.collect()
        ]

        df_expected = spark.createDataFrame(
            [
                ("1", "2020-01-01", "5", "10", "furniture"),
                ("2", "2020-01-01", "6", "10", "furniture"),
                ("3", "2020-01-01", "7", "10", "furniture"),
                ("4", "2020-01-01", "2", "10", "food"),
            ],
            ["id", "date", "category", "price", "category_name"],
        )
        expected_list = [
            (
                row["date"],
                row["id"],
                row["category"],
                row["price"],
                row["category_name"],
            )
            for row in df_expected.collect()
        ]

        self.assertNotEqual(actual_list, expected_list)
        self.assertNotEqual(df_actual.printSchema, df_expected.printSchema)
