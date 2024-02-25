from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.clean import data_cleaning
from src.fr.hymaia.exo2.aggregate import my_group_by
import unittest


class TestIntegration(unittest.TestCase):
    def test_integration_cleaning(self):
        df_client = spark.createDataFrame(
            [
                ("John", 25, 40100),
                ("Jane", 17, 90100),
                ("Bob", 30, 18200),
                ("Alice", 40, 75200),
            ],
            ["name", "age", "zip"],
        )

        df_city = spark.createDataFrame(
            [
                (40100, "Marseille"),
                (90100, "Ajaccio"),
                (18200, "Bastia"),
                (75200, "Paris"),
            ],
            ["zip", "city"],
        )

        df_result = data_cleaning(df_client, df_city)
        actual_list = [
            (row["zip"], row["name"], row["age"], row["city"], row["departement"])
            for row in df_result.collect()
        ]

        df_expected = spark.createDataFrame(
            [
                (40100, "John", 25, "Marseille", "40"),
                (18200, "Bob", 30, "Bastia", "18"),
                (75200, "Alice", 40, "Paris", "75"),
            ],
            ["zip", "name", "age", "city", "departement"],
        )
        expected_list = [
            (row["zip"], row["name"], row["age"], row["city"], row["departement"])
            for row in df_expected.collect()
        ]

        self.assertEqual(df_result.printSchema(), df_expected.printSchema())
        self.assertEqual(actual_list, expected_list)

    def test_integration_aggregate(self):
        df = spark.createDataFrame(
            [
                (40100, "John", 25, "Marseille", "40"),
                (18200, "Bob", 30, "Bastia", "18"),
                (75200, "Alice", 40, "Paris", "75"),
                (40100, "x", 25, "Marseille", "40"),
                (18200, "B", 30, "Bastia", "18"),
                (75200, "Ace", 40, "Paris", "75"),
                (20100, "Joh", 25, "Marseille", "20"),
            ],
            ["zip", "name", "age", "city", "departement"],
        )

        df_result = my_group_by(df, "departement")
        actual_list = [
            (row["departement"], row["nb_people"]) for row in df_result.collect()
        ]

        df_expected = spark.createDataFrame(
            [("18", 2), ("40", 2), ("75", 2), ("20", 1)], ["departement", "nb_people"]
        )

        expected_list = [
            (row["departement"], row["nb_people"]) for row in df_expected.collect()
        ]

        self.assertEqual(df_result.printSchema(), df_expected.printSchema())
        self.assertEqual(actual_list, expected_list)
