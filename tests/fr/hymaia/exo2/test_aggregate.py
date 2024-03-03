import unittest

from src.fr.hymaia.exo2.aggregate import my_group_by
from tests.fr.hymaia.spark_test_case import spark


class TestMyGroupBy(unittest.TestCase):
    def test_group_by(self) -> None:
        """this function tests the group_by function"""
        # Given
        df = spark.createDataFrame(
            [
                (75650, "Etienne", 30, "gottam city", "75"),
                (20167, "jonny le mechant", 25, "little gottam", "2A"),
                (20235, "Pauley", 47, "CANAVAGGIA", "2B"),
                (10304, "Jean", 40, "Ajaccio", "10"),
                (75404, "berber", 70, "gottam city", "75"),
            ],
            ["zip", "name", "age", "city", "departement"],
        )

        # When
        df_result = my_group_by(df, "departement")
        actual_list = [
            (row["departement"], row["nb_people"]) for row in df_result.collect()
        ]

        # Then
        df_expected = spark.createDataFrame(
            [("75", 2), ("10", 1), ("2A", 1), ("2B", 1)], ["departement", "nb_people"]
        )
        expected_list = [("75", 2), ("10", 1), ("2A", 1), ("2B", 1)]

        self.assertEqual(actual_list, expected_list)
        self.assertEqual(df_result.printSchema(), df_expected.printSchema())

    ################################## Failed test ##################################

    def test_group_by_fail(self) -> None:
        """this function tests the group_by function"""
        # Given
        df = spark.createDataFrame(
            [
                (75650, "Etienne", 30, "gottam city", "75"),
                (20167, "jonny le mechant", 25, "little gottam", "2A"),
                (20235, "Pauley", 47, "CANAVAGGIA", "2B"),
                (10304, "Jean", 40, "Ajaccio", "10"),
                (75404, "berber", 70, "gottam city", "75"),
            ],
            ["zip", "name", "age", "city", "departement"],
        )

        # When
        df_result = my_group_by(df, "departement")
        actual_list = [
            (row["departement"], row["nb_people"]) for row in df_result.collect()
        ]

        # Then
        df_expected = spark.createDataFrame(
            [("75", 1), ("10", 1), ("2A", 1), ("2B", 1)], ["departement", "nb_people"]
        )
        expected_list = [("75", 2), ("10", 1), ("2A", 4), ("2B", 1)]

        self.assertNotEqual(df_result.printSchema, df_expected.printSchema)
        self.assertNotEqual(actual_list, expected_list)


if __name__ == "__main__":
    unittest.main()
