import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.aggregate import my_group_by
from tests.fr.hymaia.spark_test_case import spark


class TestMyGroupBy(unittest.TestCase):
    def test_group_by(self) -> None:
        '''this function tests the group_by function'''
        # Given
        df = spark.createDataFrame(
            [
                (75650, "Etienne", 30, "gottam city", "75"),
                (20167, "jonny le mechant", 25, "little gottam", "2A"),
                (20235, "Pauley", 47, "CANAVAGGIA", "2B"),
                (10304, "Jean", 40, "Ajaccio", "10"),
                (75404, "berber", 70, "gottam city", "75")
            ],
            ["zip", "name", "age", "city", "departement"]
        )

        # When
        actual = my_group_by(df, "departement")

        # Transform actual result to match the expected format
        actual_list = [(row["departement"], row["nb_people"]) for row in actual.collect()]

        # Then
        expected = [('75', 2), ('10', 1), ('2A', 1), ('2B', 1)]

        self.assertEqual(actual_list, expected)


    def test_group_by_fail(self) -> None:
        '''this function tests the group_by function'''
        # Given
        df = spark.createDataFrame(
            [
                (75650, "Etienne", 30, "gottam city", "75"),
                (20167, "jonny le mechant", 25, "little gottam", "2A"),
                (20235, "Pauley", 47, "CANAVAGGIA", "2B"),
                (10304, "Jean", 40, "Ajaccio", "10"),
                (75404, "berber", 70, "gottam city", "75")
            ],
            ["zip", "name", "age", "city", "departement"]
        )

        # When
        actual = my_group_by(df, "departement")

        # Transform actual result to match the expected format
        actual_list = [(row["departement"], row["nb_people"]) for row in actual.collect()]

        expected = [('75', 3), ('10', 1), ('2A', 1), ('2B', 1)]  # Intentionally changed the value for '75'

        # Uncomment the line below to see the intentional failure
        #self.assertEqual(actual_list, expected)
        with self.assertRaises(AssertionError):
            self.assertEqual(actual_list, expected)



if __name__ == "__main__":
    unittest.main()
