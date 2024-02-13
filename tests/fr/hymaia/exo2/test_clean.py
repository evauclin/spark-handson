import unittest
from src.fr.hymaia.exo2.clean import departement_format, age_filter, my_join
from tests.fr.hymaia.spark_test_case import spark


class TestCleanFunctions(unittest.TestCase):
    def test_age_filter(self) -> None:
        '''this function tests the age_filter function'''
        # Given
        df = spark.createDataFrame(
            [
                ('Cussac', 27, 75020),
                ('Etienne', 17, 75020),
                ('davis', 18, 75020)],

            ["name", "age", "zip"]
        )

        # When
        df_result = age_filter(df, "age")
        actual_list = [(row["name"], row["age"], row["zip"]) for row in df_result.collect()]

        # Then
        df_expected = spark.createDataFrame(
            [
                ('Cussac', 27, 75020),
                ('davis', 18, 75020)],

            ["name", "age", "zip"]
        )
        expected_list = [(row["name"], row["age"], row["zip"]) for row in df_expected.collect()]


        self.assertEqual(actual_list, expected_list)
        self.assertEqual(df_result.printSchema(), df_expected.printSchema())


    def test_join(self) -> None:
        '''this function tests the my_join function'''
        # Given
        df_clients = spark.createDataFrame(
            [
                ('Cussac', 27, 75020)
                ],
            ["name", "age", "zip"]
        )

        df_city = spark.createDataFrame(
            [
                (75020, 'Paris')
                ],
            ["zip", "city"]
        )

        # When
        df_result = my_join(df_clients, df_city, "zip")
        actual_list = [(row["name"], row["age"], row["zip"], row["city"]) for row in df_result.collect()]

        # Then
        df_expected = spark.createDataFrame(
            [
                ('Cussac', 27, 75020, 'Paris')
                ],
            ["name", "age", "zip", "city"]
        )
        expected_list = [(row["name"], row["age"], row["zip"], row["city"]) for row in df_expected.collect()]

        self.assertEqual(actual_list, expected_list)
        self.assertEqual(df_result.printSchema(), df_expected.printSchema())


    def test_departement_format(self) -> None:
        '''this function tests the departement_format function'''
        # Given
        df_clients = spark.createDataFrame(
            [
                ('Culdusac', 27, 75020),
                ('david', 40, 972),
                ('eti', 40, 1)
                ],
            ["name", "age", "zip"]
        )

        df_city = spark.createDataFrame(
            [
                (75020, 'Paris'),
                (972, 'Martinique'),
                (1, 'first')
                ],

            ["zip", "city"]
        )

        # When
        df_result = departement_format(my_join(df_clients, df_city, "zip"))
        actual_list = [(row["name"], row["age"], row["zip"], row["city"], row["departement"]) for row in
                       df_result.collect()]

        # Then
        df_expected = spark.createDataFrame(
            [
                ('Culdusac', 27, 75020, 'Paris', '75'),
                ('david', 40, 972, 'Martinique', '97'),
                ('eti', 40, 1, 'first', '01')
                ],
            ["name", "age", "zip", "city", "departement"]
        )
        expected_list = [(row["name"], row["age"], row["zip"], row["city"], row["departement"]) for row in df_expected.collect()]
        
        self.assertEqual(actual_list, expected_list)
        self.assertEqual(df_result.printSchema(), df_expected.printSchema())

if __name__ == "__main__":
    unittest.main()
