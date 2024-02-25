import unittest
from src.fr.hymaia.exo1.wordcount import wordcount
from tests.fr.hymaia.spark_test_case import spark


class TestWordCount(unittest.TestCase):

    def test_word_count(self) -> None:
        # Given
        df = spark.createDataFrame(
            [
                ("hello world",),
                ("hello",),
            ],
            ["text"],
        )

        # When
        df_result = wordcount(df, "text")
        parsed_result = [(row["word"], row["count"]) for row in df_result.collect()]

        # Then
        df_expected_result = spark.createDataFrame(
            [
                ("world", 1),
                ("hello", 2),
            ],
            ["word", "count"],
        )

        expected_result = [("world", 1), ("hello", 2)]

        self.assertEqual(df_result.printSchema(), df_expected_result.printSchema())
        self.assertEqual(parsed_result, expected_result)


if __name__ == "__main__":
    unittest.main()
