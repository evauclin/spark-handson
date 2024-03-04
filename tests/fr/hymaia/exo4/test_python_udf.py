import time
import unittest

from src.fr.hymaia.exo4.python_udf import categorize_category


class PythonUdf(unittest.TestCase):

    def test_categorize_category(self):
        self.assertEqual(categorize_category(5), "food")
        self.assertEqual(categorize_category(6), "furniture")
        self.assertEqual(categorize_category(7), "furniture")
        self.assertEqual(categorize_category(10), "furniture")
        self.assertEqual(categorize_category(0), "food")
        self.assertEqual(categorize_category(1), "food")
        self.assertEqual(categorize_category(2), "food")
        self.assertEqual(categorize_category(3), "food")
        self.assertEqual(categorize_category(4), "food")

    #######################################FAILING TESTS############################################
    def test_categorize_category_fail(self):
        self.assertNotEqual(categorize_category(5), "furniture")
        self.assertNotEqual(categorize_category(6), "food")
        self.assertNotEqual(categorize_category(7), "food")
        self.assertNotEqual(categorize_category(10), "food")
        self.assertNotEqual(categorize_category(0), "furniture")
        self.assertNotEqual(categorize_category(1), "furniture")
        self.assertNotEqual(categorize_category(2), "furniture")
        self.assertNotEqual(categorize_category(3), "furniture")
        self.assertNotEqual(categorize_category(4), "furniture")
