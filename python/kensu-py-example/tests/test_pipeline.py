#  python -m unittest discover -s tests/unit

import logging
import sys
import unittest

import kensu.pandas as pd
from kensu.utils.kensu_provider import KensuProvider


log_format = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)


class TestPipeline(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(slef):
        pass

    def test_pipeline_checks_nrows_consistency(self):
        from src.pipeline import run
        from kensu.utils.exceptions import NrowsConsistencyError
        with self.assertRaises(NrowsConsistencyError):
            run(["may"])

if __name__ == '__main__':
    unittest.main()
