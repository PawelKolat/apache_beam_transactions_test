import unittest

from apache_beam.io import ReadFromText
from apache_beam.testing.test_pipeline import TestPipeline

import transactions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class TransactionsTest(unittest.TestCase):

    ''' transactions_sum.csv contains multiple values with total above 20 and
    date over 2009. With multiple values on same 2017-03-18 and 2017-08-31
    which have to be added '''
    def test_CompositTransform_sum_with_values_to_add(self):
        expected_outcome = [{'date': '2017-03-18', 'total_amount': '400.0003'},
                            {'date': '2017-08-31', 'total_amount': '333.0'},
                            {'date': '2018-02-27', 'total_amount': '129.12'}]


        with TestPipeline() as p:
            lines = (
                    p
                    | 'Read' >> ReadFromText('transactions_sum.csv',skip_header_lines=1)
                    | "Call My Composite Transformation" >> transactions.MyCompositeTransform()
            )
            assert_that(lines, equal_to(expected_outcome))

    ''' transactions_without.csv is the same as in working default example but 
    downloaded for unit testing'''
    def test_CompositTransform_sum_without_adding(self):
        expected_outcome = [{'date': '2017-03-18', 'total_amount': '2102.22'},
                            {'date': '2017-08-31', 'total_amount': '13700000023.08'},
                            {'date': '2018-02-27', 'total_amount': '129.12'}]


        with TestPipeline() as p:
            lines = (
                    p
                    | 'Read' >> ReadFromText('transactions_without.csv',skip_header_lines=1)
                    | "Call My Composite Transformation" >> transactions.MyCompositeTransform()
            )
            assert_that(lines, equal_to(expected_outcome))



