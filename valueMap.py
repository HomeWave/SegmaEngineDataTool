#!/usr/bin/env python 
# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
spark = SparkSession.builder .appName('my_first_app_name').getOrCreate()


def valueMap(TableName, to_replace, value, subset=None):
    '''

    :param TableName: input table
    :param to_replace: bool, int, long, float, string, list or dict.
            Value to be replaced.
            If the value is a dict, then `value` is ignored or can be omitted, and `to_replace`
            must be a mapping between a value and a replacement.
    :param value:bool, int, long, float, string, list or None.
            The replacement value must be a bool, int, long, float, string or None. If `value` is a
            list, `value` should be of the same length and type as `to_replace`.
            If `value` is a scalar and `to_replace` is a sequence, then `value` is
            used as a replacement for each item in `to_replace`.

    :param subset:optional list of column names to consider.
            Columns specified in subset that do not have matching data type are ignored.
            For example, if `value` is a string, and subset contains a non-string column,
            then the non-string column is simply ignored.

    :return:
    '''
    replace_table = TableName.na.replace(to_replace, value, subset)
    # replace_table.show()
    return replace_table




