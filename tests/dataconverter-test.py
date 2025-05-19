import unittest
import os
import sys
import pandas as pd
from pandas.testing import assert_frame_equal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from src.mqtt_to_influxdb.dataconverter import convert_dataseries_to_df

class TestDataseriesConverter(unittest.TestCase):
    def test_dataseries_simple(self):
        dataseries = {"CH1": {"data": [1,2,3,4,5], "timestamps": [0,1,2,3,4]},
                      "CH2": {"data": [6,7,8,9,10], "timestamps": [0,1,2,3,4]}}
        expected_df = pd.DataFrame(data={"CH1": [1,2,3,4,5], "CH2": [6,7,8,9,10], "timestamp": pd.to_datetime([0,1,2,3,4])}, 
                                   index=pd.to_datetime([0,1,2,3,4]))
        df = convert_dataseries_to_df(dataseries)
        assert_frame_equal(expected_df, df)

    def test_dataseries_unaligned(self):
        pass