import unittest
import os
import sys
import pandas as pd
from pandas.testing import assert_frame_equal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from src.mqtt_to_influxdb.dataconverter import convert_dataseries_to_df, cbc_dict_to_line_protocol, agg_dict_to_line_protocol

class TestDataseriesConverter(unittest.TestCase):
    def test_dataseries_simple(self):
        dataseries = {"CH1": {"data": [1,2,3,4,5], "timestamps": [0,1,2,3,4]},
                      "CH2": {"data": [6,7,8,9,10], "timestamps": [0,1,2,3,4]}}
        expected_df = pd.DataFrame(data={"CH1": [1,2,3,4,5], "CH2": [6,7,8,9,10], "timestamp": pd.to_datetime([0,1000,2000,3000,4000],utc=True)}, 
                                   index=pd.to_datetime([0,1000,2000,3000,4000],utc=True))
        df = convert_dataseries_to_df(dataseries)
        assert_frame_equal(expected_df, df)

    def test_dataseries_unaligned(self):
        pass


class TestDataseriesToLineConverter(unittest.TestCase):
    def test_dataseries_simple(self):
        dataseries = {"CH1": {"data": [1.0,2.0], "timestamps": [0,1]},
                      "CH2": {"data": [6.0,7.0], "timestamps": [0,1]}}
        tags = {"tag1": "value1"}
        expected_lp = ["cycle-by-cycle,tag1=value1 CH1=1.000000 0",
                       "cycle-by-cycle,tag1=value1 CH1=2.000000 1000",
                       "cycle-by-cycle,tag1=value1 CH2=6.000000 0",
                       "cycle-by-cycle,tag1=value1 CH2=7.000000 1000"]
        lp_data = cbc_dict_to_line_protocol(dataseries, tags)
        self.assertEqual(expected_lp, lp_data)

class TestAggDataToLineConverter(unittest.TestCase):
    def test_aggdata_simple(self):
        m_data = {"interval_sec": 1, "timestamp": 1.0, "data": {"CH1": 1.0, "CH2": [2.0, 3.0, 4.0]}}
        tags = {"interval_sec": m_data["interval_sec"]}
        expected_lp = "aggregated-data,interval_sec=1 CH1=1.000000,CH2_00=2.000000,CH2_01=3.000000,CH2_02=4.000000 1000000000"
        lp_data = agg_dict_to_line_protocol(m_data, tags)
        self.assertEqual(expected_lp, lp_data)

if __name__ == "__main__":
    unittest.main()