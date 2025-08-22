import pandas as pd
import orjson
import logging
import math

logger = logging.getLogger(__name__)

def convert_dataseries_to_df(dataseries: dict):
    df_list = []
    for channel in dataseries:
        df = pd.DataFrame(data= dataseries[channel]["data"], 
                        index=pd.to_datetime([ts*1_000 for ts in dataseries[channel]["timestamps"]], utc=True),
                        columns=[channel])
        df_list.append(df)
    df = pd.concat(df_list, axis=1)
    df["timestamp"] = df.index
    return df

def cbc_dict_to_line_protocol(m_data: dict, tags: dict, measurement: str = "cycle-by-cycle"):
    lines = []
    for ch_name, ch_values in m_data.items():
        timestamps = ch_values["timestamps"]
        values = ch_values["data"]
        tag_str = ",".join(f"{k}={v}" for k, v in tags.items())
        for i in range(len(timestamps)):            
            line = f"{measurement}"
            if tag_str:
                line += f",{tag_str}"
            line += f" {ch_name}={values[i]:f} {timestamps[i]*1000}"
            lines.append(line)
    return lines

def agg_dict_to_line_protocol(m_data: dict, tags: dict, measurement: str = "aggregated-data"):
    tag_str = ",".join(f"{k}={v}" for k, v in tags.items())
    line = ""
    line += measurement + "," + tag_str + " "

    for ch_name, val in m_data['data'].items():
        if ch_name.startswith('_'):
            continue
        if type(val) in [list]:
            for idx, item in enumerate(val):
                if (item is None) or math.isnan(item):
                    continue
                line += f"{ch_name}_{idx:02d}={float(item):f},"
        else:
            if (val is None) or math.isnan(val):
                continue
            line += f"{ch_name}={float(val):f},"
    
    line = line[:-1] # replace tailing comma
    line += f" {int(m_data['timestamp']*1e9):d}"
    return line