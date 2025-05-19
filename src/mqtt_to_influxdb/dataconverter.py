import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

def convert_dataseries_to_df(dataseries: dict):
    df_list = []
    for channel in dataseries:
        df = pd.DataFrame(data= dataseries[channel]["data"], 
                        index=pd.to_datetime([ts*1_000 for ts in dataseries[channel]["timestamps"]], utc=True),
                        columns=[channel])
        df_list.append(df)
    df = pd.concat(df_list, axis=0)
    df["timestamp"] = df.index
    return df