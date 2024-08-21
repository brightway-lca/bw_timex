import pandas as pd
import bw2data as bd

from datetime import datetime
from bw_timex import TimexLCA

def test_process_at_base_database_time(process_at_base_database_time_db):
    method = ("GWP", "example")
    database_date_dict = {
        "background_2020": datetime.strptime("2020", "%Y"),
        "background_2030": datetime.strptime("2030", "%Y"),
        "foreground": "dynamic", # flag databases that should be temporally distributed with "dynamic"
    }
    fu = ("foreground", "fu")
    tlca = TimexLCA({fu: 1}, method, database_date_dict)
    tlca.build_timeline()
    tlca.lci()
    
    expected_inventory = pd.DataFrame(
        data={
            "date": pd.Series(
                data=[
                    "01-01-2019",
                    "01-01-2020",
                    "01-01-2021",
                ],
                dtype="datetime64[s]",
            ),
            "amount": pd.Series(
                data=[0.2*336, 0.8*336+0.2*504, 0.8*504], dtype="float64"
            ),
        }
    )
    
    pd.testing.assert_frame_equal(tlca.dynamic_inventory_df[["date", "amount"]], expected_inventory)

    