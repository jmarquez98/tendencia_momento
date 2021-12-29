from  matplotlib.colors import LinearSegmentedColormap
import dataframe_image as dfi
import numpy as np
import pandas as pd
from datetime import datetime
import os
from pandas.core.common import SettingWithCopyWarning
import warnings
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
import pathlib
path = str(pathlib.Path(__file__).parent.absolute())








def get_etfs(tipo_etf="GEOGRAFIA"):

    Data_etfs = pd.read_excel(path+"/../ETFS.xlsx", sheet_name=tipo_etf)
    Data_etfs.dropna(subset=["TICKER"], inplace=True)

    if tipo_etf=="GEOGRAFIA":
        data_etfs = Data_etfs.loc[Data_etfs["TYPE"]=="countries"]
        data_etfs.sort_values(["CLASS", "CONTINENT"], ascending=(True, True))
        data_etfs = data_etfs[["TICKER", "CLASS", "CONTINENT", "ETF"]]


    elif tipo_etf=="INDUSTRIAS":
        data_etfs = pd.DataFrame(columns = Data_etfs.columns)
        sectores = list(set(Data_etfs["SECTOR"]))

        for sector in sectores:
            data_etfs = pd.concat([data_etfs, Data_etfs.loc[(Data_etfs.TYPE=="sector") & (Data_etfs.SECTOR==sector)]])
            data_etfs = pd.concat([data_etfs, Data_etfs.loc[(Data_etfs.TYPE=="industry") & (Data_etfs.SECTOR==sector)]])

    elif tipo_etf=="FACTORES":
        data_etfs = Data_etfs
        data_etfs.rename(columns={"FACTOR": "CLASS"}, inplace=True)

    elif tipo_etf=="COMMODITIES":
        data_etfs = Data_etfs
        data_etfs.rename(columns={"COMMODITY": "CLASS"}, inplace=True)

    elif tipo_etf=="DEUDA":
        data_etfs = Data_etfs
        data_etfs.rename(columns={"TYPE": "CLASS"}, inplace=True)

    else:
        raise AttributeError("'tipo_etf' attribute must equal 'GEOGRAFIA', 'INDUSTRIAS', 'FACTORES', 'COMMODITIES' or 'DEUDA'.")

    data_etfs.set_index("TICKER", inplace=True)

    return data_etfs
