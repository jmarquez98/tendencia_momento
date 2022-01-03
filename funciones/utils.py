from  matplotlib.colors import LinearSegmentedColormap
import dataframe_image as dfi
import numpy as np
import pandas as pd
from datetime import datetime
import os
import sys
import math
import copy
from pandas.core.common import SettingWithCopyWarning
import warnings
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
import pathlib
path = str(pathlib.Path(__file__).parent.absolute())

sys.path.insert(1, "../common")
import moduloGenerico as mg







def prepare_folders(short, tipo_etf):
    path_reportes = path + f"/../modelos_{short}/{tipo_etf}/reportes"
    if not os.path.exists(path_reportes):
        os.makedirs(path_reportes)
    else:
        for f in os.listdir(path_reportes): os.remove(path_reportes + "/" + f)

    for pond in ["equal", "volatility"]:
        path_reporte = path + f"/../modelos_{short}/{tipo_etf}/{tipo_etf}_{short}_{pond}.html"
        if os.path.exists(path_reporte): os.remove(path_reporte)

    return







def get_etfs(tipo_etf="GEOGRAFIA"):

    Data_etfs = pd.read_excel(path+"/../ETFS.xlsx", sheet_name=tipo_etf)
    Data_etfs.dropna(subset=["TICKER"], inplace=True)

    if tipo_etf=="GEOGRAFIA":
        data_etfs = Data_etfs.loc[Data_etfs["TYPE"]=="countries"]
        data_etfs = data_etfs.loc[data_etfs["CLASS"] == "developed"]
        data_etfs.sort_values(["CLASS", "CONTINENT"], ascending=(True, True))
        data_etfs = data_etfs[["TICKER", "CLASS", "CONTINENT", "ETF"]]


    elif tipo_etf=="INDUSTRIAS":
        data_etfs = Data_etfs.loc[Data_etfs["TYPE"] == "industry"]
        data_etfs.sort_values("SECTOR", ascending=True)
        data_etfs = data_etfs[["TICKER", "SECTOR", "INDUSTRY", "ETF"]]


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





def prepare_data(precios):
    precios = copy.deepcopy(precios)
    # Mensualizo las series de precios y le agrego la columma "Return"
    data = {}
    for ticker in precios:
        df = precios[ticker].copy(deep=True)
        df = mg.get_dateValues(dates_df=df, last_dates=5000, period="M")
        df["Return"] = df["Close"].pct_change()
        df["Volatility"] = df["Return"].rolling(120, min_periods=1).std(ddof=1)
        data[ticker] = df
    return data







def get_trends_df(data, trend_window=12, save_df=False):
    data = copy.deepcopy(data)
    retornos = pd.DataFrame()
    for ticker in data:
        df = data[ticker].copy(deep=True)
        df["Trend"] = df["Close"].pct_change(trend_window)
        df[ticker] = df["Trend"]
        if save_df: df.to_excel(path + f"/../etf_dfs/{ticker}.xlsx")
        retornos = pd.concat([retornos, df[[ticker]]], axis=1)
    retornos.sort_index(ascending=True, inplace=True)
    retornos = retornos.T

    return retornos








def get_trend_and_momentum(retornos):
    retornos = retornos.copy(deep=True)

    # Creo diccionario que indica para cada fecha que ETFs shorteo y cuales voy long
    fechas = retornos.columns.to_list()
    inversiones = {}
    for fecha in fechas:
        tabla = retornos.copy(deep=True)
        tabla.dropna(inplace=True, subset=[fecha])
        tabla.sort_values(by=fecha, ascending=False,
                          inplace=True)  # Ordeno las filas (tickers) en base a las tendencias

        cut = math.ceil(len(tabla) * 0.25)  # Cantidad de ETFs que tomaré para ir long y short

        longs = tabla.iloc[:cut]
        shorts = tabla.iloc[-cut:]

        longs = longs.loc[longs[fecha] > 0]  # Solo ire long ETFs con tendencia positiva
        shorts = shorts.loc[shorts[fecha] < 0]  # Solo ire short ETFs con tendencia negativa

        tickers_l = longs.index.to_list()
        tickers_s = shorts.index.to_list()

        inversiones[fecha] = {"long": tickers_l, "short": tickers_s}

    return inversiones









def get_returns_df(data, inversiones):
    data = copy.deepcopy(data)
    inversiones = copy.deepcopy(inversiones)

    tickers = list(data.keys())

    """
    En el dataframe posicion colocare para cada fecha y ticker el retorno que aporta al portafolio. Es decir, un ETF que
    no esta long ni short, tendra retorno cero, un ETF que esta long tendra su retorno positivo, y un ETF que esta short
    tendra su retorno * (-1). Luego se le debe aplicar ponderacion
    """
    fechas = list(inversiones.keys())
    posicion = pd.DataFrame(columns=tickers)
    rets_df = pd.DataFrame(columns=tickers)
    for i in range(1, len(fechas)):
        hoy = fechas[i]
        ayer = fechas[i - 1]

        longs = inversiones[ayer]["long"]
        shorts = inversiones[ayer]["short"]

        # Parto de un retorno=0 para todos los ETFs, luego modificare cada celda segun corresponda.
        posicion.loc[hoy] = 0

        # Si para una fecha no hay activos long ni short, entonces el retorno total de esa fecha sera cero.
        if len(longs + shorts) == 0: continue

        for ticker in longs:
            retorno = data[ticker].loc[hoy, "Return"]
            retorno = retorno
            posicion.loc[hoy, ticker] = retorno

        for ticker in shorts:
            retorno = data[ticker].loc[hoy, "Return"]
            retorno = retorno
            posicion.loc[hoy, ticker] = -retorno

    posicion.sort_index(ascending=True, inplace=True)

    return posicion








def get_weights_df(data, fechas, pos, weight_type="equal", inversiones=None):

    if pos not in ["all", "long", "short", "long/short"]:
        raise AttributeError("Attribute 'pos' must equal 'all', 'long', 'short' or 'long/short'.")

    if pos in ["long", "short", "long/short"]:
        if inversiones is None:
            raise AttributeError("Attribute 'inversiones' must be a dict with keys 'long' and 'short' if 'pos' equals 'long', 'short' or 'long/short'.")

    if weight_type not in ["equal", "volatility"]:
        raise AttributeError("Attribute 'weight_type' must equal 'equal' or 'volatility'.")

    data = copy.deepcopy(data)
    inversiones = copy.deepcopy(inversiones)

    tickers = list(data.keys())

    pond_df = pd.DataFrame(columns=tickers)
    for i in range(1, len(fechas)):
        hoy = fechas[i]
        ayer = fechas[i - 1]

        if pos=="all": tickers_fecha=tickers[:]
        else:
            longs = inversiones[ayer]["long"]
            shorts = inversiones[ayer]["short"]

            if pos=="long": tickers_fecha = longs
            elif pos=="short": tickers_fecha = shorts
            elif pos=="long/short": tickers_fecha = longs + shorts
            else: tickers_fecha = None


        pond_df.loc[hoy] = 0 # Parto de un retorno=0 para todos los ETFs, luego modificare cada celda segun corresponda.

        # Si para una fecha no hay activos long ni short, entonces el retorno total de esa fecha sera cero.
        if len(tickers_fecha) == 0: continue

        den = 0
        for ticker in tickers_fecha:
            if hoy not in data[ticker].index: continue
            if np.isnan(data[ticker].loc[hoy, "Return"]): continue

            if weight_type == "equal":
                pond_df.loc[hoy, ticker] = 1
                den += 1

            elif weight_type == "volatility":
                vol = data[ticker].loc[hoy, "Volatility"]
                pond = 1 / vol
                if not pond==pond: pond = 0
                pond_df.loc[hoy, ticker] = pond
                den += pond

        pond_df.loc[hoy] = pond_df.loc[hoy] / den

    return pond_df