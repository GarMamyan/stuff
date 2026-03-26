import requests
import pandas as pd


def get_moex_history(tickers, start_date, end_date, board="TQBR"):

    
    """
    Получает дневные данные торгов по акциям MOEX с автоматической пагинацией.
    Parameters
    ----------
    tickers : list[str]
        Список тикеров (например ["SBER", "GAZP"])
    start_date : str
        Дата начала YYYY-MM-DD
    end_date : str
        Дата конца YYYY-MM-DD
    board : str
        Режим торгов (по умолчанию TQBR)
    Returns
    -------
    dict[str, pd.DataFrame]
        словарь ticker -> dataframe
    """
    base_url = "https://iss.moex.com/iss/history/engines/stock/markets/shares/boards"
    result = {}
    for ticker in tickers:
        start = 0
        all_rows = []
        columns = None
        while True:
            url = f"{base_url}/{board}/securities/{ticker}.json"
            params = {
                "from": start_date,
                "till": end_date,
                "start": start,
                "iss.meta": "off",
                "iss.only": "history"
            }
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()["history"]
            if columns is None:
                columns = data["columns"]
            rows = data["data"]
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < 100:
                break
            start += 100
        df = pd.DataFrame(all_rows, columns=columns)
        if not df.empty:
            df = df[[
                "TRADEDATE",
                "OPEN",
                "LOW",
                "HIGH",
                "CLOSE",
                "VOLUME"
            ]]
            df["TRADEDATE"] = pd.to_datetime(df["TRADEDATE"])
            df = df.sort_values("TRADEDATE").reset_index(drop=True)
        result[ticker] = df
    return result

def get_moex_history_2_tickers(tickers, start_date, end_date):
    """
    Получает дневные данные торгов по акциям MOEX у которых сменился тикер.
    На вход принимает два тикера, в той последовательности, как они появлялись/пропадали на бирже 

    Пока функуия сырая, для акций, у которых тикер сменялся один раз, возможно потом добавлю чуть доработанную версию 
    """

    data_ticker1 = get_moex_history(tickers[0], start_date, end_date)[tickers[0][0]].dropna()
    data_ticker2 = get_moex_history(tickers[1], start_date, end_date)[tickers[1][0]].dropna()
    data = pd.concat([data_ticker1 , data_ticker2]).reset_index()
    return data


