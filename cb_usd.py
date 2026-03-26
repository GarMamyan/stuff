# выгрузка данных о курсе валют (доллар/рубль) с официального API Центрального Банка России
import pandas as pd
import requests
from datetime import datetime

def get_usd_rub_rates(start_date="2024-12-26"):
    # Преобразуем даты в формат, нужный для API
    today = datetime.today()
    end_date = today.strftime("%d/%m/%Y")
    start_date_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y")

    url = f"https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date_fmt}&date_req2={end_date}&VAL_NM_RQ=R01235"
    
    response = requests.get(url)
    response.encoding = 'windows-1251'  # ЦБ РФ использует windows-1251



    # Проверка статуса ответа
    if response.status_code != 200:
        raise Exception("Не удалось получить данные от ЦБ РФ")
    
    # Парсим XML
    from xml.etree import ElementTree as ET
    root = ET.fromstring(response.text)

    records = []
    for record in root.findall("Record"):
        date = datetime.strptime(record.attrib["Date"], "%d.%m.%Y")
        value = float(record.find("Value").text.replace(",", "."))
        records.append((date, value))

    df = pd.DataFrame(records, columns=["date", "usd_to_rub"])
    df.sort_values("date", inplace=True)
    df.set_index("date", inplace=True)

    # Создаем полный диапазон дат
    full_range = pd.date_range(start=start_date, end=today)
    df = df.reindex(full_range)

    # Заполняем пропуски предыдущими значениями
    df.ffill(inplace=True)

    # Переименовываем индекс обратно в колонку
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)

    return df

# Пример использования:
df = get_usd_rub_rates()
print(df)
