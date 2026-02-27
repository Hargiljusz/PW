import pandas as pd
import requests
import io
import time
import os
import numpy as np

def download_weather_for_airports(airports:list[str] | dict[str,str], year=2024) -> None:
    if not (isinstance(airports,list) | isinstance(airports,dict)):
        return
    
    if isinstance(airports, list):
        airport_mapping = {code: code for code in airports}
    elif isinstance(airports, dict):
        airport_mapping = airports
    else:
        raise ValueError("Parametr 'airports' musi być listą lub słownikiem.")

    #all_weather_data = []

    # Funkcja pomocnicza do łączenia kodów pogodowych z danej godziny
    def join_wxcodes(series):
        codes = series.dropna().astype(str).tolist()
        return " ".join(codes) if codes else ""

    print("Rozpoczynam pobieranie i agregację danych pogodowych za 2024 rok...")

    for iata_code, api_code in airport_mapping.items():
        print(f"Przetwarzanie lotniska: {iata_code}...")

        file_path = f'./data/raw_data/weather/weather_{iata_code}_2024.parquet'
        if os.path.exists(file_path):
            print(f"Dane dla lotniska: {iata_code} zostały już pobrane wcześniej")
            continue
        
        # URL z dodanymi zmiennymi: gust (porywy) i wxcodes (kody zjawisk)
        url = (
            f"https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
            f"station={api_code}&data=tmpf&data=sknt&data=gust&data=p01m&data=vsby&data=wxcodes"
            f"&year1=2024&month1=1&day1=1&year2=2025&month2=1&day2=1"
            f"&tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=M&trace=T&direct=no"
        )
        
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            # Wczytanie danych (na_values='M' bo IEM oznacza braki danych literą M)
            df_airport = pd.read_csv(io.StringIO(response.text), na_values='M')
            
            if not df_airport.empty:
                # Konwersja czasu na format datetime
                df_airport['valid'] = pd.to_datetime(df_airport['valid'])
                
                # Wyciągnięcie daty i godziny do łączenia (klucze merge'a)
                df_airport['FlightDate'] = df_airport['valid'].dt.date
                df_airport['Hour'] = df_airport['valid'].dt.hour
                
                # Wypełnienie brakujących porywów zerami (jeśli nie ma porywu, to wieje płynnie)
                df_airport['gust'] = df_airport['gust'].fillna(0)

                numeric_cols = ['tmpf', 'sknt', 'gust', 'p01m', 'vsby']
                for col in numeric_cols:
                    if col in df_airport.columns:
                        # 1. Zamiana 'T' (trace) na małą wartość numeryczną (np. 0.005)
                        # Jeśli wolisz 0, zamień 0.005 na 0.
                        df_airport[col] = df_airport[col].replace('T', 0.001)
                        
                        # 2. Wymuszenie typu numerycznego (błędy -> NaN)
                        df_airport[col] = pd.to_numeric(df_airport[col], errors='coerce')
                
                # AGREGACJA DO RÓWNYCH ODSTĘPÓW GODZINNYCH
                df_hourly = df_airport.groupby(['station', 'FlightDate', 'Hour']).agg({
                    'tmpf': 'mean',          # Średnia temperatura
                    'sknt': 'mean',          # Średni wiatr
                    'gust': 'max',           # Maksymalny poryw wiatru (najważniejszy dla opóźnień)
                    'p01m': 'max',           # Maksymalny opad
                    'vsby': 'min',           # Minimalna widoczność (najgorsza w danej godzinie)
                    'wxcodes': join_wxcodes  # Zebranie wszystkich zjawisk z danej godziny do jednego tekstu
                }).reset_index()
                
                for col in numeric_cols:
                    df_hourly[col] = df_hourly[col].astype(float)
                # --- INŻYNIERIA CECH (Tworzenie flag binarnych do modelu predykcyjnego) ---
                
                # 1. Burze (TS - Thunderstorm)
                df_hourly['is_thunderstorm'] = df_hourly['wxcodes'].str.contains('TS', na=False).astype(int)
                
                # 2. Śnieg (SN - Snow)
                df_hourly['is_snow'] = df_hourly['wxcodes'].str.contains('SN', na=False).astype(int)
                
                # 3. Marznący deszcz (FZ - Freezing Rain, bardzo groźne dla lotnictwa)
                df_hourly['is_freezing'] = df_hourly['wxcodes'].str.contains('FZ', na=False).astype(int)
                
                # Opcjonalnie: Usuwamy oryginalną, tekstową kolumnę wxcodes, bo zrobiliśmy z niej flagi
                df_hourly.drop(columns=['wxcodes'], inplace=True)
                
                df_hourly.to_parquet(f'./data/raw_data/weather/weather_{iata_code}_2024.parquet',index=False)
                print(f'Zapisano dane pogodowe dla lotniska: {iata_code} w pliku: ./data/raw_data/weather/weather_{iata_code}_2024.parquet')
                
        except Exception as e:
            print(f"Błąd dla lotniska {iata_code}: {e}")
            
        # Ważne: Pauza 1 sekundy, aby nie zablokować serwera (etykieta API)
        time.sleep(1)



    print("Sukces! Dane pogodowe zostały zapisane w ./data/raw_data/weather/")
  