#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import time
import re
from pathlib import Path

import pandas as pd
import requests


def norm_text(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ''
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.rstrip(',')
    return s


def status_norm(x) -> str:
    return norm_text(x).upper()


def is_missing_num(x) -> bool:
    return x is None or pd.isna(x)


def build_q(row: dict) -> str:
    q = norm_text(row.get('address'))
    if q:
        return q
    street = norm_text(row.get('street'))
    city = norm_text(row.get('city'))
    country = norm_text(row.get('country'))
    postal = norm_text(row.get('postal_code'))
    return ", ".join([p for p in [street, f"{postal} {city}".strip(), country] if p])


def make_key(row: dict, c_col: str, p_col: str, ci_col: str, s_col: str) -> str:
    c = norm_text(row.get(c_col)).lower()
    p = norm_text(row.get(p_col)).lower()
    ci = norm_text(row.get(ci_col)).lower()
    s = norm_text(row.get(s_col)).lower()
    return f"{c}|{p}|{ci}|{s}"


def nominatim_search(session: requests.Session, base_url: str, user_agent: str, params: dict, timeout: int = 30):
    url = base_url.rstrip('/') + '/search'
    headers = {'User-Agent': user_agent, 'Accept': 'application/json'}
    r = session.get(url, params=params, headers=headers, timeout=timeout)
    if r.status_code != 200:
        return None, None, f"HTTP_{r.status_code}"
    data = r.json()
    if not data:
        return None, None, 'NOT_FOUND'
    lon = data[0].get('lon')
    lat = data[0].get('lat')
    if lon is None or lat is None:
        return None, None, 'NOT_FOUND'
    try:
        return float(lon), float(lat), 'OK'
    except Exception:
        return None, None, 'NOT_FOUND'


def nominatim_structured(session, base_url, user_agent, row, timeout):
    params = {
        'format': 'jsonv2',
        'limit': 1,
        'street': norm_text(row.get('street')),
        'city': norm_text(row.get('city')),
        'country': norm_text(row.get('country')),
        'postalcode': norm_text(row.get('postal_code')),
    }
    return nominatim_search(session, base_url, user_agent, params, timeout)


def nominatim_q(session, base_url, user_agent, row, timeout):
    params = {
        'format': 'jsonv2',
        'limit': 1,
        'q': build_q(row),
    }
    return nominatim_search(session, base_url, user_agent, params, timeout)


def google_geocode(session: requests.Session, api_key: str, address: str, timeout: int = 30):
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    params = {'address': address, 'key': api_key}
    r = session.get(url, params=params, timeout=timeout)
    if r.status_code != 200:
        return None, None, f"HTTP_{r.status_code}"
    data = r.json()
    g_status = data.get('status', 'UNKNOWN')
    if g_status != 'OK':
        return None, None, f"GOOGLE_{g_status}"
    results = data.get('results') or []
    if not results:
        return None, None, 'GOOGLE_ZERO_RESULTS'
    loc = results[0].get('geometry', {}).get('location', {})
    lat = loc.get('lat')
    lon = loc.get('lng')
    if lat is None or lon is None:
        return None, None, 'GOOGLE_BAD_RESPONSE'
    return float(lon), float(lat), 'OK_GOOGLE'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--orders', required=True, help='Вхідний файл із замовленнями (Excel)')
    ap.add_argument('--dict', required=True, help='Вихідний файл-словник з адресами (Excel)')
    ap.add_argument('--orders-out', default=None, help='Файл для збереження замовлень (за замовчуванням перезапише --orders)')
    ap.add_argument('--dict-out', default=None, help='Файл для збереження словника (за замовчуванням перезапише --dict)')
    ap.add_argument('--server', default='https://nominatim.openstreetmap.org', help='Nominatim base URL')
    ap.add_argument('--user-agent', required=True, help='Custom User-Agent (required by Nominatim policy)')
    ap.add_argument('--delay', type=float, default=1.0, help='Min delay between Nominatim requests (>=1.0 recommended)')
    ap.add_argument('--timeout', type=int, default=30)
    ap.add_argument('--max', type=int, default=0, help='Process at most N eligible rows (0=all)')
    args = ap.parse_args()

    google_key = os.getenv('GOOGLE_MAPS_API_KEY', '').strip() or None

    orders_in_path = Path(args.orders)
    orders_out_path = Path(args.orders_out) if args.orders_out else orders_in_path
    dict_in_path = Path(args.dict)
    dict_out_path = Path(args.dict_out) if args.dict_out else dict_in_path

    print("Завантаження даних...")
    df_orders = pd.read_excel(orders_in_path, engine='openpyxl')
    
    # Перевірка наявності потрібних колонок у файлі замовлень
    for col in ['Заказчик страна', 'Индекс', 'Заказчик город', 'Заказчик улица']:
        if col not in df_orders.columns:
            df_orders[col] = pd.NA

    # Створення lon/lat в замовленнях, якщо їх немає
    if 'lon' not in df_orders.columns:
        df_orders['lon'] = pd.NA
    if 'lat' not in df_orders.columns:
        df_orders['lat'] = pd.NA

    if dict_in_path.exists():
        df_dict = pd.read_excel(dict_in_path, engine='openpyxl')
    else:
        df_dict = pd.DataFrame(columns=['country', 'postal_code', 'city', 'street', 'address', 'lon', 'lat', 'status'])

    for col in ['country', 'postal_code', 'city', 'street', 'address', 'lon', 'lat', 'status']:
        if col not in df_dict.columns:
            df_dict[col] = pd.NA

    # Зміна 2: Синхронізація словника з новими адресами із замовлень
    dict_keys = set()
    for _, row in df_dict.iterrows():
        dict_keys.add(make_key(row.to_dict(), 'country', 'postal_code', 'city', 'street'))

    new_rows = []
    for _, row in df_orders.iterrows():
        row_dict = row.to_dict()
        k = make_key(row_dict, 'Заказчик страна', 'Индекс', 'Заказчик город', 'Заказчик улица')
        if k not in dict_keys:
            new_rows.append({
                'country': norm_text(row_dict.get('Заказчик страна')),
                'postal_code': norm_text(row_dict.get('Индекс')),
                'city': norm_text(row_dict.get('Заказчик город')),
                'street': norm_text(row_dict.get('Заказчик улица')),
                'address': '', 
                'lon': pd.NA,
                'lat': pd.NA,
                'status': ''
            })
            dict_keys.add(k)

    if new_rows:
        print(f"Додано {len(new_rows)} нових унікальних адрес до словника.")
        df_dict = pd.concat([df_dict, pd.DataFrame(new_rows)], ignore_index=True)

    # Підготовка до геокодування
    df_dict['lon'] = pd.to_numeric(df_dict['lon'], errors='coerce').astype('Float64')
    df_dict['lat'] = pd.to_numeric(df_dict['lat'], errors='coerce').astype('Float64')
    df_dict['status'] = df_dict['status'].astype('object')

    st = df_dict['status'].apply(status_norm)
    # Відбираємо лише ті, де немає координат І статус порожній або NOT_FOUND
    eligible = (st.eq('') | st.eq('NOT_FOUND')) & (df_dict['lon'].isna() | df_dict['lat'].isna())
    idxs = df_dict.index[eligible].tolist()
    
    if args.max and args.max > 0:
        idxs = idxs[:args.max]

    if idxs:
        print(f"Розпочинаю геокодування {len(idxs)} записів у словнику...")
        nom_sess = requests.Session()
        g_sess = requests.Session()
        last_nom_call = 0.0

        def nom_wait():
            nonlocal last_nom_call
            now = time.time()
            wait = (last_nom_call + max(args.delay, 1.0)) - now
            if wait > 0:
                time.sleep(wait)
            last_nom_call = time.time()

        processed = 0
        for idx in idxs:
            row = df_dict.loc[idx].to_dict()
            current_status = status_norm(row.get('status'))
            address_for_google = build_q(row)

            lon = lat = None
            final_status = None
            path = []

            try:
                if current_status == '':
                    nom_wait()
                    lon, lat, s1 = nominatim_structured(nom_sess, args.server, args.user_agent, row, args.timeout)
                    path.append('STRUCT')
                    if s1 == 'OK':
                        final_status = 'OK'
                    elif s1.startswith('HTTP_'):
                        final_status = s1
                    else:
                        nom_wait()
                        lon, lat, s2 = nominatim_q(nom_sess, args.server, args.user_agent, row, args.timeout)
                        path.append('Q')
                        if s2 == 'OK':
                            final_status = 'OK_Q'
                        elif s2.startswith('HTTP_'):
                            final_status = s2
                        else:
                            final_status = 'NOT_FOUND'
                else:
                    nom_wait()
                    lon, lat, s2 = nominatim_q(nom_sess, args.server, args.user_agent, row, args.timeout)
                    path.append('Q')
                    if s2 == 'OK':
                        final_status = 'OK_Q'
                    elif s2.startswith('HTTP_'):
                        final_status = s2
                    else:
                        final_status = 'NOT_FOUND'

                if final_status == 'NOT_FOUND' and google_key and address_for_google:
                    lon3, lat3, s3 = google_geocode(g_sess, google_key, address_for_google, timeout=args.timeout)
                    path.append('GOOGLE')
                    if s3 == 'OK_GOOGLE':
                        lon, lat = lon3, lat3
                        final_status = 'OK_GOOGLE'
                    else:
                        final_status = s3 if s3.startswith('GOOGLE_') else 'NOT_FOUND'

            except Exception:
                final_status = 'ERROR'

            if lon is not None and lat is not None:
                if is_missing_num(df_dict.at[idx, 'lon']) or is_missing_num(df_dict.at[idx, 'lat']):
                    df_dict.at[idx, 'lon'] = lon
                    df_dict.at[idx, 'lat'] = lat

            df_dict.at[idx, 'status'] = final_status

            processed += 1
            print(f"[{processed}/{len(idxs)}] ({'->'.join(path)}) {row.get('address','')} -> {df_dict.at[idx,'lon']},{df_dict.at[idx,'lat']} status={df_dict.at[idx,'status']}")
    else:
        print("Немає нових адрес для геокодування у словнику.")

    # Збереження словника
    with pd.ExcelWriter(dict_out_path, engine='openpyxl') as writer:
        df_dict.to_excel(writer, index=False)
    print(f"Словник збережено: {dict_out_path}")

    # Зміна 1: Мапінг знайдених координат назад у файл замовлень
    print("Оновлення файлу замовлень...")
    lookup = {}
    for _, row in df_dict.iterrows():
        row_dict = row.to_dict()
        st_val = status_norm(row_dict.get('status'))
        if st_val.startswith('OK') and not is_missing_num(row_dict.get('lon')) and not is_missing_num(row_dict.get('lat')):
            k = make_key(row_dict, 'country', 'postal_code', 'city', 'street')
            lookup[k] = (row_dict['lon'], row_dict['lat'])

    updated_orders = 0
    for idx in df_orders.index:
        row_dict = df_orders.loc[idx].to_dict()
        k = make_key(row_dict, 'Заказчик страна', 'Индекс', 'Заказчик город', 'Заказчик улица')
        if k in lookup:
            df_orders.at[idx, 'lon'] = lookup[k][0]
            df_orders.at[idx, 'lat'] = lookup[k][1]
            updated_orders += 1

    with pd.ExcelWriter(orders_out_path, engine='openpyxl') as writer:
        df_orders.to_excel(writer, index=False)
        
    print(f"Готово. Оновлено {updated_orders} рядків у файлі замовлень: {orders_out_path}")


if __name__ == '__main__':
    main()

