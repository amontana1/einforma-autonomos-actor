"""
Ejemplo de scraper en Python para einforma.com (autónomos)
Usa requests + BeautifulSoup con lógica de reintentos para recorrer automáticamente
la paginación y extraer nombre, CIF, Número D-U-N-S, CNAE, Domicilio Social y Forma Jurídica.
Exporta a Excel o CSV como fallback.
Configura `delay` y `max_pages` en `INPUT.json`.
"""
import json
import os
import re
import time
import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, RequestException
from urllib3.util.retry import Retry

# Cargar configuración desde INPUT.json si existe
input_data = {}
if os.path.exists('INPUT.json'):
    try:
        input_data = json.load(open('INPUT.json', encoding='utf-8'))
    except Exception:
        print("WARNING: No se pudo parsear INPUT.json, usando valores por defecto.")

# Parámetros
DELAY = float(input_data.get('delay', 1))
MAX_PAGES = input_data.get('max_pages', None)

# Iniciar sesión con reintentos

def create_session(retries=3, backoff_factor=0.5, status_forcelist=(500,502,503,504)):
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist,
                  allowed_methods=["HEAD","GET","OPTIONS"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

session = create_session()

# URLs base\NLISTING_URL = (
    "https://www.einforma.com/rapp/resultados-busqueda/autonomos"
    "?type=AUTONOMOS&page={page}"
)
DETAIL_URL = "https://www.einforma.com/rapp/ficha/empresas?id={id}"

# Cabeceras
HEADERS = {"User-Agent": (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
)}

# Realiza petición con reintentos

def get_with_retry(url):
    try:
        resp = session.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return resp
    except Exception as e:
        print(f"Error al conectar con {url}: {e}")
        raise

# Extrae IDs de la paginación

def get_company_ids():
    ids = []
    page = 1
    while True:
        if MAX_PAGES and page > MAX_PAGES:
            break
        url = LISTING_URL.format(page=page)
        try:
            resp = get_with_retry(url)
        except:
            print(f"Fallo persistente en página {page}, deteniendo.")
            break
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.find_all('a', href=re.compile(r'/rapp/ficha/empresas\?id='))
        if not links:
            print(f"Sin resultados en página {page}. Fin.")
            break
        for a in links:
            href = a.get('href', '')
            params = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
            cid = params.get('id', [None])[0]
            if cid:
                ids.append(cid)
        unique = set(ids)
        print(f"Página {page}: {len(unique)} IDs únicos.")
        page += 1
        time.sleep(DELAY)
    return list(set(ids))

# Extrae datos de cada empresa

def parse_company(cid):
    url = DETAIL_URL.format(id=cid)
    try:
        resp = get_with_retry(url)
    except:
        print(f"No pudo obtener detalle {cid}.")
        return {'id': cid}
    soup = BeautifulSoup(resp.text, 'html.parser')
    def get_field(pattern):
        tag = soup.find('strong', text=re.compile(pattern))
        return tag.next_sibling.strip() if tag and tag.next_sibling else None
    data = {
        'id': cid,
        'name': get_field(r'Denominaci[oó]n'),
        'cif': get_field(r'^CIF$'),
        'duns': get_field(r'Número\s*D-U-N-S'),
        'cnae': get_field(r'Actividad\s*CNAE'),
        'legal_form': get_field(r'Forma\s*Jur[ií]dica'),
        'address': None
    }
    dom = soup.find('strong', text='Domicilio Social')
    if dom:
        a = dom.find_next('a')
        data['address'] = a.get_text(strip=True) if a else None
    time.sleep(DELAY)
    return data

# Flujo principal

def main():
    print(f"Delay={DELAY}s, max_pages={MAX_PAGES}")
    ids = get_company_ids()
    print(f"Total IDs: {len(ids)}")
    records = [parse_company(cid) for cid in ids]
    df = pd.DataFrame(records)
    # Exportar
    try:
        with pd.ExcelWriter('empresas.xlsx', engine='xlsxwriter') as w:
            df.to_excel(w, index=False)
        print("Guardado empresas.xlsx")
    except:
        df.to_csv('empresas.csv', index=False)
        print("Guardado empresas.csv")

if __name__=='__main__':
    main()
