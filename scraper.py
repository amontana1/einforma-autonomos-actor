"""
Ejemplo de scraper en Python para einforma.com (autónomos)
Usa requests + BeautifulSoup con lógica de reintentos para recorrer automáticamente
la paginación de autónomos y extraer nombre, CIF, Número D-U-N-S, CNAE,
Domicilio Social y Forma Jurídica, exportando a Excel o CSV como fallback si
no está disponible el motor de Excel.
"""
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, ConnectionError, HTTPError
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib.parse
import re

# Configuración de reintentos para todas las peticiones
def create_session(retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Sesión global con reintentos
session = create_session()

# URLs
LISTING_URL = "https://www.einforma.com/empresas/autonomos?page={page}"
DETAIL_URL = "https://www.einforma.com/rapp/ficha/empresas?id={id}"

# Cabeceras
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
    )
}

def get_with_retry(url, **kwargs):
    try:
        response = session.get(url, headers=HEADERS, timeout=10, **kwargs)
        response.raise_for_status()
        return response
    except (ConnectionError, HTTPError, RequestException) as err:
        print(f"Error al conectar con {url}: {err}")
        raise

def get_company_ids(delay=1, max_pages=None):
    ids = []
    page = 1
    while True:
        if max_pages and page > max_pages:
            break
        url = LISTING_URL.format(page=page)
        try:
            resp = get_with_retry(url)
        except Exception:
            print(f"Fallo persistente en página {page}, deteniendo paginación.")
            break
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = soup.select('a.sc-hAqSLs.gvIQtV, a.company-link')
        if not links:
            print(f"Sin resultados en página {page}. Fin de paginación.")
            break
        for a in links:
            href = a.get('href', '')
            params = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
            cid = params.get('id', [None])[0]
            if cid:
                ids.append(cid)
        print(f"Página {page}: encontrados {len(links)} empresas.")
        page += 1
        time.sleep(delay)
    return list(set(ids))

def parse_company(company_id, delay=1):
    url = DETAIL_URL.format(id=company_id)
    try:
        resp = get_with_retry(url)
    except Exception:
        print(f"No se pudo obtener detalle de empresa {company_id}.")
        return {'id': company_id}
    soup = BeautifulSoup(resp.text, 'html.parser')
    def get_field(label_regex):
        tag = soup.find('strong', text=re.compile(label_regex))
        return tag.next_sibling.strip() if tag else None
    data = {
        'id': company_id,
        'name': get_field(r'Denominaci[oó]n'),
        'cif': get_field(r'^CIF$'),
        'duns': get_field(r'Número\s*D-U-N-S'),
        'cnae': get_field(r'Actividad\s*CNAE'),
        'address': None,
        'legal_form': get_field(r'Forma\s*Jur[ií]dica')
    }
    dom_tag = soup.find('strong', text='Domicilio Social')
    if dom_tag:
        link = dom_tag.find_next('a')
        data['address'] = link.get_text(strip=True) if link else None
    time.sleep(delay)
    return data

def main():
    print("Iniciando extracción de IDs de autónomos...")
    company_ids = get_company_ids()
    print(f"Total de IDs encontrados: {len(company_ids)}")
    records = [parse_company(cid) for cid in company_ids]
    df = pd.DataFrame(records)
    output_excel = 'autonomos_einforma.xlsx'
    try:
        with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        print(f"Datos guardados en Excel: {output_excel}")
    except (ModuleNotFoundError, ImportError) as e:
        output_csv = 'autonomos_einforma.csv'
        df.to_csv(output_csv, index=False)
        print(f"Motor de Excel no disponible ({e}), datos guardados en CSV: {output_csv}")

if __name__ == '__main__':
    main()
