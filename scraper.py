"""
Apify Actor asíncrono en Python para scrapear autónomos de einforma.com
Utiliza Actor y HTTPX para recorrer paginación, extraer IDs y raspar detalles.
Configuración vía INPUT.json: `delay` (segundos) y `max_pages`.
Los resultados se envían al dataset de Apify con `Actor.push_data()`.
"""
import asyncio
import re

from apify import Actor
from httpx import AsyncClient
from bs4 import BeautifulSoup

async def main() -> None:
    async with Actor:
        # Leer configuración de entrada
        input_data = await Actor.get_input() or {}
        delay = float(input_data.get('delay', 1))
        max_pages = input_data.get('max_pages', None)

        # URLs base
        LISTING_URL = (
            "https://www.einforma.com/rapp/resultados-busqueda/autonomos"
            "?type=AUTONOMOS&page={page}"
        )
        DETAIL_URL = "https://www.einforma.com/rapp/ficha/empresas?id={id}"
        HEADERS = {"User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        )}

        Actor.log.info(f"Configuración: delay={delay}s, max_pages={max_pages}")

        company_ids: list[str] = []
        page = 1

        # Cliente HTTP
        async with AsyncClient(headers=HEADERS, timeout=10) as client:
            # Recorrer páginas de listados
            while True:
                if max_pages and page > int(max_pages):
                    break
                url = LISTING_URL.format(page=page)
                Actor.log.info(f"Obteniendo listado: {url}")
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                except Exception as e:
                    Actor.log.error(f"Error en listado página {page}: {e}")
                    break

                soup = BeautifulSoup(resp.text, 'html.parser')
                links = soup.find_all('a', href=re.compile(r'/rapp/ficha/empresas\?id='))
                if not links:
                    Actor.log.info(f"Sin más resultados en página {page}, fin de paginación.")
                    break

                for a in links:
                    href = a.get('href', '')
                    match = re.search(r'id=([^&]+)', href)
                    if match:
                        company_ids.append(match.group(1))

                company_ids = list(set(company_ids))
                Actor.log.info(f"Página {page}: {len(company_ids)} IDs únicos recolectados.")
                page += 1
                await asyncio.sleep(delay)

            # Raspar detalles de cada empresa
            for cid in company_ids:
                detail_url = DETAIL_URL.format(id=cid)
                Actor.log.info(f"Raspeando empresa ID={cid}: {detail_url}")
                try:
                    resp = await client.get(detail_url)
                    resp.raise_for_status()
                except Exception as e:
                    Actor.log.error(f"Error al obtener detalle {cid}: {e}")
                    continue

                soup = BeautifulSoup(resp.text, 'html.parser')
                def extract(label):
                    tag = soup.find('strong', text=re.compile(label))
                    return tag.next_sibling.strip() if tag and tag.next_sibling else None

                data = {
                    'id': cid,
                    'name': extract(r'Denominaci[oó]n'),
                    'cif': extract(r'^CIF$'),
                    'duns': extract(r'Número\s*D-U-N-S'),
                    'cnae': extract(r'Actividad\s*CNAE'),
                    'address': None,
                    'legal_form': extract(r'Forma\s*Jur[ií]dica'),
                }
                dom_tag = soup.find('strong', text='Domicilio Social')
                if dom_tag:
                    link = dom_tag.find_next('a')
                    if link:
                        data['address'] = link.get_text(strip=True)

                # Enviar datos al dataset
                await Actor.push_data(data)
                await asyncio.sleep(delay)

if __name__ == '__main__':
    asyncio.run(main())
