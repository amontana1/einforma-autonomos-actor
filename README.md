# Einforma Autónomos Scraper

Este actor raspa perfiles de autónomos en einforma.com, extrayendo:
- Nombre
- CIF
- Número D-U-N-S
- CNAE
- Domicilio Social
- Forma Jurídica

## Uso local

```bash
python scraper.py
```

## Subida a Apify

1. Asegúrate de tener un repo Git con estos archivos.
2. Push al remoto GitHub.
3. Conecta el repo como actor en Apify (GitHub integration).
4. Configura `actor.json` en la UI.
