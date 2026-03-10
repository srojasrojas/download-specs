# download-specs

Descarga masiva de fichas técnicas de producto a partir del Excel exportado desde el sistema de control de calidad.

## Requisitos

- Python 3.9+
- Instalar dependencias:

```bash
pip install -r requirements.txt
```

## Uso

Coloca el archivo `exported_specs_YYYY-MM-DD.xlsx` en esta carpeta. El script toma automáticamente el más reciente.

```bash
# Descarga todo en specs/ (sin subcarpetas)
python download_specs.py

# Organizado por país
python download_specs.py --subfolder countryName

# Organizado por proveedor
python download_specs.py --subfolder providerName
```

## Opciones

| Argumento | Default | Descripción |
|-----------|---------|-------------|
| `--subfolder COLUMN` | _(ninguno)_ | Columna usada para crear subcarpetas dentro de `--output` |
| `--output DIR` | `specs` | Carpeta raíz donde se guardan los archivos |
| `--delay SECONDS` | `2.0` | Pausa en segundos entre descargas |
| `--test N` | _(desactivado)_ | Descarga solo las primeras N specs (para probar) |
| `--no-skip` | _(desactivado)_ | Re-descarga archivos aunque ya existan localmente |

## Columnas disponibles para `--subfolder`

`countryCode`, `countryName`, `productCode`, `productVIN`, `productSKU`, `providerCode`, `providerName`

## Notas

- El script **deduplica** las URLs antes de descargar (el Excel tiene ~205K filas pero ~112K specs únicas).
- Es **reanudable**: si se interrumpe, al volver a correr salta los archivos ya descargados.
- Los archivos fallidos se reportan al final como `Failed: N` sin detener el proceso.
