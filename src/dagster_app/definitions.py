"""
Definitions Dagster – Pipeline de Secciones Censales S/C de Tenerife
Sensor MD5 + Job completo.
"""

import hashlib
from pathlib import Path

from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
    sensor,
    RunRequest,
    DefaultSensorStatus,
    define_asset_job,
    AssetSelection,
)

from src.dagster_app.assets import renta_secciones_ia, checks_secciones

all_assets = load_assets_from_modules([renta_secciones_ia])
all_checks = load_asset_checks_from_modules([checks_secciones])

pipeline_secciones = define_asset_job(
    name="pipeline_secciones_tenerife",
    selection=AssetSelection.all()
)

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


def _hash_carpeta(path: Path) -> str:
    h = hashlib.md5()
    for f in sorted(path.rglob("*")):
        if f.is_file() and f.suffix not in (".pyc", ".tmp"):
            h.update(str(f).encode())
            h.update(str(f.stat().st_mtime).encode())
    return h.hexdigest()


@sensor(
    job=pipeline_secciones,
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
    description="Lanza el pipeline completo cuando cambia algún archivo en data/"
)
def sensor_cambio_datos(context):
    if not DATA_DIR.exists():
        context.log.warning(f"Carpeta data/ no encontrada: {DATA_DIR}")
        return

    nuevo_hash = _hash_carpeta(DATA_DIR)
    ultimo_hash = context.cursor or ""

    if nuevo_hash != ultimo_hash:
        context.update_cursor(nuevo_hash)
        context.log.info("Cambio en data/ detectado – lanzando pipeline")
        yield RunRequest(run_key=nuevo_hash)
    else:
        context.log.info("Sin cambios en data")


defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[pipeline_secciones],
    sensors=[sensor_cambio_datos]
)