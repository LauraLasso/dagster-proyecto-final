"""
Pipeline DataOps – Renta y Actividad por Secciones Censales
Provincia de Santa Cruz de Tenerife, 2021-2023
Autor: Laura Lasso García

CORRECCIONES v2:
- _leer_csv(): detección automática de separador (;  ,  \t) + utf-8-sig
- cleaned_*:  renombrado por nombre de columna detectada, no por posición
- bool() explícito en todos los AssetCheckResult.passed
"""

import os, re, hashlib, subprocess
import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path

from dagster import (
    asset, Output, MetadataValue, get_dagster_logger
)

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR  = BASE_DIR / "data"
OUT_DIR   = BASE_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

LLM_URL     = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
LLM_MODEL   = "ollama/llama3.1:8b"
LLM_HEADERS = {"Content-Type": "application/json", "Authorization": "Bearer sk-1234"}


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDAD: lectura robusta de CSV con detección de separador
# ─────────────────────────────────────────────────────────────────────────────

def _leer_csv(path: Path) -> pd.DataFrame:
    """
    Lee un CSV detectando automáticamente el separador (;  ,  \t).
    Acepta codificaciones utf-8-sig (BOM) y latin-1.
    Devuelve un DataFrame con columnas en minúsculas sin espacios.
    """
    logger = get_dagster_logger()
    path   = Path(path)

    enc_detectada = "utf-8-sig"
    primera = ""

    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            primera = path.read_text(encoding=enc).splitlines()[0]
            enc_detectada = enc
            break
        except Exception:
            continue

    candidatos = {
        ";": primera.count(";"),
        ",": primera.count(","),
        "\t": primera.count("\t"),
    }
    sep = max(candidatos, key=candidatos.get)
    if candidatos[sep] == 0:
        sep = ";"

    logger.info(f"Leyendo {path.name} | sep={repr(sep)} | enc={enc_detectada}")

    for enc2 in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            df = pd.read_csv(
                path,
                sep=sep,
                encoding=enc2,
                dtype=str,
                engine="python",
                on_bad_lines="skip"
            )
            df.columns = (
                df.columns.str.strip()
                .str.lower()
                .str.replace(r"\s+", "_", regex=True)
                .str.replace(r"[áàäâ]", "a", regex=True)
                .str.replace(r"[éèëê]", "e", regex=True)
                .str.replace(r"[íìïî]", "i", regex=True)
                .str.replace(r"[óòöô]", "o", regex=True)
                .str.replace(r"[úùüû]", "u", regex=True)
                .str.replace(r"[ñ]", "n", regex=True)
            )
            logger.info(f"  → {len(df)} filas, {len(df.columns)} columnas: {list(df.columns)}")
            return df
        except Exception as e:
            logger.warning(f"  Fallo con enc={enc2}: {e}")

    raise RuntimeError(f"No se pudo leer {path}")


def _primera_col_que_contiene(df: pd.DataFrame, *fragmentos) -> str:
    """Devuelve la primera columna cuyo nombre contiene alguno de los fragmentos."""
    for frag in fragmentos:
        for col in df.columns:
            if frag in col:
                return col
    return df.columns[0]


def _to_numeric(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(
        serie.astype(str).str.replace(",", ".").str.strip(),
        errors="coerce"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. RAW
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="raw")
def raw_renta_media():
    """Renta bruta media y mediana por secciones 2021-2023 (ISTAC E30325A_000009)."""
    df = _leer_csv(DATA_DIR / "rentamedia-sc-3.csv")
    return Output(df, metadata={"rows": len(df), "columns": list(df.columns)})


@asset(group_name="raw")
def raw_distribucion_ingresos():
    """Distribución de renta según fuente de ingresos por secciones (ISTAC E30325A_000002)."""
    df = _leer_csv(DATA_DIR / "distribucion-renta-ingresos.csv")
    return Output(df, metadata={"rows": len(df), "columns": list(df.columns)})


@asset(group_name="raw")
def raw_actividad():
    """Relación con la actividad por secciones 2021-2023 (INE 66796)."""
    df = _leer_csv(DATA_DIR / "actividad-sc-3.csv")
    return Output(df, metadata={"rows": len(df), "columns": list(df.columns)})


@asset(group_name="raw")
def raw_ocupacion():
    """Sector de ocupación por secciones 2021-2023 (INE 70125)."""
    df = _leer_csv(DATA_DIR / "ocupacion-sc-3.csv")
    return Output(df, metadata={"rows": len(df), "columns": list(df.columns)})


@asset(group_name="raw")
def raw_geojson_2022():
    path = DATA_DIR / "cartografia-secciones/secciones_20220101_tenerife.json"
    gdf  = gpd.read_file(path)
    return Output(gdf, metadata={"features": len(gdf), "crs": str(gdf.crs)})


@asset(group_name="raw")
def raw_geojson_2023():
    path = DATA_DIR / "cartografia-secciones/secciones_20230101_tenerife.json"
    gdf  = gpd.read_file(path)
    return Output(gdf, metadata={"features": len(gdf)})


@asset(group_name="raw")
def raw_geojson_2024():
    path = DATA_DIR / "cartografia-secciones/secciones_20240101_tenerife.json"
    gdf  = gpd.read_file(path)
    return Output(gdf, metadata={"features": len(gdf)})


@asset(group_name="raw")
def raw_relacion_actividad():
    """
    Reutiliza actividad-sc-3.csv como proxy de relación con la actividad.
    (El CSV de INE 66796 no está disponible localmente.)
    """
    df = _leer_csv(DATA_DIR / "actividad-sc-3.csv")
    get_dagster_logger().info("raw_relacion_actividad: usando actividad-sc-3.csv como fuente")
    return Output(df, metadata={"rows": len(df), "columns": list(df.columns),
                                 "fichero": "actividad-sc-3.csv"})


# ─────────────────────────────────────────────────────────────────────────────
# 2. CLEANED
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="cleaned")
def cleaned_renta_media(raw_renta_media):
    """
    Filtra RENTA_BRUTA_MEDIA_HOGAR y RENTA_NETA_UNIDAD_CONSUMO_MEDIANA,
    luego pivota para tener una fila por (TERRITORIO_CODE, año).
    """
    df = raw_renta_media.copy()
    logger = get_dagster_logger()
    logger.info(f"cleaned_renta_media columnas: {list(df.columns)}")

    # Normalizar nombre de columna 'año' (puede venir con BOM como 'año')
    df.columns = df.columns.str.replace(r"a[ñn]o", "anio", regex=True)

    df["obs"] = _to_numeric(df["obs_value"])
    df["anio"] = _to_numeric(df["anio"]).astype("Int64")
    df = df.dropna(subset=["anio", "obs", "territorio_code"])

    media   = df[df["medidas_code"] == "RENTA_BRUTA_MEDIA_HOGAR"][
        ["territorio_code", "anio", "obs"]
    ].rename(columns={"obs": "renta_media"})

    mediana = df[df["medidas_code"] == "RENTA_NETA_UNIDAD_CONSUMO_MEDIANA"][
        ["territorio_code", "anio", "obs"]
    ].rename(columns={"obs": "renta_mediana"})

    merged = media.merge(mediana, on=["territorio_code", "anio"], how="left")
    merged = merged.rename(columns={"territorio_code": "seccion"})
    merged["anio"] = merged["anio"].astype(int)

    logger.info(f"  → {len(merged)} filas, años: {sorted(merged['anio'].unique().tolist())}")
    return Output(
        merged,
        metadata={"rows_cleaned": len(merged), "anios": sorted(merged["anio"].unique().tolist())}
    )


@asset(group_name="cleaned")
def cleaned_distribucion_ingresos(raw_distribucion_ingresos):
    """
    La fuente de ingresos viene en la columna 'MEDIDAS#es' (→ 'medidas#es' tras normalizar).
    El valor es OBS_VALUE. El identificador de sección es TERRITORIO_CODE.
    """
    df = raw_distribucion_ingresos.copy()
    logger = get_dagster_logger()
    logger.info(f"cleaned_distribucion_ingresos columnas: {list(df.columns)}")

    # 'año' puede tener BOM
    df.columns = df.columns.str.replace(r"a[ñn]o", "anio", regex=True)

    # Detectar columna de la etiqueta de medida (tiene '#' en el nombre)
    col_fuente = next((c for c in df.columns if "#" in c or "medidas" in c and "code" not in c), None)
    if col_fuente is None:
        col_fuente = _primera_col_que_contiene(df, "fuente", "tipo", "concepto", "medidas")

    col_valor = _primera_col_que_contiene(df, "obs_value", "obs", "valor", "importe")
    col_sec   = _primera_col_que_contiene(df, "territorio_code", "seccion", "geocode", "cusec")

    df = df.rename(columns={
        col_fuente: "fuente",
        col_valor:  "importe",
        col_sec:    "seccion",
    })

    df["anio"]    = _to_numeric(df["anio"]).astype("Int64")
    df["importe"] = _to_numeric(df["importe"])
    df = df.dropna(subset=["seccion", "fuente", "importe"])
    df["anio"] = df["anio"].astype(int)

    logger.info(f"  → {len(df)} filas, fuentes: {list(df['fuente'].unique())}")
    return Output(
        df,
        metadata={"rows_cleaned": len(df), "fuentes": list(df["fuente"].unique())}
    )


@asset(group_name="cleaned")
def cleaned_actividad(raw_actividad):
    """
    Columnas reales: 'Actividad económica' (sector), 'geocode' (CUSEC completo),
    'num_casos', 'Periodo' (año), 'Sexo'.
    Agrupamos por geocode + Periodo + Actividad sumando ambos sexos.
    """
    df = raw_actividad.copy()
    logger = get_dagster_logger()
    logger.info(f"cleaned_actividad columnas: {list(df.columns)}")

    # Normalizar
    col_actividad = _primera_col_que_contiene(df, "actividad", "relacion", "situacion", "estado")
    col_geocode   = _primera_col_que_contiene(df, "geocode", "territorio_code", "cusec", "seccion")
    col_anio      = _primera_col_que_contiene(df, "periodo", "anio", "ano", "year")
    col_personas  = _primera_col_que_contiene(df, "num_casos", "personas", "total", "valor")

    df = df.rename(columns={
        col_actividad: "relacion_actividad",
        col_geocode:   "seccion",
        col_anio:      "anio",
        col_personas:  "personas",
    })

    # Dentro de cleaned_actividad, después del rename:
    df["seccion"] = df["seccion"].astype(str).apply(
        lambda g: f"{g.split('_')[1]}{g.split('_')[2][1:]}{g.split('_')[3][1:]}"
        if "_" in str(g) else g
    )

    df["anio"]     = _to_numeric(df["anio"]).astype("Int64")
    df["personas"] = _to_numeric(df["personas"])
    df = df.dropna(subset=["seccion", "relacion_actividad", "personas"])

    # Agregar ambos sexos
    df = (
        df.groupby(["seccion", "anio", "relacion_actividad"], as_index=False)["personas"]
        .sum()
    )
    df["anio"] = df["anio"].astype(int)

    logger.info(f"  → {len(df)} filas, categorías: {list(df['relacion_actividad'].unique())}")
    return Output(
        df,
        metadata={"rows_cleaned": len(df), "categorias": list(df["relacion_actividad"].unique())}
    )


@asset(group_name="cleaned")
def cleaned_ocupacion(raw_ocupacion):
    df = raw_ocupacion.copy()
    logger = get_dagster_logger()

    # _leer_csv normaliza 'año' → 'ano'. Hay que renombrar 'ano' → 'anio'
    df = df.rename(columns={"ano": "anio"})
    # También cubrir si vino como 'año' sin normalizar
    df.columns = df.columns.str.replace(r"a[ñn]o", "anio", regex=True)

    col_sector   = _primera_col_que_contiene(df, "ocupacion", "sector", "rama")
    col_geocode  = _primera_col_que_contiene(df, "geocode", "territorio_code", "cusec")
    col_personas = _primera_col_que_contiene(df, "num_casos", "personas", "total", "valor")

    df = df.rename(columns={
        col_sector:   "sector",
        col_geocode:  "geocode_raw",
        col_personas: "personas",
    })

    df["seccion"] = df["geocode_raw"].astype(str).str.split("_", n=1).str[1]

    df["anio"]     = _to_numeric(df["anio"]).astype("Int64")
    df["personas"] = _to_numeric(df["personas"])
    df = df.dropna(subset=["seccion", "sector", "personas"])
    df = df.groupby(["seccion", "anio", "sector"], as_index=False)["personas"].sum()
    df["anio"] = df["anio"].astype(int)

    logger.info(f"  → {len(df)} filas, anios: {sorted(df['anio'].unique())}, seccion ejemplo: '{df['seccion'].iloc[0]}'")
    return Output(df, metadata={
        "rows_cleaned": len(df),
        "sectores": list(df["sector"].unique()),
        "seccion_ejemplo": df["seccion"].iloc[0]
    })


@asset(group_name="cleaned")
def cleaned_relacion_actividad(raw_relacion_actividad):
    df = raw_relacion_actividad.copy()
    logger = get_dagster_logger()

    # Normalizar 'año/Periodo' → 'anio'
    df.columns = df.columns.str.replace(r"a[ñn]o", "anio", regex=True)
    # df.columns = df.columns.str.replace(r"periodo", "anio", regex=True)

    # Al inicio de cleaned_relacion_actividad, antes de _primera_col_que_contiene:
    df = df.rename(columns={"ano": "anio", "periodo": "anio"})

    col_cat      = _primera_col_que_contiene(df, "actividad", "relacion", "situacion")
    col_geocode  = _primera_col_que_contiene(df, "geocode", "territorio_code", "cusec")
    col_personas = _primera_col_que_contiene(df, "num_casos", "personas", "total", "valor")
    col_anio     = _primera_col_que_contiene(df, "anio", "ano", "year", "periodo")

    df = df.rename(columns={
        col_cat:      "relacion_actividad",
        col_geocode:  "geocode_raw",
        col_personas: "personas",
        col_anio:     "anio",
    })

    # Extraer CUSEC del geocode: "20210101_38001_D01_S001" → "3800101001"
    def _extraer_cusec(g: str) -> str:
        try:
            p = str(g).split("_")
            return f"{p[1]}{p[2][1:]}{p[3][1:]}"
        except Exception:
            return g

    df["seccion"]  = df["geocode_raw"].astype(str).apply(_extraer_cusec)
    df["anio"]     = _to_numeric(df["anio"]).astype("Int64")
    df["personas"] = _to_numeric(df["personas"])
    df = df.dropna(subset=["seccion", "relacion_actividad", "personas"])
    df = df.groupby(["seccion", "anio", "relacion_actividad"], as_index=False)["personas"].sum()
    df["anio"] = df["anio"].astype(int)

    logger.info(f"  → {len(df)} filas, categorías: {list(df['relacion_actividad'].unique())}")
    return Output(df, metadata={"rows": len(df),
                                 "categorias": list(df["relacion_actividad"].unique())})

# ─────────────────────────────────────────────────────────────────────────────
# 3. VIZ_DATA
# ─────────────────────────────────────────────────────────────────────────────

@asset(group_name="viz_data")
def viz_renta_tendencia(cleaned_renta_media):
    df = (
        cleaned_renta_media
        .groupby("anio")["renta_media"]
        .agg(["mean", "median", "std"])
        .reset_index()
    )
    df.columns = ["anio", "media_provincial", "mediana_provincial", "desv_std"]
    df["desv_std"] = df["desv_std"].fillna(0)
    return Output(df, metadata={"years": list(df["anio"])})


@asset(group_name="viz_data")
def viz_distribucion_ingresos(cleaned_distribucion_ingresos):
    df = (
        cleaned_distribucion_ingresos
        .groupby(["anio", "fuente"])["importe"]
        .sum()
        .reset_index()
    )
    total = df.groupby("anio")["importe"].transform("sum")
    df["porcentaje"] = (df["importe"] / total * 100).round(2)
    return Output(df, metadata={"fuentes": list(df["fuente"].unique())})


@asset(group_name="viz_data")
def viz_actividad_distribucion(cleaned_actividad):
    df = (
        cleaned_actividad
        .groupby(["anio", "relacion_actividad"])["personas"]
        .sum()
        .reset_index()
    )
    total = df.groupby("anio")["personas"].transform("sum")
    df["porcentaje"] = (df["personas"] / total * 100).round(2)
    return Output(df, metadata={"categorias": list(df["relacion_actividad"].unique())})


@asset(group_name="viz_data")
def viz_ocupacion_distribucion(cleaned_ocupacion):
    df = (
        cleaned_ocupacion
        .groupby(["anio", "sector"])["personas"]
        .sum()
        .reset_index()
    )
    total = df.groupby("anio")["personas"].transform("sum")
    df["porcentaje"] = (df["personas"] / total * 100).round(2)
    return Output(df, metadata={"sectores": list(df["sector"].unique())})



def _col_cusec(gdf) -> str:
    """Detecta el nombre real de la columna con el código de sección censal."""
    for candidato in ("CUSEC", "cusec", "NATCODE", "natcode", "COD_SEC", "geocode"):
        if candidato in gdf.columns:
            return candidato
    # fallback: primera columna que parezca un código de 18 chars
    for col in gdf.columns:
        muestra = gdf[col].dropna().astype(str)
        if muestra.str.len().mode()[0] >= 10:
            return col
    return gdf.columns[0]

@asset(group_name="viz_data")
def viz_geo_renta_2021(cleaned_renta_media, raw_geojson_2022):
    df  = cleaned_renta_media[cleaned_renta_media["anio"] == 2021].copy()
    col = _col_cusec(raw_geojson_2022)
    gdf = raw_geojson_2022.merge(df, left_on=col, right_on="seccion", how="left")
    return Output(gdf, metadata={"col_join": col, "features_con_renta": int(gdf["renta_media"].notna().sum())})

@asset(group_name="viz_data")
def viz_geo_renta_2022(cleaned_renta_media, raw_geojson_2023):
    df  = cleaned_renta_media[cleaned_renta_media["anio"] == 2022].copy()
    col = _col_cusec(raw_geojson_2023)
    gdf = raw_geojson_2023.merge(df, left_on=col, right_on="seccion", how="left")
    return Output(gdf, metadata={"col_join": col, "features_con_renta": int(gdf["renta_media"].notna().sum())})


@asset(group_name="viz_data")
def viz_geo_renta_2023(cleaned_renta_media, raw_geojson_2024):
    df  = cleaned_renta_media[cleaned_renta_media["anio"] == 2023].copy()
    col = _col_cusec(raw_geojson_2024)
    gdf = raw_geojson_2024.merge(df, left_on=col, right_on="seccion", how="left")
    return Output(gdf, metadata={"col_join": col, "features_con_renta": int(gdf["renta_media"].notna().sum())})

@asset(group_name="viz_data")
def viz_geo_ocupacion_2023(cleaned_ocupacion, raw_geojson_2024):
    logger = get_dagster_logger()

    df = cleaned_ocupacion[cleaned_ocupacion["anio"] == 2023].copy()
    df_top = df.groupby(["seccion", "sector"])["personas"].sum().reset_index()
    df_top = df_top.loc[df_top.groupby("seccion")["personas"].idxmax()].copy()

    gdf = raw_geojson_2024.copy()
    # GeoJSON: "20240101_38001_D01_S001" → "38001_D01_S001"
    gdf["_k"] = gdf["geocode"].astype(str).str.split("_", n=1).str[1]

    logger.info(f"GeoJSON _k ejemplo: '{gdf['_k'].iloc[0]}'")
    logger.info(f"df_top seccion ejemplo: '{df_top['seccion'].iloc[0]}'")

    merged = gdf.merge(df_top, left_on="_k", right_on="seccion", how="left")
    n_ok = int(merged["sector"].notna().sum())
    logger.info(f"Secciones con sector: {n_ok}/{len(merged)}")

    return Output(merged, metadata={
        "con_sector": n_ok,
        "total": len(merged),
        "pct_join": round(n_ok / len(merged) * 100, 1)
    })

@asset(group_name="viz_data")
def viz_renta_vs_ocupacion(cleaned_renta_media, cleaned_ocupacion):
    """Relación entre renta media y % ocupados por sección y año."""
    renta = cleaned_renta_media[["seccion", "anio", "renta_media"]]
    ocup  = (cleaned_ocupacion
             .groupby(["seccion", "anio"])["personas"].sum()
             .reset_index()
             .rename(columns={"personas": "total_ocupados"}))
    df = renta.merge(ocup, on=["seccion", "anio"], how="inner")
    return Output(df, metadata={"rows": len(df)})

@asset(group_name="viz_data")
def viz_relacion_actividad(cleaned_relacion_actividad):
    """Distribución porcentual Ocupado/Parado/Inactivo por año."""
    df = cleaned_relacion_actividad.groupby(["anio", "relacion_actividad"])["personas"].sum().reset_index()
    total = df.groupby("anio")["personas"].transform("sum")
    df["porcentaje"] = (df["personas"] / total * 100).round(2)
    return Output(df, metadata={"categorias": list(df["relacion_actividad"].unique())})



# ─────────────────────────────────────────────────────────────────────────────
# 4. TEMPLATES DE PROMPTS PARA IA
# ─────────────────────────────────────────────────────────────────────────────

TEMPLATE_TECNICO = """
def generarplot(df):
    from plotnine import *
    plot = ggplot(df, aes(...))
    ...
    return plot
"""

SYSTEM_BASE = (
    "Eres un experto en Plotnine. "
    "Devuelve EXCLUSIVAMENTE el bloque def generarplot(df): sin ningún import, "
    "sin texto antes ni después, sin bloques ```python. "
    "NUNCA pongas imports dentro de la función. "
    "NUNCA uses theme_minimal(figure_size=...). Usa theme_minimal() + theme(figure_size=(...)) separados. "
    f"Usa SIEMPRE este template:\n{TEMPLATE_TECNICO}\n"
)


@asset(group_name="ia_templates")
def template_ia_tendencia_renta(viz_renta_tendencia):
    columnas = ", ".join(viz_renta_tendencia.columns)
    years    = sorted(viz_renta_tendencia["anio"].unique().tolist())
    descripcion = f"""
- Dataset: viz_renta_tendencia con columnas: {columnas}
- Años disponibles: {years}
- Estéticas (aes): anio → eje X (cuantitativa discreta), media_provincial → eje Y.
- Geometrías:
    geom_ribbon(aes(ymin=media_provincial - desv_std, ymax=media_provincial + desv_std),
                fill="#0077b6", alpha=0.2)
    geom_line(color="#0077b6", size=1.2)
    geom_point(color="#0077b6", size=3)
- Escalas: scale_y_continuous con etiquetas en €.
           scale_x_continuous con breaks=[2021, 2022, 2023].
- Etiquetas labs(): title="Evolución de la Renta Media en S/C de Tenerife (2021-2023)",
             x="Año", y="Renta media bruta (€)", caption="Fuente: ISTAC E30325A_000009"
- Tema: theme_minimal().
  IMPORTANTE: figure_size va SIEMPRE dentro de theme(), ej: theme(figure_size=(12, 6)).
- Principios Gestalt aplicados: Continuidad (línea), Figura-Fondo (ribbon).
- NO usar matplotlib. NO texto fuera del bloque def generarplot.
"""
    return Output(
        {"system": SYSTEM_BASE, "user": f"Completa el template:\n{descripcion}"},
        metadata={"prompt_chars": len(descripcion)}
    )


@asset(group_name="ia_templates")
def template_ia_distribucion_ingresos(viz_distribucion_ingresos):
    columnas = ", ".join(viz_distribucion_ingresos.columns)
    fuentes  = list(viz_distribucion_ingresos["fuente"].unique())
    descripcion = f"""
- Dataset: viz_distribucion_ingresos con columnas: {columnas}
- Fuentes disponibles: {fuentes}
- Estéticas (aes): fuente → eje X, porcentaje → eje Y, fill=fuente, facet por anio.
- Geometría: geom_bar(stat="identity", position="dodge")
- Escalas: scale_fill_brewer(type="qual", palette="Set2")
           scale_y_continuous(limits=(0, 60)) con etiquetas en %
- Etiquetas labs(): title="Distribución de Fuentes de Renta – Secciones S/C de Tenerife",
             x="", y="Porcentaje (%)", caption="Fuente: ISTAC E30325A_000002"
- Tema: theme_minimal(). figure_size=(14, 7) DENTRO de theme().
        Rotar eje X 45°: axis_text_x=element_text(angle=45, ha="right")
- Gestalt: Similaridad (color por fuente), Proximidad (barras agrupadas).
- Pequeños múltiplos (Tufte): facet_wrap("~ anio") misma escala.
- NO usar matplotlib.
"""
    return Output({"system": SYSTEM_BASE, "user": f"Completa el template:\n{descripcion}"})


@asset(group_name="ia_templates")
def template_ia_actividad(viz_actividad_distribucion):
    columnas = ", ".join(viz_actividad_distribucion.columns)
    cats = list(viz_actividad_distribucion["relacion_actividad"].unique())
    # Cambiar esta línea:
    cats_str = ", ".join(str(c) for c in cats)
    descripcion = f"""
- Dataset: viz_actividad_distribucion con columnas: {columnas}
- Categorías: {cats_str}
- Estéticas (aes): relacion_actividad → eje X, porcentaje → eje Y,
             fill=relacion_actividad, facet por anio.
- Geometría: geom_bar(stat="identity")
- Escalas: scale_fill_manual con paleta accesible:
           Ocupado="#0077b6", Parado="#e63946",
           Inactivo="#457b9d", Otro="#a8dadc"
           scale_y_continuous con etiquetas en %
- Punto Focal (Gestalt): "Ocupado" en azul intenso; resto en tonos suaves.
- Etiquetas labs(): title="Relación con la Actividad – Secciones S/C de Tenerife (2021-2023)",
             x="", y="Porcentaje (%)", caption="Fuente: INE Tabla 66796"
- Tema: theme_minimal(). figure_size=(14, 7) DENTRO de theme().
- NO usar matplotlib.
"""
    return Output({"system": SYSTEM_BASE, "user": f"Completa el template:\n{descripcion}"})


@asset(group_name="ia_templates")
def template_ia_ocupacion(viz_ocupacion_distribucion):
    columnas = ", ".join(viz_ocupacion_distribucion.columns)
    sectores = list(viz_ocupacion_distribucion["sector"].unique())
    sectores_str = ", ".join(str(s) for s in sectores)
    descripcion = f"""
- Dataset: viz_ocupacion_distribucion con columnas: {columnas}
- Sectores disponibles: {sectores_str}
- Estéticas (aes): sector → eje X, porcentaje → eje Y, fill=sector, facet por anio.
- Geometría: geom_bar(stat="identity")
- Escalas: scale_fill_brewer(type="qual", palette="Paired")
           scale_y_continuous con etiquetas en %
- Etiquetas labs(): title="Sector de Ocupación – Secciones S/C de Tenerife (2021-2023)",
             x="", y="Porcentaje (%)", caption="Fuente: INE Tabla 70125"
- Tema: theme_minimal(). figure_size=(14, 7) DENTRO de theme().
        Rotar eje X 45°: axis_text_x=element_text(angle=45, ha="right")
- NO usar matplotlib.
"""
    return Output({"system": SYSTEM_BASE, "user": f"Completa el template:\n{descripcion}"})


# ─────────────────────────────────────────────────────────────────────────────
# 5. GENERACIÓN DE CÓDIGO CON IA
# ─────────────────────────────────────────────────────────────────────────────

def _llamar_llm(system_content: str, user_content: str, temperature: float = 0.1) -> str:
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": temperature,
        "max_tokens": 900,
        "stream": False,
    }
    resp = requests.post(LLM_URL, headers=LLM_HEADERS, json=payload, timeout=90)
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"]
    raw = re.sub(r"```python", "", raw)
    raw = re.sub(r"```", "", raw)
    return raw.strip()


def _limpiar_codigo(codigo: str) -> str:
    lineas = codigo.split("\n")
    inicio = next((i for i, l in enumerate(lineas) if l.strip().startswith("def generarplot")), 0)
    return "\n".join(lineas[inicio:]).strip()


@asset(group_name="ia_code")
def codigo_generado_tendencia(template_ia_tendencia_renta):
    codigo = _llamar_llm(template_ia_tendencia_renta["system"], template_ia_tendencia_renta["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


@asset(group_name="ia_code")
def codigo_generado_ingresos(template_ia_distribucion_ingresos):
    codigo = _llamar_llm(template_ia_distribucion_ingresos["system"], template_ia_distribucion_ingresos["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


@asset(group_name="ia_code")
def codigo_generado_actividad(template_ia_actividad):
    codigo = _llamar_llm(template_ia_actividad["system"], template_ia_actividad["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


@asset(group_name="ia_code")
def codigo_generado_ocupacion(template_ia_ocupacion):
    codigo = _llamar_llm(template_ia_ocupacion["system"], template_ia_ocupacion["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


# ─────────────────────────────────────────────────────────────────────────────
# 6. RENDERIZADO DE VISUALIZACIONES
# ─────────────────────────────────────────────────────────────────────────────

def _postprocesar_codigo(codigo: str) -> str:
    # theme_minimal(figure_size=(...)) → theme_minimal() + theme(figure_size=(...))
    codigo = re.sub(
        r"theme_minimal\(\s*figure_size\s*=\s*(\([^)]+\))\s*\)",
        r"theme_minimal() + theme(figure_size=\1)",
        codigo
    )
    # theme_minimal(cualquier_otro_arg) → theme_minimal() + theme(cualquier_otro_arg)
    def _fix_theme(m):
        interior = m.group(2).strip()
        if interior:
            return m.group(1) + "theme_minimal() + theme(" + interior + ")"
        return m.group(0)
    codigo = re.sub(r"([\+\s])theme_minimal\(([^)]+)\)", _fix_theme, codigo)
    return codigo

def _ejecutar_codigo_ia(codigo: str, df):
    import plotnine
    import pandas as _pd
    import ast

    # Eliminar imports en cualquier posición (dentro o fuera de funciones)
    codigo = re.sub(r"^[ \t]*from\s+\S+\s+import\s+[^\n]+\n?", "", codigo, flags=re.MULTILINE)
    codigo = re.sub(r"^[ \t]*import\s+\S+[^\n]*\n?",           "", codigo, flags=re.MULTILINE)
    codigo = re.sub(r"^[ \t]*from\s+plotnine\s+import\s+\*[^\n]*\n?", "", codigo, flags=re.MULTILINE)

    codigo = _postprocesar_codigo(codigo)

    # Quedarse solo desde 'def generarplot' por si hay texto basura antes
    lineas = codigo.split("\n")
    inicio = next((i for i, l in enumerate(lineas) if l.strip().startswith("def generarplot")), 0)
    codigo = "\n".join(lineas[inicio:])

    ast.parse(codigo)  # valida sintaxis

    entorno = {}
    entorno["plotnine"] = plotnine
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")})
    entorno["pd"] = _pd
    entorno["df"] = df

    exec(codigo, entorno)  # noqa: S102
    return entorno["generarplot"](df)

@asset(
    group_name="publicacion",
    deps=["grafico_tendencia_renta", "grafico_distribucion_ingresos",
          "grafico_actividad", "grafico_ocupacion",
          "mapa_renta_2021", "mapa_renta_2022", "mapa_renta_2023"]
)
def publicar_gh_pages(context):
    """Publica los PNGs generados en gh-pages automáticamente."""
    import shutil, subprocess
    from pathlib import Path

    repo_dir  = BASE_DIR
    docs_dir  = repo_dir / "docs"
    docs_dir.mkdir(exist_ok=True)

    # Copiar todos los PNGs a docs/
    for png in (repo_dir / "output").glob("*.png"):
        shutil.copy(png, docs_dir / png.name)
        context.log.info(f"Copiado: {png.name}")

    # Generar index.html
    graficos = sorted(docs_dir.glob("*.png"))
    items_html = "\n".join(
        f'  <figure>\n    <img src="{p.name}" style="max-width:900px;width:100%">\n'
        f'    <figcaption>{p.stem.replace("_", " ").title()}</figcaption>\n  </figure>'
        for p in graficos
    )
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>Secciones Censales – S/C de Tenerife 2021-2023</title>
  <style>
    body {{ font-family: sans-serif; max-width: 960px; margin: auto; padding: 2em; }}
    figure {{ margin: 2em 0; }}
    figcaption {{ color: #555; font-size: 0.9em; margin-top: 0.5em; }}
  </style>
</head>
<body>
  <h1>Visualizaciones – Secciones Censales S/C de Tenerife</h1>
  <p>Pipeline DataOps con Dagster · Gramática de Gráficos con Plotnine · IA Generativa</p>
{items_html}
</body>
</html>"""

    (docs_dir / "index.html").write_text(html, encoding="utf-8")

    # Commit y push a main (GitHub Pages sirve desde /docs en main)
    try:
        subprocess.run(["git", "add", "docs/"], cwd=repo_dir, check=True)
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"], cwd=repo_dir, capture_output=True
        )
        if result.returncode != 0:
            subprocess.run(
                ["git", "commit", "-m", "Auto: actualizar gh-pages docs/"],
                cwd=repo_dir, check=True
            )
            subprocess.run(["git", "push"], cwd=repo_dir, check=True)
            context.log.info("gh-pages actualizado correctamente")
        else:
            context.log.info("gh-pages sin cambios, nada que publicar")
    except subprocess.CalledProcessError as e:
        context.log.warning(f"Push gh-pages falló: {e}")

    return Output(str(docs_dir), metadata={"archivos_publicados": len(graficos)})

def _subir_github(ruta_archivo: str):
    import subprocess
    from pathlib import Path
    logger = get_dagster_logger()
    try:
        subprocess.run(["git", "add", ruta_archivo], check=True)
        # Verificar si hay algo que commitear antes de intentarlo
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True
        )
        if result.returncode == 0:
            logger.info(f"Sin cambios en {Path(ruta_archivo).name}, nada que commitear")
            return
        subprocess.run(["git", "commit", "-m", f"Auto: {Path(ruta_archivo).name}"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        logger.warning(f"Git push falló: {e}")


def _fallback_tendencia(df, ruta):
    import plotnine as p9
    df = df.copy()
    df.columns = [c.replace("año", "anio").replace("Año", "anio") for c in df.columns]
    # Forzar anio como factor para evitar problemas de escala continua
    df["anio"] = df["anio"].astype(str)
    g = (
        p9.ggplot(df, p9.aes(x="anio", y="media_provincial"))
        + p9.geom_line(p9.aes(group=1), color="#0077b6", size=1.2)
        + p9.geom_point(color="#0077b6", size=3)
        + p9.labs(
            title="Evolución de la Renta Media en S/C de Tenerife (2021-2023)",
            x="Año", y="Renta media bruta (€)",
            caption="Fuente: ISTAC E30325A_000009"
        )
        + p9.theme_minimal()
        + p9.theme(figure_size=(12, 6))
    )
    g.save(ruta, width=12, height=6, dpi=150)


def _fallback_ingresos(df, ruta):
    import plotnine as p9
    g = (
        p9.ggplot(df, p9.aes(x="fuente", y="porcentaje", fill="fuente"))
        + p9.geom_bar(stat="identity", position="dodge")
        + p9.facet_wrap("~anio")
        + p9.scale_fill_brewer(type="qual", palette="Set2")
        + p9.labs(title="Distribución Fuentes de Renta – S/C Tenerife",
                  x="", y="Porcentaje (%)", caption="Fuente: ISTAC E30325A_000002")
        + p9.theme_minimal()
        + p9.theme(figure_size=(14, 7),
                   axis_text_x=p9.element_text(angle=45, ha="right"))
    )
    g.save(ruta, width=14, height=7, dpi=150)

def _fallback_actividad(df, ruta):
    import plotnine as p9
    g = (
        p9.ggplot(df, p9.aes(x="relacion_actividad", y="porcentaje", fill="relacion_actividad"))
        + p9.geom_bar(stat="identity")
        + p9.facet_wrap("~anio")
        + p9.scale_fill_brewer(type="qual", palette="Set3")
        + p9.labs(title="Actividad Económica – S/C Tenerife 2021-2023",
                  x="", y="Porcentaje (%)", caption="Fuente: INE/ISTAC")
        + p9.theme_minimal()
        + p9.theme(figure_size=(16, 8),
                   axis_text_x=p9.element_text(angle=45, ha="right"),
                   legend_position="none")
    )
    g.save(ruta, width=16, height=8, dpi=150)


def _fallback_ocupacion(df, ruta):
    import plotnine as p9
    g = (
        p9.ggplot(df, p9.aes(x="sector", y="porcentaje", fill="sector"))
        + p9.geom_bar(stat="identity")
        + p9.facet_wrap("~anio")
        + p9.scale_fill_brewer(type="qual", palette="Paired")
        + p9.labs(title="Sector de Ocupación – S/C Tenerife 2021-2023",
                  x="", y="Porcentaje (%)", caption="Fuente: INE Tabla 70125")
        + p9.theme_minimal()
        + p9.theme(figure_size=(16, 8),
                   axis_text_x=p9.element_text(angle=45, ha="right"),
                   legend_position="none")
    )
    g.save(ruta, width=16, height=8, dpi=150)


def _mapa_coropletico(gdf, anio_datos, anio_geojson, ruta):
    import matplotlib.pyplot as plt

    gdf2 = gdf.dropna(subset=["renta_media"]).to_crs(epsg=4326)
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    gdf2.plot(
        column="renta_media",
        cmap="YlOrRd",
        linewidth=0.3,
        edgecolor="white",
        legend=True,
        ax=ax,
        legend_kwds={"label": "Renta media bruta (€)", "orientation": "vertical"}
    )
    ax.set_title(
        f"Renta Media Bruta por Sección Censal – S/C de Tenerife {anio_datos}",
        fontsize=14,
        fontweight="bold",
        pad=12
    )
    ax.set_axis_off()
    ax.annotate(
        f"Fuente: ISTAC E30325A_000009  |  GeoJSON: secciones_censales_tfe_{anio_geojson}",
        xy=(0.01, 0.02),
        xycoords="axes fraction",
        fontsize=8,
        color="gray"
    )
    plt.tight_layout()
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    plt.close()


@asset(group_name="visualizaciones")
def grafico_tendencia_renta(context, codigo_generado_tendencia, viz_renta_tendencia):
    ruta = str(OUT_DIR / "grafico_tendencia_renta.png")
    try:
        _ejecutar_codigo_ia(codigo_generado_tendencia, viz_renta_tendencia).save(
            ruta, width=12, height=6, dpi=150
        )
    except Exception as e:
        context.log.error(f"Fallo IA → fallback: {e}")
        _fallback_tendencia(viz_renta_tendencia, ruta)

    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def grafico_distribucion_ingresos(context, codigo_generado_ingresos, viz_distribucion_ingresos):
    ruta = str(OUT_DIR / "grafico_distribucion_ingresos.png")
    try:
        _ejecutar_codigo_ia(codigo_generado_ingresos, viz_distribucion_ingresos).save(
            ruta, width=14, height=7, dpi=150
        )
    except Exception as e:
        context.log.error(f"Fallo IA → fallback: {e}")
        _fallback_ingresos(viz_distribucion_ingresos, ruta)

    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def grafico_actividad(context, codigo_generado_actividad, viz_actividad_distribucion):
    ruta = str(OUT_DIR / "grafico_actividad.png")
    try:
        _ejecutar_codigo_ia(codigo_generado_actividad, viz_actividad_distribucion).save(
            ruta, width=14, height=7, dpi=150
        )
    except Exception as e:
        context.log.error(f"Fallo IA → fallback: {e}")
        _fallback_actividad(viz_actividad_distribucion, ruta)

    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def grafico_ocupacion(context, codigo_generado_ocupacion, viz_ocupacion_distribucion):
    ruta = str(OUT_DIR / "grafico_ocupacion.png")
    try:
        _ejecutar_codigo_ia(codigo_generado_ocupacion, viz_ocupacion_distribucion).save(
            ruta, width=14, height=7, dpi=150
        )
    except Exception as e:
        context.log.error(f"Fallo IA → fallback: {e}")
        _fallback_ocupacion(viz_ocupacion_distribucion, ruta)

    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})

@asset(group_name="visualizaciones")
def mapa_ocupacion_2023(context, viz_geo_ocupacion_2023):
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np

    ruta = str(OUT_DIR / "mapa_ocupacion_2023.png")

    # Filtrar geometrías nulas o vacías ANTES de to_crs
    gdf = viz_geo_ocupacion_2023.copy()
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    gdf = gdf.dropna(subset=["sector"])

    # Reproyectar solo si hay geometrías válidas
    if len(gdf) == 0:
        context.log.warning("mapa_ocupacion_2023: sin geometrías válidas, saltando mapa")
        # Crear PNG vacío para no romper el check
        fig, ax = plt.subplots(figsize=(14, 10))
        ax.text(0.5, 0.5, "Sin datos de ocupación por sección",
                ha="center", va="center", fontsize=16, transform=ax.transAxes)
        plt.savefig(ruta, dpi=150, bbox_inches="tight")
        plt.close()
        _subir_github(ruta)
        return Output(ruta)

    gdf = gdf.to_crs(epsg=3857)   # Web Mercator evita el bug del aspect ratio con epsg:4326

    sectores     = sorted(gdf["sector"].unique())
    sector_a_num = {s: i for i, s in enumerate(sectores)}
    gdf["sector_n"] = gdf["sector"].map(sector_a_num)
    cmap = plt.get_cmap("Set3", len(sectores))

    fig, ax = plt.subplots(figsize=(14, 10))
    ax.set_aspect("equal")   # forzar aspect ratio antes de gdf.plot

    gdf.plot(
        column="sector_n",
        cmap=cmap,
        vmin=0,
        vmax=max(len(sectores) - 1, 1),
        linewidth=0.3,
        edgecolor="white",
        ax=ax,
        aspect=None          # deshabilitar cálculo automático de aspect en geopandas
    )

    patches = [
        mpatches.Patch(color=cmap(i / max(len(sectores) - 1, 1)), label=s)
        for i, s in enumerate(sectores)
    ]
    ax.legend(handles=patches, loc="lower right", fontsize=7,
              title="Sector", title_fontsize=8, framealpha=0.8)
    ax.set_title("Sector de Ocupación Predominante por Sección – S/C de Tenerife 2023",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_axis_off()
    ax.annotate("Fuente: INE Tabla 70125  |  GeoJSON: secciones_censales_tfe_2024",
                xy=(0.01, 0.02), xycoords="axes fraction", fontsize=8, color="gray")
    plt.tight_layout()
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    plt.close()

    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta), "sectores": sectores})

@asset(group_name="visualizaciones")
def mapa_renta_2021(context, viz_geo_renta_2021):
    ruta = str(OUT_DIR / "mapa_renta_secciones_2021.png")
    _mapa_coropletico(viz_geo_renta_2021, 2021, 2022, ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def mapa_renta_2022(context, viz_geo_renta_2022):
    ruta = str(OUT_DIR / "mapa_renta_secciones_2022.png")
    _mapa_coropletico(viz_geo_renta_2022, 2022, 2023, ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def mapa_renta_2023(context, viz_geo_renta_2023):
    ruta = str(OUT_DIR / "mapa_renta_secciones_2023.png")
    _mapa_coropletico(viz_geo_renta_2023, 2023, 2024, ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})