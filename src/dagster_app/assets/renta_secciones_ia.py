"""
Pipeline DataOps - Renta y Actividad por Secciones Censales
Provincia de Santa Cruz de Tenerife, 2021-2023
Autor: Laura Lasso García
Rama: practica-final-secciones
"""

import os, re, subprocess
import pandas as pd
import geopandas as gpd
import requests
from pathlib import Path

from dagster import (
    asset, Output, MetadataValue, get_dagster_logger
)

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR  = BASE_DIR / "data"
CARTO_DIR = DATA_DIR / "cartografia-secciones"
OUT_DIR   = BASE_DIR / "output"
OUT_DIR.mkdir(exist_ok=True)

LLM_URL     = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
LLM_MODEL   = "ollama/llama3.1:8b"
LLM_HEADERS = {"Content-Type": "application/json", "Authorization": "Bearer sk-1234"}

# Regla: año de datos → GeoJSON del año siguiente
# 2021 → secciones_20220101_tenerife.json
# 2022 → secciones_20230101_tenerife.json
# 2023 → secciones_20240101_tenerife.json
GEOJSON = {
    2021: CARTO_DIR / "secciones_20220101_tenerife.json",
    2022: CARTO_DIR / "secciones_20230101_tenerife.json",
    2023: CARTO_DIR / "secciones_20240101_tenerife.json",
}

# ─────────────────────────────────────────────
# 1. ASSETS DE DATOS BRUTOS
# ─────────────────────────────────────────────

@asset(group_name="raw")
def raw_renta_media():
    """Renta bruta media y mediana por secciones 2021-2023 (ISTAC E30325A_000009)."""
    df = pd.read_csv(DATA_DIR / "rentamedia-sc-3.csv", sep=";", encoding="utf-8", dtype=str)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    get_dagster_logger().info(f"Filas renta media: {len(df)}")
    return Output(df, metadata={"rows": len(df), "columns": list(df.columns)})


@asset(group_name="raw")
def raw_distribucion_ingresos():
    """Distribución de renta según fuente de ingresos (ISTAC E30325A_000002)."""
    df = pd.read_csv(DATA_DIR / "distribucion-renta-ingresos.csv", sep=";", encoding="utf-8", dtype=str)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return Output(df, metadata={"rows": len(df)})


@asset(group_name="raw")
def raw_actividad():
    """Relación con la actividad por secciones 2021-2023 (INE Tabla 66796)."""
    df = pd.read_csv(DATA_DIR / "actividad-sc-3.csv", sep=";", encoding="utf-8", dtype=str)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return Output(df, metadata={"rows": len(df)})


@asset(group_name="raw")
def raw_ocupacion():
    """Sector de ocupación por secciones 2021-2023 (INE Censo anual 70125)."""
    df = pd.read_csv(DATA_DIR / "ocupacion-sc-3.csv", sep=";", encoding="utf-8", dtype=str)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return Output(df, metadata={"rows": len(df)})


@asset(group_name="raw")
def raw_geojson_2022():
    """GeoJSON secciones censales Tenerife – vigente 01/01/2022."""
    gdf = gpd.read_file(CARTO_DIR / "secciones_20220101_tenerife.json")
    return Output(gdf, metadata={"features": len(gdf), "crs": str(gdf.crs)})


@asset(group_name="raw")
def raw_geojson_2023():
    """GeoJSON secciones censales Tenerife – vigente 01/01/2023."""
    gdf = gpd.read_file(CARTO_DIR / "secciones_20230101_tenerife.json")
    return Output(gdf, metadata={"features": len(gdf)})


@asset(group_name="raw")
def raw_geojson_2024():
    """GeoJSON secciones censales Tenerife – vigente 01/01/2024."""
    gdf = gpd.read_file(CARTO_DIR / "secciones_20240101_tenerife.json")
    return Output(gdf, metadata={"features": len(gdf)})


# ─────────────────────────────────────────────
# 2. LIMPIEZA / TRANSFORMACIÓN
# ─────────────────────────────────────────────

@asset(group_name="cleaned")
def cleaned_renta_media(raw_renta_media):
    df = raw_renta_media.copy()
    df = df.rename(columns={
        df.columns[0]: "seccion",
        df.columns[1]: "anio",
        df.columns[2]: "renta_media",
        df.columns[3]: "renta_mediana"
    })
    df["anio"]          = pd.to_numeric(df["anio"], errors="coerce")
    df["renta_media"]   = pd.to_numeric(df["renta_media"].str.replace(",", "."),   errors="coerce")
    df["renta_mediana"] = pd.to_numeric(df["renta_mediana"].str.replace(",", "."), errors="coerce")
    df = df.dropna(subset=["seccion", "anio", "renta_media"])
    df["anio"] = df["anio"].astype(int)
    return Output(df, metadata={"rows_cleaned": len(df)})


@asset(group_name="cleaned")
def cleaned_distribucion_ingresos(raw_distribucion_ingresos):
    df = raw_distribucion_ingresos.copy()
    df = df.rename(columns={
        df.columns[0]: "seccion",
        df.columns[1]: "anio",
        df.columns[2]: "fuente",
        df.columns[3]: "importe"
    })
    df["anio"]    = pd.to_numeric(df["anio"], errors="coerce")
    df["importe"] = pd.to_numeric(df["importe"].str.replace(",", "."), errors="coerce")
    df = df.dropna(subset=["seccion", "fuente", "importe"])
    df["anio"] = df["anio"].astype(int)
    return Output(df, metadata={"rows_cleaned": len(df), "fuentes": list(df["fuente"].unique())})


@asset(group_name="cleaned")
def cleaned_actividad(raw_actividad):
    df = raw_actividad.copy()
    df = df.rename(columns={
        df.columns[0]: "seccion",
        df.columns[1]: "anio",
        df.columns[2]: "relacion_actividad",
        df.columns[3]: "personas"
    })
    df["anio"]     = pd.to_numeric(df["anio"], errors="coerce")
    df["personas"] = pd.to_numeric(df["personas"].str.replace(",", "."), errors="coerce")
    df = df.dropna(subset=["seccion", "relacion_actividad", "personas"])
    df["anio"] = df["anio"].astype(int)
    return Output(df, metadata={"rows_cleaned": len(df)})


@asset(group_name="cleaned")
def cleaned_ocupacion(raw_ocupacion):
    df = raw_ocupacion.copy()
    df = df.rename(columns={
        df.columns[0]: "seccion",
        df.columns[1]: "anio",
        df.columns[2]: "sector",
        df.columns[3]: "personas"
    })
    df["anio"]     = pd.to_numeric(df["anio"], errors="coerce")
    df["personas"] = pd.to_numeric(df["personas"].str.replace(",", "."), errors="coerce")
    df = df.dropna(subset=["seccion", "sector", "personas"])
    df["anio"] = df["anio"].astype(int)
    return Output(df, metadata={"rows_cleaned": len(df), "sectores": list(df["sector"].unique())})


# ─────────────────────────────────────────────
# 3. ASSETS VIZ_DATA
# ─────────────────────────────────────────────

@asset(group_name="viz_data")
def viz_renta_tendencia(cleaned_renta_media):
    """Renta media provincial por año (media, mediana, desv. típica)."""
    df = (cleaned_renta_media
          .groupby("anio")["renta_media"]
          .agg(["mean", "median", "std"])
          .reset_index())
    df.columns = ["anio", "media_provincial", "mediana_provincial", "desv_std"]
    return Output(df, metadata={"years": list(df["anio"])})


@asset(group_name="viz_data")
def viz_distribucion_ingresos(cleaned_distribucion_ingresos):
    """Distribución porcentual de fuentes de renta por año."""
    df = cleaned_distribucion_ingresos.groupby(["anio", "fuente"])["importe"].sum().reset_index()
    total = df.groupby("anio")["importe"].transform("sum")
    df["porcentaje"] = (df["importe"] / total * 100).round(2)
    return Output(df, metadata={"fuentes": list(df["fuente"].unique())})


@asset(group_name="viz_data")
def viz_actividad_distribucion(cleaned_actividad):
    """Distribución porcentual de relación con la actividad por año."""
    df = cleaned_actividad.groupby(["anio", "relacion_actividad"])["personas"].sum().reset_index()
    total = df.groupby("anio")["personas"].transform("sum")
    df["porcentaje"] = (df["personas"] / total * 100).round(2)
    return Output(df, metadata={"categorias": list(df["relacion_actividad"].unique())})


@asset(group_name="viz_data")
def viz_ocupacion_distribucion(cleaned_ocupacion):
    """Distribución porcentual de sectores de ocupación por año."""
    df = cleaned_ocupacion.groupby(["anio", "sector"])["personas"].sum().reset_index()
    total = df.groupby("anio")["personas"].transform("sum")
    df["porcentaje"] = (df["personas"] / total * 100).round(2)
    return Output(df, metadata={"sectores": list(df["sector"].unique())})


@asset(group_name="viz_data")
def viz_geo_renta_2021(cleaned_renta_media, raw_geojson_2022):
    """Renta 2021 + GeoJSON 20220101 (regla año+1)."""
    df  = cleaned_renta_media[cleaned_renta_media["anio"] == 2021].copy()
    gdf = raw_geojson_2022.merge(df, left_on="CUSEC", right_on="seccion", how="left")
    return Output(gdf, metadata={"features_con_renta": int(gdf["renta_media"].notna().sum())})


@asset(group_name="viz_data")
def viz_geo_renta_2022(cleaned_renta_media, raw_geojson_2023):
    """Renta 2022 + GeoJSON 20230101 (regla año+1)."""
    df  = cleaned_renta_media[cleaned_renta_media["anio"] == 2022].copy()
    gdf = raw_geojson_2023.merge(df, left_on="CUSEC", right_on="seccion", how="left")
    return Output(gdf, metadata={"features_con_renta": int(gdf["renta_media"].notna().sum())})


@asset(group_name="viz_data")
def viz_geo_renta_2023(cleaned_renta_media, raw_geojson_2024):
    """Renta 2023 + GeoJSON 20240101 (regla año+1)."""
    df  = cleaned_renta_media[cleaned_renta_media["anio"] == 2023].copy()
    gdf = raw_geojson_2024.merge(df, left_on="CUSEC", right_on="seccion", how="left")
    return Output(gdf, metadata={"features_con_renta": int(gdf["renta_media"].notna().sum())})


# ─────────────────────────────────────────────
# 4. TEMPLATES DE PROMPTS PARA IA
# ─────────────────────────────────────────────

TEMPLATE_TECNICO = """
def generarplot(df):
    plot = ggplot(df, aes(...))
    ...
    return plot
"""

SYSTEM_BASE = (
    "Eres un experto en gramática de gráficos y Plotnine. "
    "Tu tarea es traducir descripciones en lenguaje natural a código ejecutable. "
    f"Usa SIEMPRE este template:\n{TEMPLATE_TECNICO}\n"
    "Devuelve EXCLUSIVAMENTE el código Python. "
    "NO incluyas texto explicativo, markdown ni comentarios fuera del código."
)


@asset(group_name="ia_templates")
def template_ia_tendencia_renta(viz_renta_tendencia):
    columnas = ", ".join(viz_renta_tendencia.columns)
    years    = sorted(viz_renta_tendencia["anio"].unique().tolist())
    descripcion = f"""
- Dataset: viz_renta_tendencia con columnas: {columnas}
- Años disponibles: {years}
- Estéticas: anio → eje X (cuantitativa discreta), media_provincial → eje Y.
- Geometrías:
    geom_ribbon(aes(ymin=media_provincial - desv_std, ymax=media_provincial + desv_std),
                fill="#0077b6", alpha=0.2)
    geom_line(color="#0077b6", size=1.2)
    geom_point(color="#0077b6", size=3)
- Escalas: scale_y_continuous etiquetas en €.
           scale_x_continuous breaks=[2021, 2022, 2023].
- Etiquetas: title="Evolución de la Renta Media en S/C de Tenerife (2021-2023)",
             x="Año", y="Renta media bruta (€)",
             caption="Fuente: ISTAC E30325A_000009 · rentamedia-sc-3.csv"
- Tema: theme_minimal().
  IMPORTANTE: figure_size va SIEMPRE dentro de theme(), p.ej. theme(figure_size=(12, 6)).
- Principios Gestalt: Continuidad (línea), Figura-Fondo (ribbon).
- NO usar matplotlib. NO texto fuera del código.
"""
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion}"
    return Output({"system": SYSTEM_BASE, "user": user_content},
                  metadata={"prompt_chars": len(user_content)})


@asset(group_name="ia_templates")
def template_ia_distribucion_ingresos(viz_distribucion_ingresos):
    columnas = ", ".join(viz_distribucion_ingresos.columns)
    fuentes  = list(viz_distribucion_ingresos["fuente"].unique())
    descripcion = f"""
- Dataset: viz_distribucion_ingresos con columnas: {columnas}
- Fuentes disponibles: {fuentes}
- Estéticas: fuente → eje X, porcentaje → eje Y, fill=fuente, facet por anio.
- Geometría: geom_bar(stat="identity", position="dodge")
- Escalas: scale_fill_brewer(type="qual", palette="Set2")  ← OBLIGATORIO type="qual"
           scale_y_continuous(limits=(0, 60), labels con %)
- Etiquetas: title="Distribución de Fuentes de Renta – Secciones S/C de Tenerife",
             x="", y="Porcentaje (%)",
             caption="Fuente: ISTAC E30325A_000002 · distribucion-renta-ingresos.csv"
- Tema: theme_minimal(). figure_size=(14, 7) DENTRO de theme().
        Rotar etiquetas eje X 45 grados: element_text(angle=45, ha="right")
- Gestalt: Similaridad (color por fuente), Proximidad (barras agrupadas).
- Pequeños múltiplos Tufte: facet_wrap("~ anio") misma escala por año.
- NO usar matplotlib.
"""
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion}"
    return Output({"system": SYSTEM_BASE, "user": user_content})


@asset(group_name="ia_templates")
def template_ia_actividad(viz_actividad_distribucion):
    columnas = ", ".join(viz_actividad_distribucion.columns)
    cats     = list(viz_actividad_distribucion["relacion_actividad"].unique())
    descripcion = f"""
- Dataset: viz_actividad_distribucion con columnas: {columnas}
- Categorías: {cats}
- Estéticas: relacion_actividad → eje X, porcentaje → eje Y,
             fill=relacion_actividad, facet por anio.
- Geometría: geom_bar(stat="identity")
- Escalas: scale_fill_manual(values={{
               "Ocupado":  "#0077b6",
               "Parado":   "#e63946",
               "Inactivo": "#457b9d",
               "Otro":     "#a8dadc"
           }})
           scale_y_continuous etiquetas en %
- Punto Focal Gestalt: "Ocupado" en azul intenso (#0077b6); resto en tonos suaves.
- Etiquetas: title="Relación con la Actividad – Secciones S/C de Tenerife (2021-2023)",
             x="", y="Porcentaje (%)",
             caption="Fuente: INE Tabla 66796 · actividad-sc-3.csv"
- Tema: theme_minimal(). figure_size=(14, 7) DENTRO de theme().
- NO usar matplotlib.
"""
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion}"
    return Output({"system": SYSTEM_BASE, "user": user_content})


@asset(group_name="ia_templates")
def template_ia_ocupacion(viz_ocupacion_distribucion):
    columnas = ", ".join(viz_ocupacion_distribucion.columns)
    sectores = list(viz_ocupacion_distribucion["sector"].unique())
    descripcion = f"""
- Dataset: viz_ocupacion_distribucion con columnas: {columnas}
- Sectores disponibles: {sectores}
- Estéticas: sector → eje X, porcentaje → eje Y, fill=sector, facet por anio.
- Geometría: geom_bar(stat="identity")
- Escalas: scale_fill_brewer(type="qual", palette="Paired")  ← OBLIGATORIO type="qual"
           scale_y_continuous etiquetas en %
- Etiquetas: title="Sector de Ocupación – Secciones S/C de Tenerife (2021-2023)",
             x="", y="Porcentaje (%)",
             caption="Fuente: INE Censo Anual 70125 · ocupacion-sc-3.csv"
- Tema: theme_minimal(). figure_size=(16, 7) DENTRO de theme().
        Rotar etiquetas eje X 45 grados.
- Gestalt: Similaridad por color para cada sector económico.
- NO usar matplotlib.
"""
    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion}"
    return Output({"system": SYSTEM_BASE, "user": user_content})


# ─────────────────────────────────────────────
# 5. GENERACIÓN DE CÓDIGO CON IA
# ─────────────────────────────────────────────

def _llamar_llm(system_content: str, user_content: str, temperature: float = 0.1) -> str:
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user",   "content": user_content}
        ],
        "temperature": temperature,
        "max_tokens": 800,
        "stream": False
    }
    resp = requests.post(LLM_URL, headers=LLM_HEADERS, json=payload, timeout=60)
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"]
    raw = re.sub(r"```python", "", raw)
    raw = re.sub(r"```",       "", raw)
    return raw.strip()


def _limpiar_codigo(codigo: str) -> str:
    """Extrae solo el bloque que empieza con 'def generarplot'."""
    lineas = codigo.split("\n")
    inicio = next((i for i, l in enumerate(lineas)
                   if l.strip().startswith("def generarplot")), 0)
    return "\n".join(lineas[inicio:]).strip()


@asset(group_name="ia_code")
def codigo_generado_tendencia(template_ia_tendencia_renta):
    codigo = _llamar_llm(template_ia_tendencia_renta["system"],
                         template_ia_tendencia_renta["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


@asset(group_name="ia_code")
def codigo_generado_ingresos(template_ia_distribucion_ingresos):
    codigo = _llamar_llm(template_ia_distribucion_ingresos["system"],
                         template_ia_distribucion_ingresos["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


@asset(group_name="ia_code")
def codigo_generado_actividad(template_ia_actividad):
    codigo = _llamar_llm(template_ia_actividad["system"],
                         template_ia_actividad["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


@asset(group_name="ia_code")
def codigo_generado_ocupacion(template_ia_ocupacion):
    codigo = _llamar_llm(template_ia_ocupacion["system"],
                         template_ia_ocupacion["user"])
    return Output(_limpiar_codigo(codigo), metadata={"chars": len(codigo)})


# ─────────────────────────────────────────────
# 6. RENDERIZADO DE VISUALIZACIONES
# ─────────────────────────────────────────────

def _ejecutar_codigo_ia(codigo: str, df):
    import plotnine
    import pandas as _pd
    entorno = globals().copy()
    entorno["plotnine"] = plotnine
    entorno.update({k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")})
    entorno["pd"] = _pd
    entorno["df"] = df
    exec(codigo, entorno)   # noqa: S102
    return entorno["generarplot"](df)


def _subir_github(ruta_archivo: str):
    try:
        subprocess.run(["git", "add",    ruta_archivo],                        check=True)
        subprocess.run(["git", "commit", "-m", f"Auto: {Path(ruta_archivo).name}"], check=True)
        subprocess.run(["git", "push"],                                        check=True)
    except subprocess.CalledProcessError as e:
        get_dagster_logger().warning(f"Git push falló: {e}")


# ── Fallbacks determinísticos ────────────────────────────────────────────────

def _fallback_tendencia(df, ruta):
    import plotnine as p9
    g = (
        p9.ggplot(df, p9.aes("anio", "media_provincial"))
        + p9.geom_ribbon(p9.aes(ymin="media_provincial - desv_std",
                                ymax="media_provincial + desv_std"),
                         fill="#0077b6", alpha=0.2)
        + p9.geom_line(color="#0077b6", size=1.2)
        + p9.geom_point(color="#0077b6", size=3)
        + p9.labs(title="Evolución de la Renta Media en S/C de Tenerife (2021-2023)",
                  x="Año", y="Renta media bruta (€)",
                  caption="Fuente: ISTAC E30325A_000009 · rentamedia-sc-3.csv")
        + p9.theme_minimal()
        + p9.theme(figure_size=(12, 6))
    )
    g.save(ruta, width=12, height=6, dpi=150)


def _fallback_ingresos(df, ruta):
    import plotnine as p9
    g = (
        p9.ggplot(df, p9.aes("fuente", "porcentaje", fill="fuente"))
        + p9.geom_bar(stat="identity", position="dodge")
        + p9.facet_wrap("~anio")
        + p9.scale_fill_brewer(type="qual", palette="Set2")
        + p9.labs(title="Distribución de Fuentes de Renta – Secciones S/C de Tenerife",
                  x="", y="Porcentaje (%)",
                  caption="Fuente: ISTAC E30325A_000002 · distribucion-renta-ingresos.csv")
        + p9.theme_minimal()
        + p9.theme(figure_size=(14, 7),
                   axis_text_x=p9.element_text(angle=45, ha="right"))
    )
    g.save(ruta, width=14, height=7, dpi=150)


def _fallback_actividad(df, ruta):
    import plotnine as p9
    colores = {"Ocupado": "#0077b6", "Parado": "#e63946",
               "Inactivo": "#457b9d", "Otro": "#a8dadc"}
    g = (
        p9.ggplot(df, p9.aes("relacion_actividad", "porcentaje", fill="relacion_actividad"))
        + p9.geom_bar(stat="identity")
        + p9.facet_wrap("~anio")
        + p9.scale_fill_manual(values=colores)
        + p9.labs(title="Relación con la Actividad – Secciones S/C de Tenerife (2021-2023)",
                  x="", y="Porcentaje (%)",
                  caption="Fuente: INE Tabla 66796 · actividad-sc-3.csv")
        + p9.theme_minimal()
        + p9.theme(figure_size=(14, 7),
                   axis_text_x=p9.element_text(angle=30, ha="right"))
    )
    g.save(ruta, width=14, height=7, dpi=150)


def _fallback_ocupacion(df, ruta):
    import plotnine as p9
    g = (
        p9.ggplot(df, p9.aes("sector", "porcentaje", fill="sector"))
        + p9.geom_bar(stat="identity")
        + p9.facet_wrap("~anio")
        + p9.scale_fill_brewer(type="qual", palette="Paired")
        + p9.labs(title="Sector de Ocupación – Secciones S/C de Tenerife (2021-2023)",
                  x="", y="Porcentaje (%)",
                  caption="Fuente: INE Censo Anual 70125 · ocupacion-sc-3.csv")
        + p9.theme_minimal()
        + p9.theme(figure_size=(16, 7),
                   axis_text_x=p9.element_text(angle=45, ha="right"))
    )
    g.save(ruta, width=16, height=7, dpi=150)


def _mapa_coropletico(gdf_raw, anio: int, geojson_anio: int, ruta: str):
    """Genera y guarda un mapa coroplético de renta media por secciones."""
    import matplotlib.pyplot as plt
    gdf = gdf_raw.dropna(subset=["renta_media"]).to_crs(epsg=4326)
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    gdf.plot(column="renta_media", cmap="YlOrRd", linewidth=0.3,
             edgecolor="white", legend=True, ax=ax,
             legend_kwds={"label": "Renta media bruta (€)", "orientation": "vertical"})
    ax.set_title(f"Renta Media Bruta por Sección Censal – S/C de Tenerife {anio}",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_axis_off()
    ax.annotate(
        f"Fuente: ISTAC E30325A_000009 · rentamedia-sc-3.csv  |  "
        f"GeoJSON: secciones_{geojson_anio}0101_tenerife.json",
        xy=(0.01, 0.02), xycoords="axes fraction", fontsize=8, color="gray"
    )
    plt.tight_layout()
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    plt.close()


# ── Assets de visualización ──────────────────────────────────────────────────

@asset(group_name="visualizaciones")
def grafico_tendencia_renta(context, codigo_generado_tendencia, viz_renta_tendencia):
    ruta = str(OUT_DIR / "grafico_tendencia_renta.png")
    try:
        grafico = _ejecutar_codigo_ia(codigo_generado_tendencia, viz_renta_tendencia)
        grafico.save(ruta, width=12, height=6, dpi=150)
    except Exception as e:
        context.log.error(f"Fallo IA, usando fallback: {e}")
        _fallback_tendencia(viz_renta_tendencia, ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def grafico_distribucion_ingresos(context, codigo_generado_ingresos, viz_distribucion_ingresos):
    ruta = str(OUT_DIR / "grafico_distribucion_ingresos.png")
    try:
        grafico = _ejecutar_codigo_ia(codigo_generado_ingresos, viz_distribucion_ingresos)
        grafico.save(ruta, width=14, height=7, dpi=150)
    except Exception as e:
        context.log.error(f"Fallo IA, usando fallback: {e}")
        _fallback_ingresos(viz_distribucion_ingresos, ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def grafico_actividad(context, codigo_generado_actividad, viz_actividad_distribucion):
    ruta = str(OUT_DIR / "grafico_actividad.png")
    try:
        grafico = _ejecutar_codigo_ia(codigo_generado_actividad, viz_actividad_distribucion)
        grafico.save(ruta, width=14, height=7, dpi=150)
    except Exception as e:
        context.log.error(f"Fallo IA, usando fallback: {e}")
        _fallback_actividad(viz_actividad_distribucion, ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def grafico_ocupacion(context, codigo_generado_ocupacion, viz_ocupacion_distribucion):
    ruta = str(OUT_DIR / "grafico_ocupacion.png")
    try:
        grafico = _ejecutar_codigo_ia(codigo_generado_ocupacion, viz_ocupacion_distribucion)
        grafico.save(ruta, width=16, height=7, dpi=150)
    except Exception as e:
        context.log.error(f"Fallo IA, usando fallback: {e}")
        _fallback_ocupacion(viz_ocupacion_distribucion, ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def mapa_renta_2021(context, viz_geo_renta_2021):
    """Mapa coroplético renta 2021 con GeoJSON secciones_20220101_tenerife.json"""
    ruta = str(OUT_DIR / "mapa_renta_secciones_2021.png")
    _mapa_coropletico(viz_geo_renta_2021, anio=2021, geojson_anio=2022, ruta=ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def mapa_renta_2022(context, viz_geo_renta_2022):
    """Mapa coroplético renta 2022 con GeoJSON secciones_20230101_tenerife.json"""
    ruta = str(OUT_DIR / "mapa_renta_secciones_2022.png")
    _mapa_coropletico(viz_geo_renta_2022, anio=2022, geojson_anio=2023, ruta=ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})


@asset(group_name="visualizaciones")
def mapa_renta_2023(context, viz_geo_renta_2023):
    """Mapa coroplético renta 2023 con GeoJSON secciones_20240101_tenerife.json"""
    ruta = str(OUT_DIR / "mapa_renta_secciones_2023.png")
    _mapa_coropletico(viz_geo_renta_2023, anio=2023, geojson_anio=2024, ruta=ruta)
    _subir_github(ruta)
    return Output(ruta, metadata={"url": MetadataValue.url(ruta)})