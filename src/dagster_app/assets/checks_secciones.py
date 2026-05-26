"""
Checks de calidad Dagster – Pipeline de Secciones Censales S/C de Tenerife.
26 checks distribuidos en todos los grupos del pipeline.
"""
import os
from dagster import asset_check, AssetCheckResult


# ══════════════════════════════════════════════════════════
# RAW
# ══════════════════════════════════════════════════════════

@asset_check(asset="raw_renta_media")
def check_raw_renta_columnas(raw_renta_media):
    df = raw_renta_media
    ok = len(df.columns) >= 4 and len(df) > 0
    return AssetCheckResult(passed=ok,
        metadata={"columnas": len(df.columns), "filas": len(df)})


@asset_check(asset="raw_renta_media")
def check_raw_renta_sin_nulos(raw_renta_media):
    nulos = int(raw_renta_media.iloc[:, :4].isnull().sum().sum())
    return AssetCheckResult(passed=bool(nulos == 0),
        metadata={"nulos_criticos": nulos})


@asset_check(asset="raw_distribucion_ingresos")
def check_raw_ingresos_columnas(raw_distribucion_ingresos):
    df = raw_distribucion_ingresos
    ok = len(df.columns) >= 4 and len(df) > 0
    return AssetCheckResult(passed=ok, metadata={"filas": len(df)})


@asset_check(asset="raw_actividad")
def check_raw_actividad_columnas(raw_actividad):
    df = raw_actividad
    ok = len(df.columns) >= 4 and len(df) > 0
    return AssetCheckResult(passed=ok, metadata={"filas": len(df)})


@asset_check(asset="raw_ocupacion")
def check_raw_ocupacion_columnas(raw_ocupacion):
    df = raw_ocupacion
    ok = len(df.columns) >= 4 and len(df) > 0
    return AssetCheckResult(passed=ok, metadata={"filas": len(df)})

@asset_check(asset="raw_relacion_actividad")
def check_raw_relacion_actividad_columnas(raw_relacion_actividad):
    ok = len(raw_relacion_actividad.columns) >= 4 and len(raw_relacion_actividad) > 0
    return AssetCheckResult(passed=bool(ok),
        metadata={"filas": len(raw_relacion_actividad),
                  "columnas": list(raw_relacion_actividad.columns)})

# ══════════════════════════════════════════════════════════
# CLEANED
# ══════════════════════════════════════════════════════════

@asset_check(asset="cleaned_renta_media")
def check_cleaned_renta_anios(cleaned_renta_media):
    anios = sorted(cleaned_renta_media["anio"].unique().tolist())
    ok    = all(a in anios for a in [2021, 2022, 2023])
    return AssetCheckResult(passed=bool(ok),
        metadata={"anios_disponibles": str(anios)})

@asset_check(asset="cleaned_renta_media")
def check_cleaned_renta_positiva(cleaned_renta_media):
    negativos = int((cleaned_renta_media["renta_media"] < 0).sum())
    return AssetCheckResult(passed=bool(negativos == 0),
        metadata={"valores_negativos": negativos})

@asset_check(asset="cleaned_fuentes_renta")
def check_fuentes_categorias(cleaned_fuentes_renta):
    """Al menos 2 fuentes de renta esperadas deben aparecer."""
    fuentes_esperadas = {
        "Sueldos y salarios", "SUELDOS_SALARIOS",
        "Otras prestaciones", "OTRAS_PRESTACIONES",
        "Pensiones", "PENSIONES"
    }
    fuentes_reales = set(cleaned_fuentes_renta["fuente"].unique())
    coincidencias = len(fuentes_esperadas & fuentes_reales)
    ok = bool(coincidencias >= 2)
    return AssetCheckResult(passed=ok,
        metadata={"fuentes_encontradas": list(fuentes_reales)[:10],
                  "coincidencias": coincidencias})


@asset_check(asset="cleaned_actividad")
def check_actividad_categorias(cleaned_actividad):
    """Verifica que hay categorías de actividad/sector en el dataset."""
    cols = list(cleaned_actividad.columns)
    # Buscar la columna categórica correcta dinámicamente
    col = next(
        (c for c in ["relacion_actividad", "sector", "actividad", "categoria"]
         if c in cols),
        None
    )
    if col is None:
        return AssetCheckResult(passed=False,
            metadata={"error": "No se encontró columna categórica",
                      "columnas_disponibles": cols})
    valores = list(cleaned_actividad[col].dropna().unique())
    ok = bool(len(valores) >= 1)
    return AssetCheckResult(passed=ok,
        metadata={"columna_usada": col,
                  "categorias_encontradas": valores[:10],
                  "num_categorias": len(valores)})


@asset_check(asset="cleaned_ocupacion")
def check_ocupacion_sectores(cleaned_ocupacion):
    n = cleaned_ocupacion["sector"].nunique()
    return AssetCheckResult(passed=bool(n >= 3),
        metadata={"num_sectores": n, "sectores": list(cleaned_ocupacion["sector"].unique())})

@asset_check(asset="cleaned_relacion_actividad")
def check_relacion_actividad_categorias(cleaned_relacion_actividad):
    """Al menos 2 categorías de relación con la actividad."""
    cats = list(cleaned_relacion_actividad["relacion_actividad"].dropna().unique())
    ok   = len(cats) >= 2
    return AssetCheckResult(passed=bool(ok),
        metadata={"categorias_encontradas": cats[:10],
                  "num_categorias": len(cats)})

# ══════════════════════════════════════════════════════════
# VIZ_DATA
# ══════════════════════════════════════════════════════════

@asset_check(asset="viz_renta_tendencia")
def check_viz_renta_tendencia(viz_renta_tendencia):
    df = viz_renta_tendencia
    ok = bool(len(df) >= 3 and df["media_provincial"].notna().all())
    return AssetCheckResult(passed=ok, metadata={"filas": len(df)})


@asset_check(asset="viz_distribucion_ingresos")
def check_viz_ingresos_suma(viz_distribucion_ingresos):
    """Porcentajes por año deben sumar ~100%."""
    for anio, grp in viz_distribucion_ingresos.groupby("anio"):
        total = grp["porcentaje"].sum()
        if abs(total - 100) > 1:
            return AssetCheckResult(passed=False,
                metadata={"anio_fallido": int(anio), "suma": float(round(total, 2))})
    return AssetCheckResult(passed=True,
        metadata={"mensaje": "Porcentajes suman ~100% en cada año"})


@asset_check(asset="viz_actividad_distribucion")
def check_viz_actividad_suma(viz_actividad_distribucion):
    for anio, grp in viz_actividad_distribucion.groupby("anio"):
        total = grp["porcentaje"].sum()
        if abs(total - 100) > 1:
            return AssetCheckResult(passed=False,
                metadata={"anio_fallido": int(anio), "suma": float(round(total, 2))})
    return AssetCheckResult(passed=True)


@asset_check(asset="viz_ocupacion_distribucion")
def check_viz_ocupacion_suma(viz_ocupacion_distribucion):
    for anio, grp in viz_ocupacion_distribucion.groupby("anio"):
        total = grp["porcentaje"].sum()
        if abs(total - 100) > 1:
            return AssetCheckResult(passed=False,
                metadata={"anio_fallido": int(anio), "suma": float(round(total, 2))})
    return AssetCheckResult(passed=True)

@asset_check(asset="viz_relacion_actividad")
def check_viz_relacion_actividad_suma(viz_relacion_actividad):
    """Porcentajes de relación con actividad por año deben sumar ~100%."""
    df = viz_relacion_actividad
    for anio, grp in df.groupby("anio"):
        total = grp["porcentaje"].sum()
        if abs(total - 100) > 1.5:
            return AssetCheckResult(passed=False,
                metadata={"anio_fallido": int(anio), "suma": float(round(total, 2))})
    return AssetCheckResult(passed=True)


@asset_check(asset="viz_geo_renta_2021")
def check_geo_merge_2021(viz_geo_renta_2021):
    pct = float(viz_geo_renta_2021["renta_media"].notna().mean() * 100)
    return AssetCheckResult(passed=bool(pct >= 50),
        metadata={"pct_secciones_con_renta": round(pct, 1)})


@asset_check(asset="viz_geo_renta_2022")
def check_geo_merge_2022(viz_geo_renta_2022):
    pct = viz_geo_renta_2022["renta_media"].notna().mean() * 100
    return AssetCheckResult(passed=bool(pct >= 50),
        metadata={"pct_secciones_con_renta": round(float(pct), 1)})


@asset_check(asset="viz_geo_renta_2023")
def check_geo_merge_2023(viz_geo_renta_2023):
    pct = viz_geo_renta_2023["renta_media"].notna().mean() * 100
    return AssetCheckResult(passed=bool(pct >= 50),
        metadata={"pct_secciones_con_renta": round(float(pct), 1)})


# ══════════════════════════════════════════════════════════
# CÓDIGO GENERADO POR IA
# ══════════════════════════════════════════════════════════

def _check_codigo(codigo: str, geometria: str) -> AssetCheckResult:
    tiene_funcion = "def generarplot" in codigo
    tiene_ggplot  = "ggplot"          in codigo
    tiene_return  = "return"          in codigo
    tiene_geom    = geometria         in codigo
    ok = all([tiene_funcion, tiene_ggplot, tiene_return, tiene_geom])
    return AssetCheckResult(passed=ok, metadata={
        "tiene_generarplot":  tiene_funcion,
        "tiene_ggplot":       tiene_ggplot,
        "tiene_return":       tiene_return,
        f"tiene_{geometria}": tiene_geom
    })


@asset_check(asset="codigo_generado_tendencia")
def check_codigo_tendencia(codigo_generado_tendencia):
    return _check_codigo(codigo_generado_tendencia, "geom_line")


@asset_check(asset="codigo_generado_ingresos")
def check_codigo_ingresos(codigo_generado_ingresos):
    return _check_codigo(codigo_generado_ingresos, "geom_bar")


@asset_check(asset="codigo_generado_actividad")
def check_codigo_actividad(codigo_generado_actividad):
    return _check_codigo(codigo_generado_actividad, "scale_fill")


@asset_check(asset="codigo_generado_ocupacion")
def check_codigo_ocupacion(codigo_generado_ocupacion):
    return _check_codigo(codigo_generado_ocupacion, "geom_bar")


# ══════════════════════════════════════════════════════════
# VISUALIZACIONES (PNG > 10 KB)
# ══════════════════════════════════════════════════════════

def _check_png(ruta_png: str) -> AssetCheckResult:
    existe = os.path.exists(ruta_png)
    size   = os.path.getsize(ruta_png) if existe else 0
    ok     = existe and size > 10_000
    return AssetCheckResult(passed=ok,
        metadata={"ruta": ruta_png, "size_bytes": size, "existe": existe})


@asset_check(asset="grafico_tendencia_renta")
def check_grafico_tendencia(grafico_tendencia_renta):
    return _check_png(grafico_tendencia_renta)


@asset_check(asset="grafico_distribucion_ingresos")
def check_grafico_ingresos(grafico_distribucion_ingresos):
    return _check_png(grafico_distribucion_ingresos)


@asset_check(asset="grafico_actividad")
def check_grafico_actividad(grafico_actividad):
    return _check_png(grafico_actividad)


@asset_check(asset="grafico_ocupacion")
def check_grafico_ocupacion(grafico_ocupacion):
    return _check_png(grafico_ocupacion)


@asset_check(asset="mapa_renta_2021")
def check_mapa_2021(mapa_renta_2021):
    return _check_png(mapa_renta_2021)


@asset_check(asset="mapa_renta_2022")
def check_mapa_2022(mapa_renta_2022):
    return _check_png(mapa_renta_2022)


@asset_check(asset="mapa_renta_2023")
def check_mapa_2023(mapa_renta_2023):
    return _check_png(mapa_renta_2023)

@asset_check(asset="mapa_ocupacion_2023")
def check_mapa_ocupacion_2023(mapa_ocupacion_2023):
    return _check_png(mapa_ocupacion_2023)