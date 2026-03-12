"""
MSB León — API de Mantenimiento Inteligente
FastAPI + DB2 endpoints para watsonx Orchestrate

Tablas reales: EQUIPOS, CORRECTIVOS, PREVENTIVOS, RISK_SCORE,
               POLITICA_MANTENIMIENTO, PLAN_MANTENIMIENTO
"""

from dotenv import load_dotenv
load_dotenv()

import os
import ibm_db
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import get_db2_connection, execute_query, get_schema
from plan_predictivo import router as plan_router

# ============================================================
# APP
# ============================================================
app = FastAPI(
    title="MSB Mantenimiento Inteligente API",
    description="API para análisis predictivo de fallas y mantenimiento basado en condición. Diseñada para ser consumida por watsonx Orchestrate como skills.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(plan_router)



# ============================================================
# MODELOS PYDANTIC
# ============================================================
class EquipoPerfil(BaseModel):
    NUMERO_SERIE: str
    NUMERO_ECONOMICO: Optional[str] = None
    MODELO: Optional[str] = None
    CLIENTE: Optional[str] = None
    FECHA_INICIO_OP: Optional[str] = None
    TOTAL_CORRECTIVOS: Optional[int] = 0
    TOTAL_PREVENTIVOS: Optional[int] = 0
    RATIO_CORR_PREV: Optional[float] = None
    ULTIMO_HOROMETRO: Optional[float] = None
    PROMEDIO_USO_DIA: Optional[float] = None
    CATEGORIA_DOMINANTE: Optional[str] = None
    RISK_SCORE: Optional[float] = None
    NIVEL_RIESGO: Optional[str] = None
    BANDA_HOROMETRO: Optional[str] = None
    ULTIMA_FALLA: Optional[str] = None
    ULTIMO_PREVENTIVO: Optional[str] = None


class RiskScoreResponse(BaseModel):
    NUMERO_SERIE: str
    RISK_SCORE: float
    NIVEL_RIESGO: str
    TOTAL_CORRECTIVOS: int
    CATEGORIA_DOMINANTE: Optional[str]
    ULTIMO_HOROMETRO: Optional[float]
    PROMEDIO_USO_DIA: Optional[float]
    RATIO_CORR_PREV: Optional[float]


class EquipoCritico(BaseModel):
    NUMERO_SERIE: str
    NUMERO_ECONOMICO: Optional[str]
    MODELO: Optional[str]
    CLIENTE: Optional[str]
    RISK_SCORE: float
    NIVEL_RIESGO: str
    TOTAL_CORRECTIVOS: int
    CATEGORIA_DOMINANTE: Optional[str]
    ULTIMO_HOROMETRO: Optional[float]
    PROMEDIO_USO_DIA: Optional[float]


class RecomendacionResponse(BaseModel):
    numero_serie: str
    recomendacion: str


# ============================================================
# ENDPOINTS
# ============================================================

# ----------------------------------------------------------
# 1. PERFIL COMPLETO — JOIN EQUIPOS + RISK_SCORE
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}",
    response_model=EquipoPerfil,
    summary="Perfil completo de equipo",
    description="Retorna toda la información del equipo incluyendo métricas de riesgo, horómetro, uso diario y categoría de falla dominante.",
    tags=["Equipos"],
)
def get_equipo_perfil(numero_serie: str):
    S = get_schema()
    sql = f"""
        SELECT
            e.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
            e.FECHA_INICIO_OP,
            COALESCE(r.TOTAL_CORRECTIVOS, 0)  AS TOTAL_CORRECTIVOS,
            COALESCE(r.TOTAL_PREVENTIVOS, 0)  AS TOTAL_PREVENTIVOS,
            r.RATIO_CORR_PREV,
            r.ULTIMO_HOROMETRO,
            r.PROMEDIO_USO_DIA,
            r.CATEGORIA_DOMINANTE,
            r.RISK_SCORE,
            r.NIVEL_RIESGO,
            r.BANDA_HOROMETRO,
            r.ULTIMA_FALLA,
            r.ULTIMO_PREVENTIVO
        FROM {S}.EQUIPOS e
        LEFT JOIN {S}.RISK_SCORE r ON e.NUMERO_SERIE = r.NUMERO_SERIE
        WHERE e.NUMERO_SERIE = ?
    """
    results = execute_query(sql, (numero_serie,))
    if not results:
        raise HTTPException(status_code=404, detail=f"Equipo {numero_serie} no encontrado")
    return results[0]


# ----------------------------------------------------------
# 2. SCORE DE RIESGO
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/riesgo",
    response_model=RiskScoreResponse,
    summary="Score de riesgo del equipo",
    description="Niveles: CRITICO (≥25), ALTO (≥20), MEDIO (≥15), BAJO (<15).",
    tags=["Riesgo"],
)
def get_risk_score(numero_serie: str):
    S = get_schema()
    sql = f"""
        SELECT NUMERO_SERIE, RISK_SCORE, NIVEL_RIESGO,
               TOTAL_CORRECTIVOS, CATEGORIA_DOMINANTE,
               ULTIMO_HOROMETRO, PROMEDIO_USO_DIA, RATIO_CORR_PREV
        FROM {S}.RISK_SCORE
        WHERE NUMERO_SERIE = ?
    """
    results = execute_query(sql, (numero_serie,))
    if not results:
        raise HTTPException(status_code=404, detail=f"Sin datos de riesgo para {numero_serie}")
    return results[0]


# ----------------------------------------------------------
# 3. HISTORIAL — UNION CORRECTIVOS + PREVENTIVOS
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/historial",
    summary="Historial unificado correctivo + preventivo",
    description="Retorna el historial completo de mantenimientos ordenado por fecha descendente.",
    tags=["Equipos"],
)
def get_historial(
    numero_serie: str,
    tipo: Optional[str] = Query(None, description="CORRECTIVO o PREVENTIVO"),
    limit: int = Query(50, ge=1, le=500),
):
    S = get_schema()

    q_corr = f"""
        SELECT 'CORRECTIVO' AS TIPO, FECHA_SERVICIO AS FECHA,
               HOROMETRO, CATEGORIA_FALLA AS DETALLE,
               FALLA_REPORTADA AS DESCRIPCION, TECNICO
        FROM {S}.CORRECTIVOS WHERE NUMERO_SERIE = ?
    """
    q_prev = f"""
        SELECT 'PREVENTIVO' AS TIPO, FECHA_REAL_SERVICIO AS FECHA,
               HOROMETRO_ACTUAL AS HOROMETRO, TIPO_MANTENIMIENTO AS DETALLE,
               TECNICO_ASIGNADO AS DESCRIPCION, TECNICO_ASIGNADO AS TECNICO
        FROM {S}.PREVENTIVOS WHERE NUMERO_SERIE = ?
    """

    if tipo and tipo.upper() == "CORRECTIVO":
        sql = f"{q_corr} ORDER BY FECHA DESC FETCH FIRST {limit} ROWS ONLY"
        params = (numero_serie,)
    elif tipo and tipo.upper() == "PREVENTIVO":
        sql = f"{q_prev} ORDER BY FECHA DESC FETCH FIRST {limit} ROWS ONLY"
        params = (numero_serie,)
    else:
        sql = f"""
            SELECT * FROM ({q_corr} UNION ALL {q_prev}) AS H
            ORDER BY FECHA DESC
            FETCH FIRST {limit} ROWS ONLY
        """
        params = (numero_serie, numero_serie)

    return execute_query(sql, params)


# ----------------------------------------------------------
# 4. CORRECTIVOS DE UN EQUIPO
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/correctivos",
    summary="Detalle de fallas correctivas",
    tags=["Correctivos"],
)
def get_correctivos(numero_serie: str):
    S = get_schema()
    sql = f"""
        SELECT FECHA_SERVICIO, HOROMETRO, CATEGORIA_FALLA,
               FALLA_REPORTADA, REFACCIONES, OBSERVACIONES, TECNICO
        FROM {S}.CORRECTIVOS
        WHERE NUMERO_SERIE = ?
        ORDER BY FECHA_SERVICIO DESC
    """
    return execute_query(sql, (numero_serie,))


# ----------------------------------------------------------
# 5. EQUIPOS CRÍTICOS
# ----------------------------------------------------------
@app.get(
    "/equipos/criticos",
    response_model=List[EquipoCritico],
    summary="Equipos con mayor riesgo de falla",
    tags=["Riesgo"],
)
def get_equipos_criticos(
    nivel: Optional[str] = Query(None, description="CRITICO, ALTO, MEDIO, BAJO"),
    cliente: Optional[str] = Query(None),
    modelo: Optional[str] = Query(None),
    top: int = Query(20, ge=1, le=100),
):
    S = get_schema()
    conditions, params = [], []

    if nivel:
        conditions.append("r.NIVEL_RIESGO = ?")
        params.append(nivel.upper())
    if cliente:
        conditions.append("UPPER(e.CLIENTE) LIKE UPPER(?)")
        params.append(f"%{cliente}%")
    if modelo:
        conditions.append("UPPER(e.MODELO) LIKE UPPER(?)")
        params.append(f"%{modelo}%")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT e.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
               r.RISK_SCORE, r.NIVEL_RIESGO, r.TOTAL_CORRECTIVOS,
               r.CATEGORIA_DOMINANTE, r.ULTIMO_HOROMETRO, r.PROMEDIO_USO_DIA
        FROM {S}.EQUIPOS e
        JOIN {S}.RISK_SCORE r ON e.NUMERO_SERIE = r.NUMERO_SERIE
        {where}
        ORDER BY r.RISK_SCORE DESC
        FETCH FIRST {top} ROWS ONLY
    """
    return execute_query(sql, tuple(params))


# ----------------------------------------------------------
# 6. PATRONES POR CATEGORÍA — calculado desde CORRECTIVOS
# ----------------------------------------------------------
@app.get(
    "/patrones/categorias",
    summary="Distribución de fallas por categoría",
    tags=["Patrones"],
)
def get_patrones_categoria():
    S = get_schema()
    sql = f"""
        SELECT
            CATEGORIA_FALLA,
            COUNT(*)                                                AS TOTAL_FALLAS,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1)     AS PORCENTAJE,
            COUNT(DISTINCT NUMERO_SERIE)                           AS EQUIPOS_AFECTADOS,
            MIN(FECHA_SERVICIO)                                    AS PRIMERA_FALLA,
            MAX(FECHA_SERVICIO)                                    AS ULTIMA_FALLA
        FROM {S}.CORRECTIVOS
        WHERE CATEGORIA_FALLA IS NOT NULL
        GROUP BY CATEGORIA_FALLA
        ORDER BY TOTAL_FALLAS DESC
    """
    return execute_query(sql)


# ----------------------------------------------------------
# 7. ANÁLISIS POR BANDA DE HORÓMETRO — calculado desde CORRECTIVOS
# ----------------------------------------------------------
@app.get(
    "/patrones/horometro",
    summary="Fallas por banda de horómetro",
    tags=["Patrones"],
)
def get_patrones_horometro():
    S = get_schema()
    sql = f"""
        SELECT
            CASE
                WHEN HOROMETRO < 1000  THEN '0-1K'
                WHEN HOROMETRO < 2500  THEN '1K-2.5K'
                WHEN HOROMETRO < 5000  THEN '2.5K-5K'
                WHEN HOROMETRO < 10000 THEN '5K-10K'
                ELSE '10K+'
            END                                                         AS BANDA_HOROMETRO,
            COUNT(*)                                                    AS TOTAL_CORRECTIVOS,
            COUNT(DISTINCT NUMERO_SERIE)                                AS EQUIPOS,
            ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT NUMERO_SERIE), 1)    AS PROMEDIO_POR_EQUIPO
        FROM {S}.CORRECTIVOS
        WHERE HOROMETRO IS NOT NULL
        GROUP BY
            CASE
                WHEN HOROMETRO < 1000  THEN '0-1K'
                WHEN HOROMETRO < 2500  THEN '1K-2.5K'
                WHEN HOROMETRO < 5000  THEN '2.5K-5K'
                WHEN HOROMETRO < 10000 THEN '5K-10K'
                ELSE '10K+'
            END
        ORDER BY MIN(HOROMETRO)
    """
    return execute_query(sql)


# ----------------------------------------------------------
# 8. RECOMENDACIÓN — lógica Python sobre RISK_SCORE
# ----------------------------------------------------------
@app.get(
    "/equipo/{numero_serie}/recomendacion",
    response_model=RecomendacionResponse,
    summary="Recomendación de mantenimiento inteligente",
    tags=["Recomendaciones"],
)
def get_recomendacion(numero_serie: str):
    S = get_schema()
    results = execute_query(
        f"SELECT * FROM {S}.RISK_SCORE WHERE NUMERO_SERIE = ?",
        (numero_serie,)
    )
    if not results:
        raise HTTPException(status_code=404, detail=f"Sin datos para {numero_serie}")

    r           = results[0]
    nivel       = r.get("NIVEL_RIESGO", "")
    score       = r.get("RISK_SCORE", 0) or 0
    cat         = r.get("CATEGORIA_DOMINANTE") or "No identificada"
    horo        = r.get("ULTIMO_HOROMETRO", 0) or 0
    uso         = r.get("PROMEDIO_USO_DIA", 0) or 0
    correctivos = r.get("TOTAL_CORRECTIVOS", 0) or 0

    if nivel == "CRITICO":
        rec = (
            f"⚠️ EQUIPO CRÍTICO (score {score:.1f}). "
            f"{correctivos} correctivos registrados; falla dominante: {cat}. "
            f"Horómetro: {horo:.0f} hrs — uso {uso:.1f} hrs/día. "
            "Acción inmediata: programar revisión en los próximos 7 días e inspeccionar sistema dominante."
        )
    elif nivel == "ALTO":
        rec = (
            f"🔶 RIESGO ALTO (score {score:.1f}). "
            f"{correctivos} correctivos; falla dominante: {cat}. Horómetro: {horo:.0f} hrs. "
            "Programar servicio en los próximos 15 días con atención al sistema problemático."
        )
    elif nivel == "MEDIO":
        rec = (
            f"🟡 RIESGO MEDIO (score {score:.1f}). Falla dominante: {cat}. "
            f"Horómetro: {horo:.0f} hrs. "
            "Preventivo estándar. Próximo servicio en 30 días o 200 hrs."
        )
    else:
        rec = (
            f"✅ RIESGO BAJO (score {score:.1f}). Equipo estable — {correctivos} correctivos. "
            f"Horómetro: {horo:.0f} hrs. "
            "Continuar preventivo regular. Próximo servicio en 45 días o 300 hrs."
        )

    return {"numero_serie": numero_serie, "recomendacion": rec}


# ----------------------------------------------------------
# 9. BUSCAR EQUIPO
# ----------------------------------------------------------
@app.get("/equipos/buscar", summary="Buscar por número económico o serie parcial", tags=["Equipos"])
def buscar_equipo(q: str = Query(..., min_length=2)):
    S = get_schema()
    term = f"%{q}%"
    sql = f"""
        SELECT e.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
               COALESCE(r.RISK_SCORE, 0)              AS RISK_SCORE,
               COALESCE(r.NIVEL_RIESGO, 'SIN DATOS')  AS NIVEL_RIESGO,
               COALESCE(r.TOTAL_CORRECTIVOS, 0)       AS TOTAL_CORRECTIVOS
        FROM {S}.EQUIPOS e
        LEFT JOIN {S}.RISK_SCORE r ON e.NUMERO_SERIE = r.NUMERO_SERIE
        WHERE UPPER(e.NUMERO_SERIE)     LIKE UPPER(?)
           OR UPPER(e.NUMERO_ECONOMICO) LIKE UPPER(?)
        ORDER BY COALESCE(r.RISK_SCORE, 0) DESC
        FETCH FIRST 20 ROWS ONLY
    """
    return execute_query(sql, (term, term))


# ----------------------------------------------------------
# 10. RESUMEN EJECUTIVO
# ----------------------------------------------------------
@app.get("/resumen", summary="Resumen ejecutivo del estado de la flota", tags=["Resumen"])
def get_resumen():
    S = get_schema()
    total_corr = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {S}.CORRECTIVOS")[0]["TOTAL"]
    total_prev = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {S}.PREVENTIVOS")[0]["TOTAL"]
    criticos   = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {S}.RISK_SCORE WHERE NIVEL_RIESGO = 'CRITICO'")[0]["TOTAL"]
    altos      = execute_query(f"SELECT COUNT(*) AS TOTAL FROM {S}.RISK_SCORE WHERE NIVEL_RIESGO = 'ALTO'")[0]["TOTAL"]
    cat_top    = execute_query(f"""
        SELECT CATEGORIA_FALLA, COUNT(*) AS TOTAL
        FROM {S}.CORRECTIVOS
        WHERE CATEGORIA_FALLA IS NOT NULL
        GROUP BY CATEGORIA_FALLA
        ORDER BY COUNT(*) DESC
        FETCH FIRST 1 ROW ONLY
    """)
    flota = execute_query(f"""
        SELECT COUNT(*) AS EQUIPOS,
               AVG(PROMEDIO_USO_DIA) AS AVG_USO,
               AVG(ULTIMO_HOROMETRO) AS AVG_HORO
        FROM {S}.RISK_SCORE
        WHERE PROMEDIO_USO_DIA IS NOT NULL
    """)

    return {
        "total_correctivos":       total_corr,
        "total_preventivos":       total_prev,
        "ratio_global":            round(total_corr / max(total_prev, 1) * 100, 1),
        "equipos_criticos":        criticos,
        "equipos_alto_riesgo":     altos,
        "categoria_mas_frecuente": cat_top[0] if cat_top else None,
        "flota":                   flota[0] if flota else None,
    }


# ----------------------------------------------------------
# 11. ALERTAS PREVENTIVOS VENCIDOS
# ----------------------------------------------------------
@app.get(
    "/alertas/preventivos-vencidos",
    summary="Equipos con preventivo próximo o vencido",
    tags=["Alertas"],
)
def get_preventivos_vencidos(dias: int = Query(7)):
    S = get_schema()
    sql = f"""
        SELECT
            p.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
            p.FECHA_SUGERIDA_PMM,
            DAYS(p.FECHA_SUGERIDA_PMM) - DAYS(CURRENT DATE) AS DIAS_RESTANTES,
            COALESCE(r.RISK_SCORE, 0)             AS RISK_SCORE,
            COALESCE(r.NIVEL_RIESGO, 'SIN DATOS') AS NIVEL_RIESGO,
            p.HOROMETRO_ACTUAL
        FROM {S}.PREVENTIVOS p
        JOIN  {S}.EQUIPOS e       ON p.NUMERO_SERIE = e.NUMERO_SERIE
        LEFT JOIN {S}.RISK_SCORE r ON p.NUMERO_SERIE = r.NUMERO_SERIE
        WHERE p.FECHA_SUGERIDA_PMM IS NOT NULL
          AND p.FECHA_SUGERIDA_PMM <= CURRENT DATE + {dias} DAYS
          AND p.ID_PREVENTIVO = (
              SELECT MAX(p2.ID_PREVENTIVO)
              FROM {S}.PREVENTIVOS p2
              WHERE p2.NUMERO_SERIE = p.NUMERO_SERIE
          )
        ORDER BY COALESCE(r.RISK_SCORE, 0) DESC
    """
    return execute_query(sql)


# ============================================================
# HEALTH CHECK
# ============================================================
@app.get("/health", tags=["System"])
def health():
    return {"status": "ok", "service": "MSB Mantenimiento Inteligente API", "version": "1.0.0"}


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
