"""
MSB León — Endpoints de Mantenimiento Predictivo Basado en Riesgo

Montado como router en main.py:
    from plan_predictivo import router as plan_router
    app.include_router(plan_router)
"""

import ibm_db
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from db import get_db2_connection, execute_query, SCHEMA

router = APIRouter()


# ============================================================
# MODELOS
# ============================================================
class PoliticaMantenimiento(BaseModel):
    id_politica: int
    nivel_riesgo: str
    tipo_mantenimiento: str
    intervalo_horas: int
    intervalo_dias_max: int
    alcance: Optional[str]
    checklist_extra: Optional[str]
    prioridad: int


class PlanServicio(BaseModel):
    id_plan: int
    numero_serie: str
    nivel_riesgo_asignado: Optional[str]
    risk_score_al_generar: Optional[float]
    tipo_mantenimiento: Optional[str]
    fecha_programada: str
    horometro_estimado: Optional[float]
    alcance: Optional[str]
    checklist: Optional[str]
    prioridad: int
    estado: str


class AgendaItem(BaseModel):
    fecha_programada: str
    prioridad: int
    nivel_riesgo: Optional[str]
    risk_score: Optional[float]
    numero_serie: str
    numero_economico: Optional[str]
    modelo: Optional[str]
    cliente: Optional[str]
    tipo_mantenimiento: Optional[str]
    horometro_estimado: Optional[float]
    alcance: Optional[str]
    checklist: Optional[str]
    categoria_dominante: Optional[str]
    total_correctivos: Optional[int]


class ComparativaPlan(BaseModel):
    numero_serie: str
    numero_economico: Optional[str]
    nivel_riesgo: Optional[str]
    risk_score: Optional[float]
    total_correctivos: Optional[int]
    promedio_uso_dia: Optional[float]
    intervalo_fijo_hrs: int
    dias_plan_fijo: Optional[int]
    intervalo_riesgo_hrs: Optional[int]
    dias_plan_riesgo: Optional[int]
    servicios_menos_por_anio: Optional[int]


class CargaSemanal(BaseModel):
    semana_num: int
    fecha_inicio_semana: Optional[str]
    total_servicios: int
    criticos: int
    altos: int
    medios: int
    bajos: int


# ============================================================
# ENDPOINTS
# ============================================================

# ----------------------------------------------------------
# 1. GENERAR PLAN PARA UN EQUIPO
# ----------------------------------------------------------
@router.post(
    "/plan/generar/{numero_serie}",
    summary="Generar plan predictivo para un equipo",
    description="Genera el plan de mantenimiento basado en el nivel de riesgo del equipo. "
                "Calcula fechas e intervalos dinámicos según el horómetro, uso diario y categoría de falla dominante. "
                "Si el equipo es CRITICO, los intervalos son más cortos y el checklist más exhaustivo. "
                "Si es BAJO, los intervalos se extienden para no desperdiciar recursos.",
    tags=["Plan Predictivo"],
)
def generar_plan_equipo(
    numero_serie: str,
    meses: int = Query(6, ge=1, le=12, description="Horizonte de planificación en meses"),
):
    conn = get_db2_connection()
    try:
        stmt = ibm_db.prepare(conn, f"CALL {SCHEMA}.SP_GENERAR_PLAN_EQUIPO(?, ?)")
        ibm_db.execute(stmt, (numero_serie, meses))
        ibm_db.commit(conn)

        count_sql = f"""
            SELECT COUNT(*) AS TOTAL,
                   MIN(FECHA_PROGRAMADA) AS PRIMERA,
                   MAX(FECHA_PROGRAMADA) AS ULTIMA
            FROM {SCHEMA}.PLAN_MANTENIMIENTO
            WHERE NUMERO_SERIE = ? AND ESTADO = 'PENDIENTE'
        """
        stmt2 = ibm_db.prepare(conn, count_sql)
        ibm_db.execute(stmt2, (numero_serie,))
        row = ibm_db.fetch_assoc(stmt2)

        return {
            "status": "ok",
            "numero_serie": numero_serie,
            "meses_planificados": meses,
            "servicios_generados": row["TOTAL"] if row else 0,
            "primer_servicio": str(row["PRIMERA"]) if row and row["PRIMERA"] else None,
            "ultimo_servicio": str(row["ULTIMA"]) if row and row["ULTIMA"] else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)


# ----------------------------------------------------------
# 2. GENERAR PLAN PARA TODA LA FLOTA
# ----------------------------------------------------------
@router.post(
    "/plan/generar-flota",
    summary="Generar plan predictivo para toda la flota",
    description="Genera planes de mantenimiento para TODOS los equipos activos. "
                "Cada equipo obtiene su propio plan según su nivel de riesgo. "
                "Los equipos BAJO riesgo tendrán menos servicios programados que los CRITICO.",
    tags=["Plan Predictivo"],
)
def generar_plan_flota(
    meses: int = Query(6, ge=1, le=12, description="Horizonte de planificación en meses"),
):
    conn = get_db2_connection()
    try:
        stmt = ibm_db.prepare(conn, f"CALL {SCHEMA}.SP_GENERAR_PLAN_FLOTA(?)")
        ibm_db.execute(stmt, (meses,))
        ibm_db.commit(conn)

        resumen = execute_query(f"""
            SELECT NIVEL_RIESGO_ASIGNADO, COUNT(*) AS SERVICIOS,
                   COUNT(DISTINCT NUMERO_SERIE) AS EQUIPOS
            FROM {SCHEMA}.PLAN_MANTENIMIENTO
            WHERE ESTADO = 'PENDIENTE'
            GROUP BY NIVEL_RIESGO_ASIGNADO
            ORDER BY
                CASE NIVEL_RIESGO_ASIGNADO
                    WHEN 'CRITICO' THEN 1
                    WHEN 'ALTO' THEN 2
                    WHEN 'MEDIO' THEN 3
                    ELSE 4
                END
        """)

        return {
            "status": "ok",
            "meses_planificados": meses,
            "resumen_por_nivel": resumen,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)


# ----------------------------------------------------------
# 3. VER PLAN DE UN EQUIPO
# ----------------------------------------------------------
@router.get(
    "/plan/{numero_serie}",
    response_model=List[PlanServicio],
    summary="Ver plan de mantenimiento de un equipo",
    description="Retorna todos los servicios programados del equipo, con fechas, "
                "horómetro estimado, alcance y checklist personalizado según su riesgo.",
    tags=["Plan Predictivo"],
)
def get_plan_equipo(
    numero_serie: str,
    estado: Optional[str] = Query(None, description="Filtrar: PENDIENTE, EJECUTADO, CANCELADO"),
):
    where = "WHERE pm.NUMERO_SERIE = ?"
    params = [numero_serie]
    if estado:
        where += " AND pm.ESTADO = ?"
        params.append(estado.upper())

    sql = f"""
        SELECT pm.* FROM {SCHEMA}.PLAN_MANTENIMIENTO pm
        {where}
        ORDER BY pm.FECHA_PROGRAMADA
    """
    results = execute_query(sql, tuple(params))
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"No hay plan para {numero_serie}. Genéralo primero con POST /plan/generar/{numero_serie}"
        )
    return results


# ----------------------------------------------------------
# 4. AGENDA SEMANAL PRIORIZADA
# ----------------------------------------------------------
@router.get(
    "/plan/agenda/semanal",
    response_model=List[AgendaItem],
    summary="Agenda semanal priorizada por riesgo",
    description="Retorna los servicios programados para los próximos N días, "
                "ORDENADOS POR PRIORIDAD (los CRITICO primero, los BAJO al final). "
                "Este es el endpoint principal para la operación diaria.",
    tags=["Plan Predictivo"],
)
def get_agenda_semanal(
    dias: int = Query(7, ge=1, le=90, description="Ventana de días a mostrar"),
    nivel: Optional[str] = Query(None, description="Filtrar por nivel de riesgo"),
    cliente: Optional[str] = Query(None, description="Filtrar por cliente"),
):
    conditions = [
        "pm.ESTADO = 'PENDIENTE'",
        f"pm.FECHA_PROGRAMADA BETWEEN CURRENT DATE AND CURRENT DATE + {dias} DAYS",
    ]
    params = []

    if nivel:
        conditions.append("pm.NIVEL_RIESGO_ASIGNADO = ?")
        params.append(nivel.upper())
    if cliente:
        conditions.append("UPPER(e.CLIENTE) LIKE UPPER(?)")
        params.append(f"%{cliente}%")

    where = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT
            pm.FECHA_PROGRAMADA, pm.PRIORIDAD,
            pm.NIVEL_RIESGO_ASIGNADO AS NIVEL_RIESGO,
            pm.RISK_SCORE_AL_GENERAR AS RISK_SCORE,
            e.NUMERO_SERIE, e.NUMERO_ECONOMICO, e.MODELO, e.CLIENTE,
            pm.TIPO_MANTENIMIENTO, pm.HOROMETRO_ESTIMADO,
            pm.ALCANCE, pm.CHECKLIST,
            rs.CATEGORIA_DOMINANTE, rs.TOTAL_CORRECTIVOS
        FROM {SCHEMA}.PLAN_MANTENIMIENTO pm
        JOIN {SCHEMA}.EQUIPOS e ON pm.NUMERO_SERIE = e.NUMERO_SERIE
        LEFT JOIN {SCHEMA}.RISK_SCORE rs ON pm.NUMERO_SERIE = rs.NUMERO_SERIE
        {where}
        ORDER BY pm.PRIORIDAD ASC, pm.FECHA_PROGRAMADA ASC
    """
    return execute_query(sql, tuple(params))


# ----------------------------------------------------------
# 5. COMPARATIVA PLAN FIJO VS PLAN POR RIESGO
# ----------------------------------------------------------
@router.get(
    "/plan/comparativa",
    response_model=List[ComparativaPlan],
    summary="Comparar plan fijo vs plan basado en riesgo",
    description="Muestra para cada equipo la diferencia entre mantener un intervalo fijo de 200 hrs "
                "vs el intervalo dinámico basado en riesgo. "
                "Los equipos BAJO riesgo AHORRAN servicios. Los CRITICO reciben MÁS atención.",
    tags=["Plan Predictivo"],
)
def get_comparativa(
    nivel: Optional[str] = Query(None, description="Filtrar por nivel"),
):
    if nivel:
        sql = f"""
            SELECT * FROM {SCHEMA}.V_COMPARATIVA_PLANES
            WHERE NIVEL_RIESGO = ?
            ORDER BY RISK_SCORE DESC
        """
        params = (nivel.upper(),)
    else:
        sql = f"""
            SELECT * FROM {SCHEMA}.V_COMPARATIVA_PLANES
            ORDER BY
                CASE NIVEL_RIESGO
                    WHEN 'CRITICO' THEN 1 WHEN 'ALTO' THEN 2
                    WHEN 'MEDIO' THEN 3 ELSE 4
                END,
                RISK_SCORE DESC
        """
        params = ()

    return execute_query(sql, params)


# ----------------------------------------------------------
# 6. CARGA DE TRABAJO SEMANAL
# ----------------------------------------------------------
@router.get(
    "/plan/carga-semanal",
    response_model=List[CargaSemanal],
    summary="Carga de trabajo semanal proyectada",
    description="Proyección de cuántos servicios hay por semana y su distribución por nivel de riesgo. "
                "Útil para dimensionar técnicos y recursos.",
    tags=["Plan Predictivo"],
)
def get_carga_semanal(
    semanas: int = Query(12, ge=1, le=52, description="Semanas a proyectar"),
):
    sql = f"""
        SELECT * FROM {SCHEMA}.V_CARGA_SEMANAL
        WHERE SEMANA_NUM <= ?
        ORDER BY SEMANA_NUM
    """
    return execute_query(sql, (semanas,))


# ----------------------------------------------------------
# 7. POLÍTICAS DE MANTENIMIENTO
# ----------------------------------------------------------
@router.get(
    "/politicas",
    response_model=List[PoliticaMantenimiento],
    summary="Ver políticas de mantenimiento por nivel de riesgo",
    description="Retorna las reglas que definen los intervalos y alcance de cada tipo de servicio "
                "según el nivel de riesgo. Estas políticas son la base del motor predictivo.",
    tags=["Políticas"],
)
def get_politicas(
    nivel: Optional[str] = Query(None, description="Filtrar por nivel de riesgo"),
):
    where = "WHERE NIVEL_RIESGO = ?" if nivel else ""
    params = (nivel.upper(),) if nivel else ()

    sql = f"""
        SELECT * FROM {SCHEMA}.POLITICA_MANTENIMIENTO
        {where}
        ORDER BY
            CASE NIVEL_RIESGO
                WHEN 'CRITICO' THEN 1 WHEN 'ALTO' THEN 2
                WHEN 'MEDIO' THEN 3 ELSE 4
            END,
            TIPO_MANTENIMIENTO
    """
    return execute_query(sql, params)


@router.put(
    "/politicas/{id_politica}",
    summary="Actualizar política de mantenimiento",
    description="Modifica los intervalos o alcance de una política. "
                "Después de cambiar una política, regenerar los planes para que tomen efecto.",
    tags=["Políticas"],
)
def update_politica(
    id_politica: int,
    intervalo_horas: Optional[int] = None,
    intervalo_dias_max: Optional[int] = None,
    alcance: Optional[str] = None,
    checklist_extra: Optional[str] = None,
):
    sets, params = [], []
    if intervalo_horas is not None:
        sets.append("INTERVALO_HORAS = ?"); params.append(intervalo_horas)
    if intervalo_dias_max is not None:
        sets.append("INTERVALO_DIAS_MAX = ?"); params.append(intervalo_dias_max)
    if alcance is not None:
        sets.append("ALCANCE = ?"); params.append(alcance)
    if checklist_extra is not None:
        sets.append("CHECKLIST_EXTRA = ?"); params.append(checklist_extra)

    if not sets:
        raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")

    params.append(id_politica)
    sql = f"UPDATE {SCHEMA}.POLITICA_MANTENIMIENTO SET {', '.join(sets)} WHERE ID_POLITICA = ?"

    conn = get_db2_connection()
    try:
        stmt = ibm_db.prepare(conn, sql)
        ibm_db.execute(stmt, tuple(params))
        ibm_db.commit(conn)
        return {"status": "ok", "message": f"Política {id_politica} actualizada"}
    finally:
        ibm_db.close(conn)


# ----------------------------------------------------------
# 8. MARCAR SERVICIO COMO EJECUTADO
# ----------------------------------------------------------
@router.put(
    "/plan/{id_plan}/ejecutar",
    summary="Marcar un servicio como ejecutado",
    description="Registra que un servicio programado fue realizado. "
                "Opcionalmente captura el horómetro real y observaciones.",
    tags=["Plan Predictivo"],
)
def ejecutar_servicio(
    id_plan: int,
    horometro_real: Optional[float] = None,
    observaciones: Optional[str] = None,
):
    sql = f"""
        UPDATE {SCHEMA}.PLAN_MANTENIMIENTO
        SET ESTADO = 'EJECUTADO',
            FECHA_EJECUTADO = CURRENT DATE,
            HOROMETRO_REAL = ?,
            OBSERVACIONES = ?
        WHERE ID_PLAN = ?
    """
    conn = get_db2_connection()
    try:
        stmt = ibm_db.prepare(conn, sql)
        ibm_db.execute(stmt, (horometro_real, observaciones, id_plan))
        ibm_db.commit(conn)
        return {"status": "ok", "message": f"Servicio {id_plan} marcado como ejecutado"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)
