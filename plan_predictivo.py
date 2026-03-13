"""
MSB León — Endpoints de Mantenimiento Predictivo Basado en Riesgo

Montado como router en main.py:
    from plan_predictivo import router as plan_router
    app.include_router(plan_router)
"""

import ibm_db
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict
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
    model_config = ConfigDict(
        alias_generator=lambda field: field.upper(),  # acepta MAYÚSCULAS del DB
        populate_by_name=True,                        # también acepta minúsculas
    )

    fecha_programada: str
    prioridad: int
    nivel_riesgo: str
    risk_score: str
    numero_serie: str
    numero_economico: str
    modelo: str
    cliente: str
    tipo_mantenimiento: str
    horometro_estimado: str
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
        # numero_serie se sanitiza manualmente para evitar inyección
        serie_safe = numero_serie.replace("'", "''")
        sql = f"CALL {SCHEMA}.SP_GENERAR_PLAN_EQUIPO('{serie_safe}', {int(meses)})"
        ibm_db.exec_immediate(conn, sql)
        ibm_db.commit(conn)

        count_sql = f"""
            SELECT COUNT(*) AS TOTAL,
                   MIN(FECHA_PROGRAMADA) AS PRIMERA,
                   MAX(FECHA_PROGRAMADA) AS ULTIMA
            FROM {SCHEMA}.PLAN_MANTENIMIENTO
            WHERE NUMERO_SERIE = '{serie_safe}' AND ESTADO = 'PENDIENTE'
        """
        row = ibm_db.fetch_assoc(ibm_db.exec_immediate(conn, count_sql))

        return {
            "status": "ok",
            "numero_serie": numero_serie,
            "meses_planificados": meses,
            "servicios_generados": row["TOTAL"] if row else 0,
            "primer_servicio": str(row["PRIMERA"]) if row and row["PRIMERA"] else None,
            "ultimo_servicio": str(row["ULTIMA"]) if row and row["ULTIMA"] else None,
        }
    except Exception as e:
        ibm_db.rollback(conn)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)


# ----------------------------------------------------------
# 2. GENERAR PLAN PARA TODA LA FLOTA
# ----------------------------------------------------------
from datetime import date, timedelta
from fastapi import APIRouter, Query, HTTPException
import ibm_db

router = APIRouter()

CHECKLIST_POR_CATEGORIA = {
    'Eléctrico / Electrónico':    '|EXTRA: Termografía eléctrica|Revisar arnés completo|Prueba alternador',
    'Enfriamiento':               '|EXTRA: Inspección radiador|Test termostato|Verificar coolant|Mangueras enfriamiento',
    'Hidráulico':                 '|EXTRA: Análisis aceite hidráulico|Mangueras HP|Bomba hidráulica|Sellos cilindros',
    'Arranque / Batería':         '|EXTRA: Prueba carga batería|Test marcha|Conexiones|Bornes',
    'Mástil / Elevación':         '|EXTRA: Cadenas mástil|Rodillos|Cilindros elevación|Aceite mástil',
    'Dirección':                  '|EXTRA: Cilindro dirección|Articulaciones|Aceite dirección|Holguras',
    'Frenos':                     '|EXTRA: Pastillas/zapatas|Prueba frenado|Discos|Líquido frenos',
    'Escape / Mofle':             '|EXTRA: Inspección escape|Mofle|Manifold|Juntas',
    'Diagnóstico / Habilitación': '|EXTRA: Diagnóstico completo sistemas|Verificar códigos falla',
}


def _fetch_all(stmt) -> list:
    """Convierte un resultado ibm_db en lista de dicts."""
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append(dict(row))
        row = ibm_db.fetch_assoc(stmt)
    return rows


def _cargar_politicas(conn) -> dict:
    """Políticas agrupadas por NIVEL_RIESGO."""
    stmt = ibm_db.exec_immediate(
        conn,
        f"SELECT * FROM {SCHEMA}.POLITICA_MANTENIMIENTO WHERE ACTIVO = 1"
    )
    pol_by_nivel = {}
    for p in _fetch_all(stmt):
        pol_by_nivel.setdefault(p['NIVEL_RIESGO'], []).append(p)
    return pol_by_nivel


def _cargar_equipos(conn, numero_serie: str = None) -> list:
    """
    Lee RISK_SCORE (ya tiene todo lo que necesita el motor de plan).
    Filtra por equipo si se pasa numero_serie.
    """
    filtro = ""
    if numero_serie:
        serie_safe = numero_serie.replace("'", "''")
        filtro = f"WHERE NUMERO_SERIE = '{serie_safe}'"

    sql = f"""
        SELECT
            NUMERO_SERIE      AS SERIE,
            NIVEL_RIESGO      AS NIVEL,
            RISK_SCORE        AS RISK_SCORE,
            ULTIMO_HOROMETRO  AS ULTIMO_HORO,
            PROMEDIO_USO_DIA  AS AVG_USO,
            CATEGORIA_DOMINANTE AS CAT_DOMINANTE
        FROM {SCHEMA}.RISK_SCORE
        {filtro}
    """
    stmt = ibm_db.exec_immediate(conn, sql)
    return _fetch_all(stmt)


def _generar_plan(conn, equipos: list, pol_by_nivel: dict, meses: int) -> int:
    """Núcleo compartido. Inserta en PLAN_MANTENIMIENTO y devuelve total insertado."""
    fecha_hoy    = date.today()
    fecha_limite = fecha_hoy + timedelta(days=meses * 30)

    sql_insert = f"""
        INSERT INTO {SCHEMA}.PLAN_MANTENIMIENTO
        (NUMERO_SERIE, NIVEL_RIESGO_ASIGNADO, RISK_SCORE_AL_GENERAR,
         TIPO_MANTENIMIENTO, FECHA_PROGRAMADA, HOROMETRO_ESTIMADO,
         ALCANCE, CHECKLIST, PRIORIDAD, ESTADO)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDIENTE')
    """
    stmt_insert = ibm_db.prepare(conn, sql_insert)

    total = 0

    for eq in equipos:
        serie = eq['SERIE']
        nivel = str(eq['NIVEL'])
        risk  = float(eq['RISK_SCORE'] or 0)
        horo  = float(eq['ULTIMO_HORO'] or 0)
        uso   = float(eq['AVG_USO']) if eq['AVG_USO'] and float(eq['AVG_USO']) > 0 else 4.0
        cat   = str(eq['CAT_DOMINANTE']) if eq['CAT_DOMINANTE'] else None

        pols = pol_by_nivel.get(nivel, pol_by_nivel.get('BAJO', []))

        for pol in pols:
            int_horas    = pol['INTERVALO_HORAS']
            int_dias_max = pol['INTERVALO_DIAS_MAX']
            alcance      = pol['ALCANCE'] or ''
            checklist    = pol['CHECKLIST_EXTRA'] or ''

            if cat and cat != 'None':
                alcance   += f' + FOCO EN: {cat}'
                checklist += CHECKLIST_POR_CATEGORIA.get(cat, '')

            dias_por_uso   = int(int_horas / uso) if uso > 0 else int_dias_max
            dias_intervalo = max(min(dias_por_uso, int_dias_max), 3)

            fecha_next = fecha_hoy + timedelta(days=dias_intervalo)
            while fecha_next <= fecha_limite:
                horo_est = horo + (fecha_next - fecha_hoy).days * uso
                try:
                    ibm_db.execute(stmt_insert, (
                        serie, nivel, risk,
                        pol['TIPO_MANTENIMIENTO'],
                        fecha_next.strftime('%Y-%m-%d'),
                        round(horo_est, 1),
                        alcance[:1000],
                        checklist[:1000],
                        pol['PRIORIDAD'],
                    ))
                    total += 1
                except Exception:
                    pass
                fecha_next += timedelta(days=dias_intervalo)

    ibm_db.commit(conn)
    return total


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 1 — Toda la flota
# ─────────────────────────────────────────────────────────────────────────────
@router.post(
    "/plan/generar-flota",
    summary="Generar plan predictivo para toda la flota",
    description="Genera planes de mantenimiento para TODOS los equipos activos. "
                "Cada equipo obtiene su propio plan según su nivel de riesgo.",
    tags=["Plan Predictivo"],
)
def generar_plan_flota(
    meses: int = Query(6, ge=1, le=12, description="Horizonte de planificación en meses"),
):
    conn = get_db2_connection()
    try:
        ibm_db.exec_immediate(
            conn,
            f"DELETE FROM {SCHEMA}.PLAN_MANTENIMIENTO WHERE ESTADO = 'PENDIENTE'"
        )
        ibm_db.commit(conn)

        pol_by_nivel = _cargar_politicas(conn)
        equipos      = _cargar_equipos(conn)

        if not equipos:
            raise HTTPException(status_code=404, detail="No hay equipos con risk score calculado")

        total = _generar_plan(conn, equipos, pol_by_nivel, meses)

        resumen = execute_query(f"""
            SELECT NIVEL_RIESGO_ASIGNADO, COUNT(*) AS SERVICIOS,
                   COUNT(DISTINCT NUMERO_SERIE) AS EQUIPOS
            FROM {SCHEMA}.PLAN_MANTENIMIENTO
            WHERE ESTADO = 'PENDIENTE'
            GROUP BY NIVEL_RIESGO_ASIGNADO
            ORDER BY
                CASE NIVEL_RIESGO_ASIGNADO
                    WHEN 'CRITICO' THEN 1
                    WHEN 'ALTO'    THEN 2
                    WHEN 'MEDIO'   THEN 3
                    ELSE 4
                END
        """)

        return {
            "status": "ok",
            "meses_planificados": meses,
            "equipos_planificados": len(equipos),
            "total_servicios": total,
            "resumen_por_nivel": resumen,
        }
    except HTTPException:
        raise
    except Exception as e:
        ibm_db.rollback(conn)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT 2 — Un solo equipo
# ─────────────────────────────────────────────────────────────────────────────
@router.post(
    "/plan/generar/{numero_serie}",
    summary="Generar plan predictivo para un equipo",
    description="Genera el plan de mantenimiento basado en el nivel de riesgo del equipo.",
    tags=["Plan Predictivo"],
)
def generar_plan_equipo(
    numero_serie: str,
    meses: int = Query(6, ge=1, le=12, description="Horizonte de planificación en meses"),
):
    conn = get_db2_connection()
    try:
        equipos = _cargar_equipos(conn, numero_serie=numero_serie)
        if not equipos:
            raise HTTPException(
                status_code=404,
                detail=f"Equipo '{numero_serie}' no encontrado en RISK_SCORE"
            )

        serie_safe = numero_serie.replace("'", "''")
        ibm_db.exec_immediate(
            conn,
            f"DELETE FROM {SCHEMA}.PLAN_MANTENIMIENTO "
            f"WHERE NUMERO_SERIE = '{serie_safe}' AND ESTADO = 'PENDIENTE'"
        )
        ibm_db.commit(conn)

        pol_by_nivel = _cargar_politicas(conn)
        total        = _generar_plan(conn, equipos, pol_by_nivel, meses)

        stmt = ibm_db.exec_immediate(conn, f"""
            SELECT COUNT(*) AS TOTAL,
                   MIN(FECHA_PROGRAMADA) AS PRIMERA,
                   MAX(FECHA_PROGRAMADA) AS ULTIMA
            FROM {SCHEMA}.PLAN_MANTENIMIENTO
            WHERE NUMERO_SERIE = '{serie_safe}' AND ESTADO = 'PENDIENTE'
        """)
        row = ibm_db.fetch_assoc(stmt)

        return {
            "status": "ok",
            "numero_serie": numero_serie,
            "nivel_riesgo": equipos[0]['NIVEL'],
            "risk_score": float(equipos[0]['RISK_SCORE'] or 0),
            "meses_planificados": meses,
            "servicios_generados": int(row["TOTAL"]) if row else 0,
            "primer_servicio": str(row["PRIMERA"]) if row and row["PRIMERA"] else None,
            "ultimo_servicio":  str(row["ULTIMA"])  if row and row["ULTIMA"]  else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        ibm_db.rollback(conn)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        ibm_db.close(conn)


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
