"""
router: mtbf.py
Calcula MTBF dinámico desde CORRECTIVOS y actualiza MEANFTB.
Usa execute_query / get_schema de db.py — mismo patrón que main.py
"""

from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta, datetime
from collections import defaultdict, Counter
from db import execute_query, get_schema

mtbf_router = APIRouter(prefix="/mtbf", tags=["MTBF / Predictivo"])

# ─── Schemas ──────────────────────────────────────────────────────────────────
class EquipoMTBF(BaseModel):
    numero_serie:        str
    eventos:             int
    primera_falla:       Optional[str]
    ultima_falla:        Optional[str]
    mtbf_dias:           float
    fecha_inspeccion:    Optional[str]
    fecha_prox_falla:    Optional[str]
    riesgo:              str
    categoria_frecuente: str
    promedio_uso_dia:    Optional[float]
    estimado_calculado:  bool

class MTBFResponse(BaseModel):
    fecha_calculo:    str
    periodo_analisis: str
    total_equipos:    int
    equipos_alto:     int
    equipos_medio:    int
    equipos_bajo:     int
    equipos:          list[EquipoMTBF]

# ─── Helpers ──────────────────────────────────────────────────────────────────
def clasificar_riesgo(mtbf: float) -> str:
    if mtbf < 30:  return "ALTO"
    if mtbf <= 45: return "MEDIO"
    return "BAJO"

def parse_fecha(val) -> Optional[date]:
    if val is None: return None
    if isinstance(val, date): return val
    if isinstance(val, str):
        try: return datetime.fromisoformat(val[:10]).date()
        except: return None
    return None

# ─── GET /mtbf/calcular ───────────────────────────────────────────────────────
@mtbf_router.get("/calcular", response_model=MTBFResponse)
def calcular_mtbf(
    anio: int = Query(default=2025),
    min_eventos: int = Query(default=1, ge=1),
    solo_alto_riesgo: bool = Query(default=False),
):
    S = get_schema()

    rows = execute_query(f"""
        SELECT NUMERO_SERIE, FECHA_SERVICIO, CATEGORIA_FALLA
        FROM {S}.CORRECTIVOS
        WHERE YEAR(FECHA_SERVICIO) = ?
          AND FECHA_SERVICIO IS NOT NULL
          AND NUMERO_SERIE IS NOT NULL
        ORDER BY NUMERO_SERIE, FECHA_SERVICIO
    """, (anio,))

    grupos = defaultdict(lambda: {"fechas": [], "categorias": []})
    for r in rows:
        ns = (r["NUMERO_SERIE"] or "").strip().upper()
        f  = parse_fecha(r["FECHA_SERVICIO"])
        if ns and f:
            grupos[ns]["fechas"].append(f)
            if r["CATEGORIA_FALLA"]:
                grupos[ns]["categorias"].append(r["CATEGORIA_FALLA"].strip())

    prev_rows = execute_query(f"""
        SELECT NUMERO_SERIE, PROMEDIO_USO_DIA
        FROM {S}.PREVENTIVOS
        WHERE PROMEDIO_USO_DIA IS NOT NULL AND PROMEDIO_USO_DIA > 0
    """)
    uso_map = defaultdict(list)
    for r in prev_rows:
        ns = (r["NUMERO_SERIE"] or "").strip().upper()
        uso_map[ns].append(float(r["PROMEDIO_USO_DIA"]))
    promedio_uso = {ns: round(sum(v)/len(v), 2) for ns, v in uso_map.items()}

    resultado = []
    for ns, data in grupos.items():
        fechas  = sorted(data["fechas"])
        eventos = len(fechas)
        if eventos < min_eventos:
            continue
        primera = fechas[0]
        ultima  = fechas[-1]
        if eventos > 1:
            mtbf     = round((ultima - primera).days / (eventos - 1), 1)
            estimado = False
        else:
            mtbf     = 365.0
            estimado = True

        categoria = Counter(data["categorias"]).most_common(1)[0][0] if data["categorias"] else "SIN DATOS"
        resultado.append(EquipoMTBF(
            numero_serie=ns,
            eventos=eventos,
            primera_falla=primera.isoformat(),
            ultima_falla=ultima.isoformat(),
            mtbf_dias=mtbf,
            fecha_inspeccion=(ultima + timedelta(days=mtbf * 0.8)).isoformat(),
            fecha_prox_falla=(ultima + timedelta(days=mtbf)).isoformat(),
            riesgo=clasificar_riesgo(mtbf),
            categoria_frecuente=categoria,
            promedio_uso_dia=promedio_uso.get(ns),
            estimado_calculado=estimado,
        ))

    if solo_alto_riesgo:
        resultado = [e for e in resultado if e.riesgo == "ALTO"]

    orden = {"ALTO": 0, "MEDIO": 1, "BAJO": 2}
    resultado.sort(key=lambda x: (orden[x.riesgo], -x.eventos))

    return MTBFResponse(
        fecha_calculo=date.today().isoformat(),
        periodo_analisis=str(anio),
        total_equipos=len(resultado),
        equipos_alto=sum(1 for e in resultado if e.riesgo == "ALTO"),
        equipos_medio=sum(1 for e in resultado if e.riesgo == "MEDIO"),
        equipos_bajo=sum(1 for e in resultado if e.riesgo == "BAJO"),
        equipos=resultado,
    )


# ─── POST /mtbf/actualizar-meanftb ───────────────────────────────────────────
@mtbf_router.post("/actualizar-meanftb")
def actualizar_meanftb(anio: int = Query(default=2025)):
    S = get_schema()

    rows = execute_query(f"""
        SELECT NUMERO_SERIE, FECHA_SERVICIO, CATEGORIA_FALLA
        FROM {S}.CORRECTIVOS
        WHERE YEAR(FECHA_SERVICIO) = ?
          AND FECHA_SERVICIO IS NOT NULL AND NUMERO_SERIE IS NOT NULL
        ORDER BY NUMERO_SERIE, FECHA_SERVICIO
    """, (anio,))

    grupos = defaultdict(lambda: {"fechas": [], "categorias": []})
    for r in rows:
        ns = (r["NUMERO_SERIE"] or "").strip().upper()
        f  = parse_fecha(r["FECHA_SERVICIO"])
        if ns and f:
            grupos[ns]["fechas"].append(f)
            if r["CATEGORIA_FALLA"]:
                grupos[ns]["categorias"].append(r["CATEGORIA_FALLA"].strip())

    actualizados = insertados = 0
    for ns, data in grupos.items():
        fechas  = sorted(data["fechas"])
        eventos = len(fechas)
        ultima  = fechas[-1]
        primera = fechas[0]
        mtbf    = round((ultima - primera).days / (eventos - 1), 1) if eventos > 1 else 365.0
        prox    = (ultima + timedelta(days=mtbf)).isoformat()
        insp    = (ultima + timedelta(days=mtbf * 0.8)).isoformat()
        riesgo  = clasificar_riesgo(mtbf)
        cat     = Counter(data["categorias"]).most_common(1)[0][0] if data["categorias"] else ""

        existe = execute_query(f"SELECT COUNT(*) AS CNT FROM {S}.MEANFTB WHERE EQUIPO = ?", (ns,))
        if existe and existe[0]["CNT"] > 0:
            execute_query(f"""
                UPDATE {S}.MEANFTB SET
                    EVENTOS_2025 = ?, MTBF_DIAS = ?, CATEGORIA_PROBABLE = ?,
                    FECHA_INSPECCION = ?, FECHA_PROX_FALLA = ?,
                    RIESGO = ?, FECHA_ULT_CORRECTIVO = ?
                WHERE EQUIPO = ?
            """, (eventos, mtbf, cat, insp, prox, riesgo, ultima.isoformat(), ns))
            actualizados += 1
        else:
            execute_query(f"""
                INSERT INTO {S}.MEANFTB
                    (EQUIPO, EVENTOS_2025, MTBF_DIAS, CATEGORIA_PROBABLE, FALLA_PROBABLE,
                     FECHA_INSPECCION, FECHA_PROX_FALLA, RIESGO, FECHA_ULT_CORRECTIVO)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ns, eventos, mtbf, cat, cat, insp, prox, riesgo, ultima.isoformat()))
            insertados += 1

    return {
        "status": "ok",
        "anio_analizado": anio,
        "registros_actualizados": actualizados,
        "registros_insertados": insertados,
        "total_procesados": actualizados + insertados,
    }
