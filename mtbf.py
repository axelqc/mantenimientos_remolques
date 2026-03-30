"""
router: mtbf.py
Montar en main.py:  app.include_router(mtbf_router)

Calcula MTBF real desde CORRECTIVOS y opcionalmente actualiza MEANFTB.

Fórmulas:
  MTBF      = (fecha_ultima - fecha_primera) / (eventos - 1)
             Si solo 1 evento: 365 / eventos  (estimado conservador)
  Próx falla = fecha_ultima + MTBF días
  Inspección = fecha_ultima + (MTBF * 0.8)   (20% antes de la falla estimada)
  Riesgo:   MTBF < 30d → ALTO | 30-45d → MEDIO | >45d → BAJO
"""

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from typing import Optional
import ibm_db_dbi as db2
from datetime import date, timedelta
from collections import Counter
import os

mtbf_router = APIRouter(prefix="/mtbf", tags=["MTBF / Predictivo"])

SCHEMA = os.getenv("DB2_SCHEMA", "MSB_LEON")

# ─── Conexión DB2 ─────────────────────────────────────────────────────────────
def get_db2_conn():
    conn_str = (
        f"DATABASE={os.getenv('DB2_DATABASE')};"
        f"HOSTNAME={os.getenv('DB2_HOST')};"
        f"PORT={os.getenv('DB2_PORT', '50000')};"
        f"PROTOCOL=TCPIP;"
        f"UID={os.getenv('DB2_USER')};"
        f"PWD={os.getenv('DB2_PASSWORD')};"
    )
    conn = db2.connect(conn_str, "", "")
    try:
        yield conn
    finally:
        conn.close()

# ─── Schemas ──────────────────────────────────────────────────────────────────
class EquipoMTBF(BaseModel):
    numero_serie:        str
    eventos:             int
    primera_falla:       Optional[date]
    ultima_falla:        Optional[date]
    mtbf_dias:           float
    fecha_inspeccion:    Optional[date]   # MTBF * 0.8 desde última falla
    fecha_prox_falla:    Optional[date]   # última falla + MTBF
    riesgo:              str              # ALTO / MEDIO / BAJO
    categoria_frecuente: str              # categoría de falla más común
    promedio_uso_dia:    Optional[float]  # desde PREVENTIVOS
    estimado_calculado:  bool             # True si solo 1 evento (no hay rango real)

class MTBFResponse(BaseModel):
    fecha_calculo:   date
    periodo_analisis:str
    total_equipos:   int
    equipos_alto:    int
    equipos_medio:   int
    equipos_bajo:    int
    equipos:         list[EquipoMTBF]

# ─── Helpers ──────────────────────────────────────────────────────────────────
def clasificar_riesgo(mtbf: float) -> str:
    if mtbf < 30:
        return "ALTO"
    elif mtbf <= 45:
        return "MEDIO"
    return "BAJO"

def categoria_mas_frecuente(categorias: list[str]) -> str:
    if not categorias:
        return "SIN DATOS"
    return Counter(categorias).most_common(1)[0][0]

# ─── GET /mtbf/calcular ───────────────────────────────────────────────────────
@mtbf_router.get("/calcular", response_model=MTBFResponse)
def calcular_mtbf(
    anio: int = Query(default=2025, description="Año a analizar. Default: 2025."),
    min_eventos: int = Query(default=1, ge=1, description="Mínimo de eventos para incluir equipo."),
    solo_alto_riesgo: bool = Query(default=False, description="Si True, retorna solo equipos ALTO riesgo."),
    conn=Depends(get_db2_conn),
):
    """
    Calcula MTBF por equipo (NUMERO_SERIE) desde CORRECTIVOS.
    Enriquece con PROMEDIO_USO_DIA de PREVENTIVOS.
    """
    cursor = conn.cursor()

    # ── 1. Traer correctivos del año filtrado ─────────────────────────────────
    cursor.execute(f"""
        SELECT
            NUMERO_SERIE,
            FECHA_SERVICIO,
            CATEGORIA_FALLA
        FROM {SCHEMA}.CORRECTIVOS
        WHERE YEAR(FECHA_SERVICIO) = ?
          AND FECHA_SERVICIO IS NOT NULL
          AND TRIM(NUMERO_SERIE) <> ''
          AND NUMERO_SERIE IS NOT NULL
        ORDER BY NUMERO_SERIE, FECHA_SERVICIO
    """, (anio,))

    rows = cursor.fetchall()

    # ── 2. Agrupar en Python por NUMERO_SERIE ─────────────────────────────────
    from collections import defaultdict
    grupos: dict[str, dict] = defaultdict(lambda: {
        "fechas": [], "categorias": []
    })

    for numero_serie, fecha_servicio, categoria in rows:
        ns = numero_serie.strip().upper()
        grupos[ns]["fechas"].append(fecha_servicio)
        if categoria:
            grupos[ns]["categorias"].append(categoria.strip())

    # ── 3. Traer PROMEDIO_USO_DIA de PREVENTIVOS (último registro por equipo) ─
    cursor.execute(f"""
        SELECT NUMERO_SERIE, PROMEDIO_USO_DIA
        FROM {SCHEMA}.PREVENTIVOS
        WHERE PROMEDIO_USO_DIA IS NOT NULL
          AND PROMEDIO_USO_DIA > 0
          AND NUMERO_SERIE IS NOT NULL
        FETCH FIRST 5000 ROWS ONLY
    """)
    # Tomar promedio de uso por equipo
    uso_map: dict[str, list[float]] = defaultdict(list)
    for ns, uso in cursor.fetchall():
        if uso and uso > 0:
            uso_map[ns.strip().upper()].append(float(uso))

    promedio_uso: dict[str, float] = {
        ns: round(sum(vals) / len(vals), 2)
        for ns, vals in uso_map.items()
    }

    # ── 4. Calcular MTBF por equipo ───────────────────────────────────────────
    resultado: list[EquipoMTBF] = []

    for numero_serie, data in grupos.items():
        fechas     = sorted(data["fechas"])
        categorias = data["categorias"]
        eventos    = len(fechas)

        if eventos < min_eventos:
            continue

        primera = fechas[0]
        ultima  = fechas[-1]

        # MTBF real si hay más de 1 evento, estimado si solo hay 1
        if eventos > 1:
            rango_dias      = (ultima - primera).days
            mtbf            = round(rango_dias / (eventos - 1), 1)
            estimado        = False
        else:
            # Solo 1 evento: estimado conservador = 365 / 1 = 365 días (bajo riesgo por definición)
            mtbf            = round(365.0 / eventos, 1)
            estimado        = True

        fecha_prox_falla  = ultima + timedelta(days=mtbf)
        fecha_inspeccion  = ultima + timedelta(days=mtbf * 0.8)

        resultado.append(EquipoMTBF(
            numero_serie=numero_serie,
            eventos=eventos,
            primera_falla=primera,
            ultima_falla=ultima,
            mtbf_dias=mtbf,
            fecha_inspeccion=fecha_inspeccion,
            fecha_prox_falla=fecha_prox_falla,
            riesgo=clasificar_riesgo(mtbf),
            categoria_frecuente=categoria_mas_frecuente(categorias),
            promedio_uso_dia=promedio_uso.get(numero_serie),
            estimado_calculado=estimado,
        ))

    if solo_alto_riesgo:
        resultado = [e for e in resultado if e.riesgo == "ALTO"]

    # Ordenar: ALTO → MEDIO → BAJO, luego más eventos primero
    orden_riesgo = {"ALTO": 0, "MEDIO": 1, "BAJO": 2}
    resultado.sort(key=lambda x: (orden_riesgo[x.riesgo], -x.eventos))

    return MTBFResponse(
        fecha_calculo=date.today(),
        periodo_analisis=str(anio),
        total_equipos=len(resultado),
        equipos_alto=sum(1 for e in resultado if e.riesgo == "ALTO"),
        equipos_medio=sum(1 for e in resultado if e.riesgo == "MEDIO"),
        equipos_bajo=sum(1 for e in resultado if e.riesgo == "BAJO"),
        equipos=resultado,
    )


# ─── POST /mtbf/actualizar-meanftb ────────────────────────────────────────────
@mtbf_router.post("/actualizar-meanftb")
def actualizar_meanftb(
    anio: int = Query(default=2025),
    conn=Depends(get_db2_conn),
):
    """
    Recalcula MTBF desde CORRECTIVOS y actualiza (o inserta) registros en MEANFTB.
    Úsalo para mantener MEANFTB sincronizado con el historial real.
    """
    # Reutilizar lógica de cálculo
    from fastapi.testclient import TestClient  # solo para reusar lógica interna

    # Calcular directo sin pasar por HTTP
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT NUMERO_SERIE, FECHA_SERVICIO, CATEGORIA_FALLA
        FROM {SCHEMA}.CORRECTIVOS
        WHERE YEAR(FECHA_SERVICIO) = ?
          AND FECHA_SERVICIO IS NOT NULL
          AND NUMERO_SERIE IS NOT NULL
        ORDER BY NUMERO_SERIE, FECHA_SERVICIO
    """, (anio,))

    from collections import defaultdict
    grupos = defaultdict(lambda: {"fechas": [], "categorias": []})
    for ns, fecha, cat in cursor.fetchall():
        ns = ns.strip().upper()
        grupos[ns]["fechas"].append(fecha)
        if cat:
            grupos[ns]["categorias"].append(cat.strip())

    actualizados = 0
    insertados   = 0

    for numero_serie, data in grupos.items():
        fechas  = sorted(data["fechas"])
        eventos = len(fechas)
        ultima  = fechas[-1]
        primera = fechas[0]

        mtbf = round((ultima - primera).days / (eventos - 1), 1) if eventos > 1 else 365.0
        fecha_prox  = ultima + timedelta(days=mtbf)
        fecha_insp  = ultima + timedelta(days=mtbf * 0.8)
        riesgo      = clasificar_riesgo(mtbf)
        categoria   = categoria_mas_frecuente(data["categorias"])

        # Verificar si ya existe en MEANFTB
        cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.MEANFTB WHERE EQUIPO = ?", (numero_serie,))
        existe = cursor.fetchone()[0] > 0

        if existe:
            cursor.execute(f"""
                UPDATE {SCHEMA}.MEANFTB SET
                    EVENTOS_2025         = ?,
                    MTBF_DIAS            = ?,
                    CATEGORIA_PROBABLE   = ?,
                    FECHA_INSPECCION     = ?,
                    FECHA_PROX_FALLA     = ?,
                    RIESGO               = ?,
                    FECHA_ULT_CORRECTIVO = ?
                WHERE EQUIPO = ?
            """, (eventos, mtbf, categoria, fecha_insp, fecha_prox, riesgo, ultima, numero_serie))
            actualizados += 1
        else:
            cursor.execute(f"""
                INSERT INTO {SCHEMA}.MEANFTB
                    (EQUIPO, EVENTOS_2025, MTBF_DIAS, CATEGORIA_PROBABLE,
                     FALLA_PROBABLE, FECHA_INSPECCION, FECHA_PROX_FALLA,
                     RIESGO, FECHA_ULT_CORRECTIVO)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (numero_serie, eventos, mtbf, categoria,
                  categoria_mas_frecuente(data["categorias"]),
                  fecha_insp, fecha_prox, riesgo, ultima))
            insertados += 1

    conn.commit()

    return {
        "status": "ok",
        "anio_analizado": anio,
        "registros_actualizados": actualizados,
        "registros_insertados": insertados,
        "total_procesados": actualizados + insertados,
    }
