"""
router: stock_preventivo.py
Montar en main.py:  app.include_router(stock_router)

Lógica:
  1. Detectar equipos con falla próxima en 30 días (MEANFTB, riesgo ALTO o MEDIO)
  2. Buscar en CORRECTIVOS qué refacciones usó cada equipo históricamente
  3. Cruzar con NUMSPARTE para obtener stock mínimo
  4. Retornar lista de partes sugeridas con cantidad y contexto de equipos en riesgo
"""

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from typing import Optional
import ibm_db_dbi as db2
from datetime import date, timedelta
from collections import defaultdict
import os

stock_router = APIRouter(prefix="/stock", tags=["Stock Preventivo"])

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
class EquipoEnRiesgo(BaseModel):
    numero_serie:     str
    mtbf_dias:        float
    riesgo:           str
    fecha_prox_falla: Optional[date]
    dias_restantes:   int
    categoria:        str
    falla_probable:   str

class ParteSugerida(BaseModel):
    part_number:              str
    categoria:                str
    descripcion_falla:        str
    usos_historicos:          int       # veces que apareció en CORRECTIVOS para estos equipos
    stock_minimo:             int       # de NUMSPARTE
    cantidad_sugerida:        int       # max(usos_historicos, stock_minimo)
    equipos_en_riesgo:        list[EquipoEnRiesgo]
    prioridad:                str       # CRITICA / ALTA / MEDIA

class StockPreventivoResponse(BaseModel):
    fecha_calculo:          date
    horizonte_dias:         int
    equipos_en_riesgo:      int
    partes_sugeridas:       int
    partes:                 list[ParteSugerida]

# ─── Prioridad ────────────────────────────────────────────────────────────────
PRIORIDAD_CATEGORIA = {
    "ARRANQUE / POTENCIA (BATERÍA / CARGADOR)": "CRITICA",
    "FRENOS":                                   "CRITICA",
    "ENFRIAMIENTO / CALENTAMIENTO":             "ALTA",
    "DIRECCIÓN / EJE":                          "ALTA",
    "ELÉCTRICO / CONTROLES":                    "MEDIA",
    "HIDRÁULICO (FUGAS / MANGUERAS / CILINDROS)":"MEDIA",
}

def get_prioridad(categoria: str, riesgo: str) -> str:
    base = PRIORIDAD_CATEGORIA.get(categoria.strip(), "MEDIA")
    # Si el equipo es ALTO riesgo y la parte es MEDIA, sube a ALTA
    if riesgo == "ALTO" and base == "MEDIA":
        return "ALTA"
    return base

# ─── GET /stock/preventivo ────────────────────────────────────────────────────
@stock_router.get("/preventivo", response_model=StockPreventivoResponse)
def stock_preventivo(
    horizonte_dias: int = Query(default=30, ge=7, le=90,
        description="Días a futuro para detectar fallas. Default: 30."),
    conn=Depends(get_db2_conn),
):
    """
    Genera recomendación de stock preventivo:
    - Equipos con FECHA_PROX_FALLA en los próximos {horizonte_dias} días
    - Riesgo ALTO (MTBF < 30d) o MEDIO (MTBF 30-45d)
    - Refacciones históricamente usadas por esos equipos
    - Cruzadas con catálogo NUMSPARTE para stock mínimo
    """
    cursor = conn.cursor()
    hoy         = date.today()
    fecha_limite = hoy + timedelta(days=horizonte_dias)

    # ── 1. Equipos en riesgo desde MEANFTB ────────────────────────────────────
    cursor.execute(f"""
        SELECT
            EQUIPO,
            MTBF_DIAS,
            RIESGO,
            FECHA_PROX_FALLA,
            CATEGORIA_PROBABLE,
            FALLA_PROBABLE
        FROM {SCHEMA}.MEANFTB
        WHERE RIESGO IN ('ALTO', 'MEDIO')
          AND FECHA_PROX_FALLA IS NOT NULL
          AND FECHA_PROX_FALLA <= '{fecha_limite.strftime('%Y-%m-%d')}'
        ORDER BY FECHA_PROX_FALLA ASC
    """)

    equipos_riesgo_rows = cursor.fetchall()

    if not equipos_riesgo_rows:
        return StockPreventivoResponse(
            fecha_calculo=hoy,
            horizonte_dias=horizonte_dias,
            equipos_en_riesgo=0,
            partes_sugeridas=0,
            partes=[],
        )

    # Construir lista de equipos en riesgo
    equipos_en_riesgo: list[EquipoEnRiesgo] = []
    numeros_serie: list[str] = []

    for equipo, mtbf, riesgo, fecha_prox, categoria, falla in equipos_riesgo_rows:
        dias_restantes = (fecha_prox - hoy).days if fecha_prox else 0
        equipos_en_riesgo.append(EquipoEnRiesgo(
            numero_serie=equipo,
            mtbf_dias=float(mtbf or 0),
            riesgo=riesgo or "",
            fecha_prox_falla=fecha_prox,
            dias_restantes=dias_restantes,
            categoria=categoria or "",
            falla_probable=falla or "",
        ))
        numeros_serie.append(equipo.strip().upper())

    # ── 2. Historial de CORRECTIVOS para esos equipos ─────────────────────────
    # Traer todas las refacciones usadas históricamente por estos equipos
    placeholders = ", ".join(["?" for _ in numeros_serie])
    cursor.execute(f"""
        SELECT
            UPPER(TRIM(NUMERO_SERIE)),
            REFACCIONES
        FROM {SCHEMA}.CORRECTIVOS
        WHERE UPPER(TRIM(NUMERO_SERIE)) IN ({placeholders})
          AND REFACCIONES IS NOT NULL
          AND TRIM(REFACCIONES) <> ''
          AND UPPER(TRIM(REFACCIONES)) <> 'NINGUNA'
    """, numeros_serie)

    correctivos_rows = cursor.fetchall()

    # ── 3. Catálogo de partes desde NUMSPARTE ─────────────────────────────────
    cursor.execute(f"""
        SELECT
            NO__PARTE,
            "Categoría asociada",
            FALLAS_ASOCIADAS__TOP2_,
            EQUIPOS_FRECUENTES__TOP3_,
            STOCK_MIN_SUGERIDO
        FROM {SCHEMA}.NUMSPARTE
    """)
    catalogo_rows = cursor.fetchall()

    # Índice: part_number → datos del catálogo
    catalogo: dict[str, dict] = {}
    for part, cat, fallas, equipos_top, stock_min in catalogo_rows:
        catalogo[part.strip().upper()] = {
            "categoria":    cat or "",
            "fallas":       fallas or "",
            "equipos_top":  equipos_top or "",
            "stock_minimo": int(stock_min or 0),
        }

    all_part_numbers = set(catalogo.keys())

    # ── 4. Detectar part numbers en REFACCIONES (búsqueda en texto) ───────────
    # Para cada correctivo, buscar qué part numbers del catálogo aparecen en el texto
    # Estructura: { part_number: { equipo: count } }
    uso_por_parte: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for numero_serie, refacciones_texto in correctivos_rows:
        texto_upper = refacciones_texto.upper()
        for part_number in all_part_numbers:
            if part_number in texto_upper:
                uso_por_parte[part_number][numero_serie] += 1

    # ── 5. Construir sugerencias por parte ────────────────────────────────────
    partes_sugeridas: list[ParteSugerida] = []

    for part_number, equipos_uso in uso_por_parte.items():
        # Solo incluir partes usadas por equipos actualmente en riesgo
        equipos_en_riesgo_para_parte = [
            e for e in equipos_en_riesgo
            if e.numero_serie in equipos_uso
        ]
        if not equipos_en_riesgo_para_parte:
            continue

        usos_total  = sum(equipos_uso[e.numero_serie] for e in equipos_en_riesgo_para_parte)
        datos_cat   = catalogo[part_number]
        stock_min   = datos_cat["stock_minimo"]
        cantidad    = max(usos_total, stock_min)
        categoria   = datos_cat["categoria"]

        # Riesgo más alto entre los equipos que usan esta parte
        riesgo_max = "MEDIO"
        if any(e.riesgo == "ALTO" for e in equipos_en_riesgo_para_parte):
            riesgo_max = "ALTO"

        partes_sugeridas.append(ParteSugerida(
            part_number=part_number,
            categoria=categoria,
            descripcion_falla=datos_cat["fallas"],
            usos_historicos=usos_total,
            stock_minimo=stock_min,
            cantidad_sugerida=cantidad,
            equipos_en_riesgo=equipos_en_riesgo_para_parte,
            prioridad=get_prioridad(categoria, riesgo_max),
        ))

    # Ordenar: CRITICA → ALTA → MEDIA, luego más usos primero
    orden_prioridad = {"CRITICA": 0, "ALTA": 1, "MEDIA": 2}
    partes_sugeridas.sort(key=lambda x: (
        orden_prioridad.get(x.prioridad, 9),
        -x.usos_historicos,
    ))

    return StockPreventivoResponse(
        fecha_calculo=hoy,
        horizonte_dias=horizonte_dias,
        equipos_en_riesgo=len(equipos_en_riesgo),
        partes_sugeridas=len(partes_sugeridas),
        partes=partes_sugeridas,
    )
