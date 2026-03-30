"""
router: stock_preventivo.py
Stock preventivo cruzando MEANFTB + CORRECTIVOS + NUMSPARTE.
Usa execute_query / get_schema de db.py — mismo patrón que main.py
"""

from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta
from collections import defaultdict
from db import execute_query, get_schema

stock_router = APIRouter(prefix="/stock", tags=["Stock Preventivo"])

PRIORIDAD_CATEGORIA = {
    "ARRANQUE / POTENCIA (BATERIA/CARGADOR)": "CRITICA",
    "FRENOS":                                 "CRITICA",
    "ENFRIAMIENTO / CALENTAMIENTO":           "ALTA",
    "DIRECCION / EJE":                        "ALTA",
    "ELECTRICO / CONTROLES":                  "MEDIA",
    "HIDRAULICO (FUGAS / MANGUERAS)":         "MEDIA",
}

def get_prioridad(categoria: str, riesgo: str) -> str:
    # Normalizar para comparar sin acentos
    cat = categoria.strip().upper()
    cat = cat.replace("É","E").replace("Á","A").replace("Í","I").replace("Ó","O").replace("Ú","U")
    base = PRIORIDAD_CATEGORIA.get(cat, "MEDIA")
    if riesgo == "ALTO" and base == "MEDIA":
        return "ALTA"
    return base

# ─── Schemas ──────────────────────────────────────────────────────────────────
class EquipoEnRiesgo(BaseModel):
    numero_serie:     str
    mtbf_dias:        float
    riesgo:           str
    fecha_prox_falla: Optional[str]
    dias_restantes:   int
    categoria:        str
    falla_probable:   str

class ParteSugerida(BaseModel):
    part_number:       str
    categoria:         str
    descripcion_falla: str
    usos_historicos:   int
    stock_minimo:      int
    cantidad_sugerida: int
    equipos_en_riesgo: list[EquipoEnRiesgo]
    prioridad:         str

class StockPreventivoResponse(BaseModel):
    fecha_calculo:     str
    horizonte_dias:    int
    equipos_en_riesgo: int
    partes_sugeridas:  int
    partes:            list[ParteSugerida]

# ─── GET /stock/preventivo ────────────────────────────────────────────────────
@stock_router.get("/preventivo", response_model=StockPreventivoResponse)
def stock_preventivo(
    horizonte_dias: int = Query(default=30, ge=7, le=90),
):
    S    = get_schema()
    hoy  = date.today()
    fecha_limite = (hoy + timedelta(days=horizonte_dias)).isoformat()

    # ── 1. Equipos en riesgo desde MEANFTB ────────────────────────────────────
    # Aliases limpios para evitar problemas con acentos/paréntesis en las claves del dict
    equipos_rows = execute_query(f"""
        SELECT
            "EQUIPO"                    AS EQUIPO,
            "MTBF (dias)"              AS MTBF_DIAS,
            "RIESGO__POR_MTBF_"        AS RIESGO,
            "Prox falla est. (fecha)"  AS FECHA_PROX_FALLA,
            "Categoria probable"       AS CATEGORIA,
            "FALLA_PROBABLE"           AS FALLA_PROBABLE
        FROM {S}.MEANFTB
        WHERE ("RIESGO__POR_MTBF_" LIKE 'ALTO%' OR "RIESGO__POR_MTBF_" LIKE 'MEDIO%')
          AND "Prox falla est. (fecha)" IS NOT NULL
          AND "Prox falla est. (fecha)" <= '{fecha_limite}'
        ORDER BY "Prox falla est. (fecha)" ASC
    """)

    if not equipos_rows:
        return StockPreventivoResponse(
            fecha_calculo=hoy.isoformat(),
            horizonte_dias=horizonte_dias,
            equipos_en_riesgo=0,
            partes_sugeridas=0,
            partes=[],
        )

    equipos_en_riesgo = []
    numeros_serie     = []

    for r in equipos_rows:
        fecha_prox     = r.get("FECHA_PROX_FALLA") or ""
        fecha_prox_str = fecha_prox[:10] if fecha_prox else None
        dias_restantes = (date.fromisoformat(fecha_prox_str) - hoy).days if fecha_prox_str else 0
        riesgo_raw     = r.get("RIESGO") or ""
        riesgo_norm    = "ALTO" if riesgo_raw.upper().startswith("ALTO") else "MEDIO"

        equipos_en_riesgo.append(EquipoEnRiesgo(
            numero_serie=r.get("EQUIPO") or "",
            mtbf_dias=float(r.get("MTBF_DIAS") or 0),
            riesgo=riesgo_norm,
            fecha_prox_falla=fecha_prox_str,
            dias_restantes=dias_restantes,
            categoria=r.get("CATEGORIA") or "",
            falla_probable=r.get("FALLA_PROBABLE") or "",
        ))
        numeros_serie.append((r.get("EQUIPO") or "").strip().upper())

    # ── 2. Historial de refacciones para esos equipos ─────────────────────────
    placeholders = ", ".join(["?" for _ in numeros_serie])
    correctivos_rows = execute_query(f"""
        SELECT NUMERO_SERIE, REFACCIONES
        FROM {S}.CORRECTIVOS
        WHERE NUMERO_SERIE IN ({placeholders})
          AND REFACCIONES IS NOT NULL
          AND TRIM(REFACCIONES) <> ''
          AND UPPER(TRIM(REFACCIONES)) <> 'NINGUNA'
    """, tuple(numeros_serie))

    # ── 3. Catálogo NUMSPARTE (alias para columna con acento) ─────────────────
    catalogo_rows = execute_query(f"""
        SELECT
            NO__PARTE,
            "Categoria asociada"   AS CATEGORIA,
            FALLAS_ASOCIADAS__TOP2_,
            STOCK_MIN_SUGERIDO
        FROM {S}.NUMSPARTE
    """)

    catalogo = {}
    for r in catalogo_rows:
        pn = (r.get("NO__PARTE") or "").strip().upper()
        catalogo[pn] = {
            "categoria":    r.get("CATEGORIA") or "",
            "fallas":       r.get("FALLAS_ASOCIADAS__TOP2_") or "",
            "stock_minimo": int(r.get("STOCK_MIN_SUGERIDO") or 0),
        }

    all_parts = set(catalogo.keys())

    # ── 4. Detectar part numbers en texto de REFACCIONES ─────────────────────
    uso_por_parte: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in correctivos_rows:
        ns    = (r.get("NUMERO_SERIE") or "").strip().upper()
        texto = (r.get("REFACCIONES") or "").upper()
        for pn in all_parts:
            if pn in texto:
                uso_por_parte[pn][ns] += 1

    # ── 5. Construir sugerencias ──────────────────────────────────────────────
    partes_sugeridas = []
    for pn, equipos_uso in uso_por_parte.items():
        equipos_parte = [e for e in equipos_en_riesgo if e.numero_serie in equipos_uso]
        if not equipos_parte:
            continue

        usos_total = sum(equipos_uso[e.numero_serie] for e in equipos_parte)
        datos      = catalogo[pn]
        stock_min  = datos["stock_minimo"]
        cantidad   = max(usos_total, stock_min)
        riesgo_max = "ALTO" if any(e.riesgo == "ALTO" for e in equipos_parte) else "MEDIO"

        partes_sugeridas.append(ParteSugerida(
            part_number=pn,
            categoria=datos["categoria"],
            descripcion_falla=datos["fallas"],
            usos_historicos=usos_total,
            stock_minimo=stock_min,
            cantidad_sugerida=cantidad,
            equipos_en_riesgo=equipos_parte,
            prioridad=get_prioridad(datos["categoria"], riesgo_max),
        ))

    orden = {"CRITICA": 0, "ALTA": 1, "MEDIA": 2}
    partes_sugeridas.sort(key=lambda x: (orden.get(x.prioridad, 9), -x.usos_historicos))

    return StockPreventivoResponse(
        fecha_calculo=hoy.isoformat(),
        horizonte_dias=horizonte_dias,
        equipos_en_riesgo=len(equipos_en_riesgo),
        partes_sugeridas=len(partes_sugeridas),
        partes=partes_sugeridas,
    )
