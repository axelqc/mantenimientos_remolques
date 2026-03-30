"""
router: stock_preventivo.py
Stock preventivo cruzando MEANFTB + EQUIPOS + CORRECTIVOS + NUMSPARTE.
MEANFTB.EQUIPO = NUMERO_ECONOMICO → JOIN EQUIPOS → NUMERO_SERIE → CORRECTIVOS
"""

from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta
from collections import defaultdict
from db import execute_query, get_schema

stock_router = APIRouter(prefix="/stock", tags=["Stock Preventivo"])

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
    if riesgo == "ALTO" and base == "MEDIA":
        return "ALTA"
    return base

class EquipoEnRiesgo(BaseModel):
    numero_economico: str
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

@stock_router.get("/preventivo", response_model=StockPreventivoResponse)
def stock_preventivo(horizonte_dias: int = Query(default=30, ge=7, le=90)):
    S    = get_schema()
    hoy  = date.today()
    fecha_limite = (hoy + timedelta(days=horizonte_dias)).isoformat()

    # 1. Equipos en riesgo: JOIN MEANFTB → EQUIPOS para obtener NUMERO_SERIE
    equipos_rows = execute_query(f"""
        SELECT
            m.EQUIPO            AS NUMERO_ECONOMICO,
            e.NUMERO_SERIE      AS NUMERO_SERIE,
            m.MTBF_DIAS,
            m.RIESGO,
            m.FECHA_PROX_FALLA,
            m.CATEGORIA_PROBABLE,
            m.FALLA_PROBABLE
        FROM {S}.MEANFTB m
        JOIN {S}.EQUIPOS e ON UPPER(TRIM(e.NUMERO_ECONOMICO)) = UPPER(TRIM(m.EQUIPO))
        WHERE (m.RIESGO LIKE 'ALTO%' OR m.RIESGO LIKE 'MEDIO%')
          AND m.FECHA_PROX_FALLA IS NOT NULL
          AND m.FECHA_PROX_FALLA <= '{fecha_limite}'
        ORDER BY m.FECHA_PROX_FALLA ASC
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
        ns             = (r.get("NUMERO_SERIE") or "").strip().upper()

        equipos_en_riesgo.append(EquipoEnRiesgo(
            numero_economico=r.get("NUMERO_ECONOMICO") or "",
            numero_serie=ns,
            mtbf_dias=float(r.get("MTBF_DIAS") or 0),
            riesgo=riesgo_norm,
            fecha_prox_falla=fecha_prox_str,
            dias_restantes=dias_restantes,
            categoria=r.get("CATEGORIA_PROBABLE") or "",
            falla_probable=r.get("FALLA_PROBABLE") or "",
        ))
        if ns:
            numeros_serie.append(ns)

    if not numeros_serie:
        return StockPreventivoResponse(
            fecha_calculo=hoy.isoformat(),
            horizonte_dias=horizonte_dias,
            equipos_en_riesgo=len(equipos_en_riesgo),
            partes_sugeridas=0,
            partes=[],
        )

    # 2. Refacciones históricas usando NUMERO_SERIE real
    placeholders = ", ".join(["?" for _ in numeros_serie])
    correctivos_rows = execute_query(f"""
        SELECT NUMERO_SERIE, REFACCIONES
        FROM {S}.CORRECTIVOS
        WHERE UPPER(TRIM(NUMERO_SERIE)) IN ({placeholders})
          AND REFACCIONES IS NOT NULL
          AND TRIM(REFACCIONES) <> ''
          AND UPPER(TRIM(REFACCIONES)) <> 'NINGUNA'
    """, tuple(numeros_serie))

    # 3. Catálogo NUMSPARTE
    catalogo_rows = execute_query(f"""
        SELECT NO__PARTE, CATEGORIA_ASOCIADA,
               FALLAS_ASOCIADAS__TOP2_, STOCK_MIN_SUGERIDO
        FROM {S}.NUMSPARTE
    """)

    catalogo = {}
    for r in catalogo_rows:
        pn = (r.get("NO__PARTE") or "").strip().upper()
        catalogo[pn] = {
            "categoria":    r.get("CATEGORIA_ASOCIADA") or "",
            "fallas":       r.get("FALLAS_ASOCIADAS__TOP2_") or "",
            "stock_minimo": int(r.get("STOCK_MIN_SUGERIDO") or 0),
        }

    all_parts = set(catalogo.keys())

    # 4. Detectar part numbers en REFACCIONES
    uso_por_parte: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in correctivos_rows:
        ns    = (r.get("NUMERO_SERIE") or "").strip().upper()
        texto = (r.get("REFACCIONES") or "").upper()
        for pn in all_parts:
            if pn in texto:
                uso_por_parte[pn][ns] += 1

    # 5. Construir sugerencias
    partes_sugeridas = []
    for pn, equipos_uso in uso_por_parte.items():
        equipos_parte = [e for e in equipos_en_riesgo if e.numero_serie in equipos_uso]
        if not equipos_parte:
            continue

        usos_total = sum(equipos_uso[e.numero_serie] for e in equipos_parte)
        datos      = catalogo[pn]
        stock_min  = datos["stock_minimo"]
        riesgo_max = "ALTO" if any(e.riesgo == "ALTO" for e in equipos_parte) else "MEDIO"

        partes_sugeridas.append(ParteSugerida(
            part_number=pn,
            categoria=datos["categoria"],
            descripcion_falla=datos["fallas"],
            usos_historicos=usos_total,
            stock_minimo=stock_min,
            cantidad_sugerida=max(usos_total, stock_min),
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
