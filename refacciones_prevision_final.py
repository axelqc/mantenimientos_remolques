"""
router: refacciones_prevision_final.py
Previsión de compra de refacciones desde NUMSPARTE.
Usa execute_query / get_schema de db.py
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from datetime import date
from db import execute_query, get_schema
import math

refacciones_router = APIRouter(prefix="/refacciones", tags=["Previsión de Refacciones"])

PRIORIDAD_MAP = {
    "ARRANQUE / POTENCIA (BATERÍA / CARGADOR)": 0,
    "FRENOS":                                   0,
    "ENFRIAMIENTO / CALENTAMIENTO":             1,
    "DIRECCIÓN / EJE":                          1,
    "ELÉCTRICO / CONTROLES":                    2,
    "HIDRÁULICO (FUGAS / MANGUERAS / CILINDROS)":2,
}
NIVELES = ["CRITICA", "ALTA", "MEDIA", "BAJA"]

def get_prioridad(categoria: str, alerta: bool) -> str:
    idx = PRIORIDAD_MAP.get(categoria.strip(), 2)
    if alerta and idx > 0:
        idx -= 1
    return NIVELES[idx]

class RefaccionPrevision(BaseModel):
    part_number:              str
    categoria:                str
    fallas_asociadas:         str
    equipos_frecuentes:       str
    usos_2025:                int
    tasa_mensual:             float
    proyeccion_horizonte:     float
    stock_minimo:             int
    cantidad_sugerida_compra: int
    prioridad:                str
    alerta:                   bool

class PrevisionResponse(BaseModel):
    fecha_analisis:    str
    horizonte_dias:    int
    total_refacciones: int
    total_alertas:     int
    refacciones:       list[RefaccionPrevision]


def _calcular_prevision_logica(
    horizonte_dias: int,
    categoria: Optional[str],
    solo_alertas: bool,
) -> PrevisionResponse:
    S = get_schema()

    sql = f"""
        SELECT NO__PARTE, CATEGORIA_ASOCIADA,
               FALLAS_ASOCIADAS__TOP2_, EQUIPOS_FRECUENTES__TOP3_,
               USOS_2025, STOCK_MIN_SUGERIDO
        FROM {S}.NUMSPARTE
    """
    params = []
    if categoria:
        sql += " WHERE UPPER(CATEGORIA_ASOCIADA) LIKE ?"
        params.append(f"%{categoria.upper()}%")

    rows = execute_query(sql, params)

    if not rows:
        raise HTTPException(status_code=404, detail="No se encontraron refacciones.")

    meses_horizonte = horizonte_dias / 30
    resultado = []

    for r in rows:
        usos_2025    = int(r.get("USOS_2025") or 0)
        stock_minimo = int(r.get("STOCK_MIN_SUGERIDO") or 0)
        categoria_r  = r.get("CATEGORIA_ASOCIADA") or ""
        tasa_mensual = round(usos_2025 / 12, 2)
        proyeccion   = round(tasa_mensual * meses_horizonte, 2)
        cantidad_raw = math.ceil(proyeccion)
        alerta       = cantidad_raw > 0 and cantidad_raw >= stock_minimo
        cantidad     = max(cantidad_raw, stock_minimo) if cantidad_raw > 0 else 0

        resultado.append(RefaccionPrevision(
            part_number=r.get("NO__PARTE") or "",
            categoria=categoria_r,
            fallas_asociadas=r.get("FALLAS_ASOCIADAS__TOP2_") or "",
            equipos_frecuentes=r.get("EQUIPOS_FRECUENTES__TOP3_") or "",
            usos_2025=usos_2025,
            tasa_mensual=tasa_mensual,
            proyeccion_horizonte=proyeccion,
            stock_minimo=stock_minimo,
            cantidad_sugerida_compra=cantidad,
            prioridad=get_prioridad(categoria_r, alerta),
            alerta=alerta,
        ))

    orden = {"CRITICA": 0, "ALTA": 1, "MEDIA": 2, "BAJA": 3}
    resultado.sort(key=lambda x: (not x.alerta, orden.get(x.prioridad, 9), -x.usos_2025))

    if solo_alertas:
        resultado = [r for r in resultado if r.alerta]

    return PrevisionResponse(
        fecha_analisis=date.today().isoformat(),
        horizonte_dias=horizonte_dias,
        total_refacciones=len(resultado),
        total_alertas=sum(1 for r in resultado if r.alerta),
        refacciones=resultado,
    )


@refacciones_router.get("/prevision", response_model=PrevisionResponse)
def calcular_prevision(
    horizonte_dias: int = Query(default=30, ge=7, le=180),
    categoria: Optional[str] = Query(default=None),
    solo_alertas: bool = Query(default=False),
):
    return _calcular_prevision_logica(horizonte_dias, categoria, solo_alertas)


@refacciones_router.get("/alertas-criticas", response_model=PrevisionResponse)
def alertas_criticas(horizonte_dias: int = Query(default=30)):
    return _calcular_prevision_logica(horizonte_dias, categoria=None, solo_alertas=True)
