"""
router: refacciones_prevision.py
Montar en main.py:  app.include_router(refacciones_router)

Tabla DB2: NUMSPARTE
Columnas:
  NO__PARTE              → part number
  USOS_2025              → consumo histórico anual
  "Categoría asociada"   → categoría (requiere comillas, tiene acento)
  EQUIPOS_FRECUENTES__TOP3_
  FALLAS_ASOCIADAS__TOP2_
  STOCK_MIN_SUGERIDO
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional
import ibm_db_dbi as db2
from datetime import date, timedelta
import math, os

refacciones_router = APIRouter(prefix="/refacciones", tags=["Previsión de Refacciones"])

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

# ─── Prioridad por categoría ──────────────────────────────────────────────────
PRIORIDAD_MAP = {
    "ARRANQUE / POTENCIA (BATERÍA / CARGADOR)": 0,
    "FRENOS":                                   0,
    "ENFRIAMIENTO / CALENTAMIENTO":             1,
    "DIRECCIÓN / EJE":                          1,
    "ELÉCTRICO / CONTROLES":                    2,
    "HIDRÁULICO (FUGAS / MANGUERAS / CILINDROS)":2,
    "TRANSMISIÓN / LUBRICACIÓN":                2,
    "CÓDIGO / DIAGNÓSTICO":                     3,
    "ESCAPE / EMISIONES":                       3,
    "LLANTAS / RODADURA":                       3,
}
NIVELES = ["CRITICA", "ALTA", "MEDIA", "BAJA"]

def get_prioridad(categoria: str, alerta: bool) -> str:
    idx = PRIORIDAD_MAP.get(categoria.strip(), 2)
    if alerta and idx > 0:
        idx -= 1
    return NIVELES[idx]

# ─── Schemas ──────────────────────────────────────────────────────────────────
class PrevisionRequest(BaseModel):
    horizonte_dias: int = Field(default=30, ge=7, le=180,
        description="Días futuros a proyectar. Default: 30.")
    categoria: Optional[str] = Field(default=None,
        description="Filtrar por categoría. Ej: 'FRENOS'")
    solo_alertas: bool = Field(default=False,
        description="Si True, retorna solo refacciones que requieren compra.")

class RefaccionPrevision(BaseModel):
    part_number:              str
    categoria:                str
    fallas_asociadas:         str
    equipos_frecuentes:       str
    usos_2025:                int     # Consumo histórico real del año
    tasa_mensual:             float   # usos_2025 / 12
    proyeccion_horizonte:     float   # tasa_mensual * (horizonte_dias / 30)
    stock_minimo:             int
    cantidad_sugerida_compra: int
    prioridad:                str
    alerta:                   bool

class PrevisionResponse(BaseModel):
    fecha_analisis:    date
    horizonte_dias:    int
    total_refacciones: int
    total_alertas:     int
    refacciones:       list[RefaccionPrevision]

# ─── Endpoint principal ───────────────────────────────────────────────────────
@refacciones_router.post("/prevision", response_model=PrevisionResponse)
def calcular_prevision(req: PrevisionRequest, conn=Depends(get_db2_conn)):
    """
    Genera previsión de compra de refacciones basada en:
    - Consumo histórico real (USOS_2025)
    - Stock mínimo sugerido (STOCK_MIN_SUGERIDO)
    - Proyección al horizonte solicitado
    """
    cursor = conn.cursor()

    # Nota: "Categoría asociada" va entre comillas dobles por espacios y acento
    sql = f"""
        SELECT
            NO__PARTE,
            "Categoría asociada",
            FALLAS_ASOCIADAS__TOP2_,
            EQUIPOS_FRECUENTES__TOP3_,
            USOS_2025,
            STOCK_MIN_SUGERIDO
        FROM {SCHEMA}.NUMSPARTE
    """
    params = []
    if req.categoria:
        sql += ' WHERE UPPER("Categoría asociada") LIKE ?'
        params.append(f"%{req.categoria.upper()}%")

    cursor.execute(sql, params if params else [])
    rows = cursor.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No se encontraron refacciones.")

    meses_horizonte = req.horizonte_dias / 30
    resultado: list[RefaccionPrevision] = []

    for part_number, categoria, fallas, equipos, usos_2025, stock_minimo in rows:
        usos_2025   = usos_2025 or 0
        stock_minimo = stock_minimo or 0

        tasa_mensual = round(usos_2025 / 12, 2)
        proyeccion   = round(tasa_mensual * meses_horizonte, 2)
        cantidad_raw = math.ceil(proyeccion)

        # Alerta: la proyección en el horizonte iguala o supera el stock mínimo
        alerta = cantidad_raw > 0 and cantidad_raw >= stock_minimo

        # Sugerir al menos el stock mínimo si hay consumo esperado
        cantidad_sugerida = max(cantidad_raw, stock_minimo) if cantidad_raw > 0 else 0

        resultado.append(RefaccionPrevision(
            part_number=part_number,
            categoria=categoria or "",
            fallas_asociadas=fallas or "",
            equipos_frecuentes=equipos or "",
            usos_2025=usos_2025,
            tasa_mensual=tasa_mensual,
            proyeccion_horizonte=proyeccion,
            stock_minimo=stock_minimo,
            cantidad_sugerida_compra=cantidad_sugerida,
            prioridad=get_prioridad(categoria or "", alerta),
            alerta=alerta,
        ))

    # Ordenar: alertas primero → prioridad → mayor consumo
    orden = {"CRITICA": 0, "ALTA": 1, "MEDIA": 2, "BAJA": 3}
    resultado.sort(key=lambda x: (
        not x.alerta,
        orden.get(x.prioridad, 9),
        -x.usos_2025,
    ))

    if req.solo_alertas:
        resultado = [r for r in resultado if r.alerta]

    return PrevisionResponse(
        fecha_analisis=date.today(),
        horizonte_dias=req.horizonte_dias,
        total_refacciones=len(resultado),
        total_alertas=sum(1 for r in resultado if r.alerta),
        refacciones=resultado,
    )

# ─── Shortcut: alertas críticas ───────────────────────────────────────────────
@refacciones_router.get("/alertas-criticas", response_model=PrevisionResponse)
def alertas_criticas(horizonte_dias: int = 30, conn=Depends(get_db2_conn)):
    """Shortcut: solo refacciones que requieren compra en el horizonte dado."""
    return calcular_prevision(
        PrevisionRequest(horizonte_dias=horizonte_dias, solo_alertas=True),
        conn,
    )
