"""
MSB León — Helpers compartidos de conexión DB2
Importar desde main.py y plan_predictivo.py
"""

import os
import ibm_db
from decimal import Decimal
from datetime import date, datetime
from fastapi import HTTPException
from dotenv import load_dotenv
load_dotenv()  # Carga variables de entorno desde .env

DB2_CONFIG = {
    "host":     os.getenv("DB2_HOST", "localhost"),
    "port":     os.getenv("DB2_PORT", "50000"),
    "database": os.getenv("DB2_DATABASE", "MSBMTTO"),
    "user":     os.getenv("DB2_USER", "db2inst1"),
    "password": os.getenv("DB2_PASSWORD", ""),
    "schema":   os.getenv("DB2_SCHEMA", "MSB"),
}

SCHEMA = DB2_CONFIG["schema"]

def get_schema():
    return os.getenv("DB2_SCHEMA", "MSB")

def get_db2_connection():
    """Crea conexión a DB2 usando ibm_db."""
    conn_str = (
        f"DATABASE={DB2_CONFIG['database']};"
        f"HOSTNAME={DB2_CONFIG['host']};"
        f"PORT={DB2_CONFIG['port']};"
        f"PROTOCOL=TCPIP;"
        f"UID={DB2_CONFIG['user']};"
        f"PWD={DB2_CONFIG['password']};"
        f"SECURITY=SSL;" 
    )
    try:
        return ibm_db.connect(conn_str, "", "")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión DB2: {str(e)}")


def execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """Ejecuta query y retorna lista de diccionarios."""
    conn = get_db2_connection()
    try:
        if params:
            stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt, params)
        else:
            stmt = ibm_db.exec_immediate(conn, sql)

        results = []
        row = ibm_db.fetch_assoc(stmt)
        while row:
            clean_row = {}
            for k, v in row.items():
                if isinstance(v, Decimal):
                    clean_row[k] = float(v)
                elif isinstance(v, (date, datetime)):
                    clean_row[k] = v.isoformat()
                else:
                    clean_row[k] = v
            results.append(clean_row)
            row = ibm_db.fetch_assoc(stmt)
        return results
    finally:
        ibm_db.close(conn)
