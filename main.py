import io
import uuid
import jwt
from datetime import datetime
from typing import Dict, Any
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, status, Form, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from fastapi import FastAPI, Depends, HTTPException, status, Form, File, UploadFile, BackgroundTasks
from database import init_db, engine, get_session
from models import User
from fastapi import FastAPI, Depends, HTTPException, status
from sqlmodel import Session, select
import os
import pandas as pd

app = FastAPI()
@app.on_event("startup")
def on_startup():
    init_db() # Crea las tablas si no existen
    
    # Creamos el usuario administrador inicial si no existe
    with Session(engine) as session:
        statement = select(User).where(User.email == "fabrigaoli@gmail.com.py")
        usuario_existente = session.exec(statement).first()
        
        if not usuario_existente:
            admin = User(email="fabrigaoli@gmail.com.py")
            admin.set_password("Fg200472") # Se guarda encriptada
            session.add(admin)
            session.commit()
            print("¡Usuario administrador inicial creado en PostgreSQL!")
# ═════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE CORS: PERMITE QUE TU HTML SE CONECTE CON PYTHON
# ═════════════════════════════════════════════════════════════════════
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite que cualquier archivo HTML local se conecte
    allow_credentials=True,
    allow_methods=["*"],  # Permite enviar POST, GET, etc.
    allow_headers=["*"],  # Permite enviar archivos y tokens de seguridad
)

# Configuración de Seguridad (JWT)
SECRET_KEY = "CONCILIA_APP_SUPER_SECRET_KEY_2026"
ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="v1/auth/login")

# Base de datos en memoria para simular el estado de las tareas asíncronas
# En producción, esto se almacena en PostgreSQL y Redis
TASKS_DB: Dict[str, Dict[str, Any]] = {}

# ═════════════════════════════════════════════════════════════════════
# 1. DEPENDENCIA DE SEGURIDAD (MULTI-TENANCY)
# ═════════════════════════════════════════════════════════════════════
async def get_current_tenant(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """
    Intercepta el JWT Token, lo valida y extrae los datos de la empresa (Tenant).
    Si el token es inválido o expiró, corta la petición inmediatamente.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciales de acceso inválidas o expiradas.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        company_id: int = payload.get("company_id")
        user_id: int = payload.get("user_id")
        role: str = payload.get("role")
        
        if company_id is None or user_id is None:
            raise credentials_exception
            
        return {"company_id": company_id, "user_id": user_id, "role": role}
    except jwt.PyJWTError:
        raise credentials_exception


# ═════════════════════════════════════════════════════════════════════
# 2. MOTOR DE CONCILIACIÓN EN SEGUNDO PLANO (PANDAS)
# ═════════════════════════════════════════════════════════════════════
def core_reconciliation_worker(
    task_id: str, 
    mayor_bytes: bytes, 
    gateway_bytes: bytes, 
    company_id: int
):
    """
    Trabajador asíncrono. Ejecuta el cruce de datos matemático a alta velocidad
    usando dataframes de Pandas sin bloquear el tráfico de la API.
    """
    try:
        TASKS_DB[task_id]["status"] = "processing"
        TASKS_DB[task_id]["progress"] = 20

        # 1. Leer los archivos binarios directamente a memoria (Soporta Excel y CSV)
        df_mayor = pd.read_excel(io.BytesIO(mayor_bytes))
        df_gateway = pd.read_excel(io.BytesIO(gateway_bytes))
        
        TASKS_DB[task_id]["progress"] = 50

        # Estandarizar nombres de columnas a minúsculas para evitar errores humanos
        df_mayor.columns = [c.lower().strip() for c in df_mayor.columns]
        df_gateway.columns = [c.lower().strip() for c in df_gateway.columns]

        # 2. EL CRUCE MAESTRO (OUTER JOIN por Nro de Comprobante / Referencia)
        # Asumimos que los archivos tienen una columna 'referencia' o 'comprobante'
        join_key = 'referencia' if 'referencia' in df_mayor.columns else 'comprobante'
        
        df_cruce = pd.merge(
            df_mayor, 
            df_gateway, 
            on=join_key, 
            how='outer', 
            suffixes=('_mayor', '_gateway')
        )

        TASKS_DB[task_id]["progress"] = 80

        # 3. Cálculo de métricas y detección de desvíos en Guaraníes
        df_cruce['monto_mayor'] = df_cruce['monto_mayor'].fillna(0)
        df_cruce['monto_gateway'] = df_cruce['monto_gateway'].fillna(0)
        df_cruce['desvio'] = df_cruce['monto_mayor'] - df_cruce['monto_gateway']

        # Clasificación automática del estado de la transacción
        def categorizar(row):
            if row['monto_mayor'] == 0: return 'FALTANTE_EN_MAYOR'
            if row['monto_gateway'] == 0: return 'FALTANTE_EN_PASARELA'
            if row['desvio'] != 0: return 'DESVIO_MONTO'
            return 'OK'

        df_cruce['match_status'] = df_cruce.apply(categorizar, axis=1)

        # 4. Consolidación de Macro-Métricas para el Dashboard
        total_mayor = float(df_cruce['monto_mayor'].sum())
        total_gateway = float(df_cruce['monto_gateway'].sum())
        total_desviado = float(df_cruce[df_cruce['match_status'] != 'OK']['desvio'].abs().sum())
        
        # Filtrar solo las filas que representan problemas (Desvíos o faltantes)
        df_discrepancias = df_cruce[df_cruce['match_status'] != 'OK']
        lista_discrepancias = df_discrepancias[[join_key, 'monto_mayor', 'monto_gateway', 'desvio', 'match_status']].to_dict(orient='records')

        # Guardar el resultado final de la auditoría en nuestro estado
        TASKS_DB[task_id].update({
            "status": "completed",
            "progress": 100,
            "results": {
                "metrics": {
                    "total_mayor": total_mayor,
                    "total_gateway": total_gateway,
                    "total_deviated": total_desviado,
                    "match_rate": round((1 - (len(df_discrepancias) / len(df_cruce))) * 100, 2) if len(df_cruce) > 0 else 100
                },
                "discrepancies": lista_discrepancias
            }
        })

    except Exception as e:
        # Si un archivo viene corrupto o mal mapeado, se captura el error de forma segura
        TASKS_DB[task_id].update({
            "status": "failed",
            "progress": 100,
            "error": f"Error crítico de procesamiento: {str(e)}"
        })


# ═════════════════════════════════════════════════════════════════════
# 3. ENDPOINTS DE LA API REST
# ═════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════
# ESTRUCTURA Y BASE DE DATOS PARA INICIO DE SESIÓN
# ═════════════════════════════════════════════════════════════════════
class LoginRequest(BaseModel):
    username: str
    password: str

# Base de datos simulada en memoria con empresas de Paraguay
EMPRESAS_DB = {
    "auditor@retail.com.py": {"password": "Retail2026!", "tenant_id": 101, "nombre": "Retail S.A."},
    "contabilidad@sudameris.com.py": {"password": "SudaSecure77", "tenant_id": 102, "nombre": "Banco Sudameris"},
    "admin@bancard.com.py": {"password": "BancardB2B#", "tenant_id": 103, "nombre": "Procesadora Bancard"}
}

@app.post("/v1/auth/login")
@app.post("/v1/auth/login")
def login(formulario_data: dict = None, session: Session = Depends(get_session)):
    
    # 1. Si los datos llegan vacíos desde JavaScript
    if not formulario_data:
        raise HTTPException(
            status_code=401, 
            detail="ERRORfrontend: El servidor recibió un formulario totalmente VACÍO."
        )
    
    email_recibido = formulario_data.get("email") or formulario_data.get("username")
    password_recibida = formulario_data.get("password")
    
    # 2. Si JavaScript envió datos, pero con nombres incorrectos
    if not email_recibido:
        llaves_enviadas = list(formulario_data.keys())
        raise HTTPException(
            status_code=401, 
            detail=f"ERRORfrontend: No enviaste ni 'email' ni 'username'. Enviaste estos campos: {llaves_enviadas}"
        )
        
    # 3. Buscar en la Base de Datos
    statement = select(User).where(User.email == email_recibido)
    usuario = session.exec(statement).first()
    
    # 4. Si el correo no existe en Railway
    if not usuario:
        raise HTTPException(
            status_code=401, 
            detail=f"ERROR_BASE_DATOS: El correo '{email_recibido}' NO existe registrado en PostgreSQL."
        )
        
    # 5. Si el correo existe pero la contraseña está mal
    if not usuario.verify_password(password_recibida):
        raise HTTPException(
            status_code=401, 
            detail="ERROR_PASSWORD: El usuario existe, pero la CONTRASEÑA es incorrecta."
        )
        
    return {"status": "success", "message": "¡Bienvenido!"}@app.post("/v1/reconciliations/process", status_code=status.HTTP_202_ACCEPTED, tags=["Conciliador"])
async def process_reconciliation(
    background_tasks: BackgroundTasks,
    file_mayor: UploadFile = File(...),
    file_gateway: UploadFile = File(...),
    processor: str = Form(...),
    tenant: dict = Depends(get_current_tenant)
):
    """
    Endpoint Core: Recibe los archivos financieros, inyecta de forma transparente
    el company_id del cliente y dispara el motor matemático asíncronamente.
    """
    task_id = f"task_{uuid.uuid4().hex[:8]}"
    
    # Inicializamos la estructura de la tarea
    TASKS_DB[task_id] = {
        "company_id": tenant["company_id"],
        "status": "pending",
        "progress": 0,
        "processor": processor,
        "created_at": datetime.now().isoformat()
    }
    
    # Leemos los archivos en memoria antes de pasarlos al hilo secundario
    mayor_bytes = await file_mayor.read()
    gateway_bytes = await file_gateway.read()
    
    # Se añade la tarea pesada al pool de hilos de FastAPI de forma no bloqueante
    background_tasks.add_task(
        core_reconciliation_worker, 
        task_id, 
        mayor_bytes, 
        gateway_bytes, 
        tenant["company_id"]
    )
    
    return {
        "task_id": task_id,
        "status": "pending",
        "message": f"Archivos para {processor.upper()} recibidos. Procesamiento en cola."
    }


@app.get("/v1/reconciliations/tasks/{task_id}", tags=["Conciliador"])
async def get_task_status(task_id: str, tenant: dict = Depends(get_current_tenant)):
    """
    Endpoint de monitoreo (Polling). Permite al frontend consultar el estado
    de la conciliación y pintar la barra de carga en tiempo real.
    """
    task = TASKS_DB.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="La tarea solicitada no existe.")
        
    # CONTROL CRÍTICO MULTI-TENANT: La empresa A no puede consultar tareas de la empresa B
    if task["company_id"] != tenant["company_id"]:
        raise HTTPException(status_code=403, detail="No autorizado para ver este recurso.")
        
    return {
        "task_id": task_id,
        "status": task["status"],
        "progress_percentage": task["progress"],
        "results": task.get("results") if task["status"] == "completed" else None,
        "error": task.get("error") if task["status"] == "failed" else None
    }
from fastapi.responses import HTMLResponse  # <-- Asegúrate de que esta línea esté arriba con tus otros imports
import os

# ... (Todo tu código existente de FastAPI, CORS, Procesamiento, etc.) ...

# ═════════════════════════════════════════════════════════════════════
# RUTA PARA SERVIR EL DASHBOARD COMO UNA WEB REAL
# ═════════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def servir_dashboard():
    # Cambia "ConciliaAppXX.html" por el nombre exacto de tu archivo si es diferente
    nombre_archivo = "ConciliaAppXX.html" 
    
    ruta_completa = os.path.join(os.path.dirname(__file__), nombre_archivo)
    
    try:
        with open(ruta_completa, "r", encoding="utf-8") as archivo:
            return archivo.read()
    except FileNotFoundError:
        return HTMLResponse(
            content=f"<h2>Error: No se encontró el archivo '{nombre_archivo}' en la carpeta de Python.</h2>", 
            status_code=404
        )