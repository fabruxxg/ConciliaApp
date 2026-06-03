import io
import uuid
import jwt
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, status, Form, File, UploadFile, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from database import init_db, engine, get_session
from models import User, ReconciliationHistory
from sqlmodel import Session, select
import os

app = FastAPI()

@app.on_event("startup")
def on_startup():
    init_db() # Crea las tablas en PostgreSQL si no existen
    
    with Session(engine) as session:
        # ═════════════════════════════════════════════════════════════════════
        # CONFIGURA AQUÍ LOS USUARIOS QUE DESEAS EN TU SISTEMA
        # ═════════════════════════════════════════════════════════════════════
        # Puedes cambiar o añadir aquí todos los correos que quieras.
        # Todos se crearán con la contraseña por defecto: Fg200472
        # ═════════════════════════════════════════════════════════════════════
        usuarios_a_crear = [
            "fabrigaoli@gmail.com.py",
            "tu-correo-personal@retail.com.py",   # <-- ¡Cambia este por el tuyo!
            "auditor@retail.com.py",
            "gerencia@retail.com.py"
        ]
        
        for email_usuario in usuarios_a_crear:
            statement = select(User).where(User.email == email_usuario)
            usuario_existente = session.exec(statement).first()
            
            if not usuario_existente:
                nuevo_usuario = User(email=email_usuario)
                nuevo_usuario.set_password("Fg200472") 
                session.add(nuevo_usuario)
                print(f"¡Usuario {email_usuario} creado con éxito!")
        
        session.commit()

# ═════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE CORS: PERMITE QUE TU HTML SE CONECTE CON PYTHON
# ═════════════════════════════════════════════════════════════════════
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

# Configuración de Seguridad (JWT)
SECRET_KEY = "CONCILIA_APP_SUPER_SECRET_KEY_2026"
ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="v1/auth/login")

TASKS_DB: Dict[str, Dict[str, Any]] = {}

# ═════════════════════════════════════════════════════════════════════
# 1. DEPENDENCIA DE SEGURIDAD (MULTI-TENANCY)
# ═════════════════════════════════════════════════════════════════════
async def get_current_tenant(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
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
def core_reconciliation_worker(task_id: str, mayor_bytes: bytes, gateway_bytes: bytes, company_id: int):
    try:
        TASKS_DB[task_id]["status"] = "processing"
        TASKS_DB[task_id]["progress"] = 20

        df_mayor = pd.read_excel(io.BytesIO(mayor_bytes))
        df_gateway = pd.read_excel(io.BytesIO(gateway_bytes))
        
        TASKS_DB[task_id]["progress"] = 50

        df_mayor.columns = [c.lower().strip() for c in df_mayor.columns]
        df_gateway.columns = [c.lower().strip() for c in df_gateway.columns]

        join_key = 'referencia' if 'referencia' in df_mayor.columns else 'comprobante'
        
        df_cruce = pd.merge(df_mayor, df_gateway, on=join_key, how='outer', suffixes=('_mayor', '_gateway'))

        TASKS_DB[task_id]["progress"] = 80

        df_cruce['monto_mayor'] = df_cruce['monto_mayor'].fillna(0)
        df_cruce['monto_gateway'] = df_cruce['monto_gateway'].fillna(0)
        df_cruce['desvio'] = df_cruce['monto_mayor'] - df_cruce['monto_gateway']

        def categorizar(row):
            if row['monto_mayor'] == 0: return 'FALTANTE_EN_MAYOR'
            if row['monto_gateway'] == 0: return 'FALTANTE_EN_PASARELA'
            if row['desvio'] != 0: return 'DESVIO_MONTO'
            return 'OK'

        df_cruce['match_status'] = df_cruce.apply(categorizar, axis=1)

        total_mayor = float(df_cruce['monto_mayor'].sum())
        total_gateway = float(df_cruce['monto_gateway'].sum())
        total_desviado = float(df_cruce[df_cruce['match_status'] != 'OK']['desvio'].abs().sum())
        
        df_discrepancias = df_cruce[df_cruce['match_status'] != 'OK']
        lista_discrepancias = df_discrepancias[[join_key, 'monto_mayor', 'monto_gateway', 'desvio', 'match_status']].to_dict(orient='records')

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
        TASKS_DB[task_id].update({
            "status": "failed",
            "progress": 100,
            "error": f"Error crítico de procesamiento: {str(e)}"
        })


# ═════════════════════════════════════════════════════════════════════
# 3. ENDPOINT DE AUTENTICACIÓN (LOGIN)
# ═════════════════════════════════════════════════════════════════════
class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/v1/auth/login")
def login(formulario_data: dict = None, session: Session = Depends(get_session)):
    if formulario_data is None:
        return {"error": "El servidor no recibió ningún dato"}
    
    if not formulario_data:
        raise HTTPException(
            status_code=401, 
            detail="ERRORfrontend: El servidor recibió un formulario totalmente VACÍO."
        )
    
    email_recibido = formulario_data.get("email") or formulario_data.get("username")
    password_recibida = formulario_data.get("password")
    
    if not email_recibido:
        llaves_enviadas = list(formulario_data.keys())
        raise HTTPException(
            status_code=401, 
            detail=f"ERRORfrontend: No enviaste ni 'email' ni 'username'. Enviaste estos campos: {llaves_enviadas}"
        )
        
    statement = select(User).where(User.email == email_recibido)
    usuario = session.exec(statement).first()
    
    if not usuario:
        raise HTTPException(
            status_code=401, 
            detail=f"ERROR_BASE_DATOS: El correo '{email_recibido}' NO existe registrado en PostgreSQL."
        )
        
    if not usuario.verify_password(password_recibida):
        raise HTTPException(
            status_code=401, 
            detail="ERROR_PASSWORD: El usuario existe, pero la CONTRASEÑA es incorrecta."
        )

    # 🌟 GENERACIÓN DEL TOKEN JWT REQUERIDO PARA SEGUIR OPERANDO LA APP
    payload = {
        "company_id": 101, 
        "user_id": usuario.id, 
        "role": "admin",
        "exp": datetime.utcnow() + timedelta(hours=12)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    
    # 🌟 EXTRAEMOS EL NOMBRE ANTES DEL '@' PARA EL SALUDO DINÁMICO
    nombre_personalizado = usuario.email.split("@")[0].capitalize()
        
    return {
        "status": "success", 
        "message": "¡Bienvenido!",
        "access_token": token,
        "usuario_nombre": nombre_personalizado,
        "empresa": "Retail S.A."
    }


# ═════════════════════════════════════════════════════════════════════
# 4. ENDPOINTS CORE DE CONCILIACIÓN
# ═════════════════════════════════════════════════════════════════════
@app.post("/v1/reconciliations/process", status_code=status.HTTP_202_ACCEPTED, tags=["Conciliador"])
async def process_reconciliation(
    background_tasks: BackgroundTasks,
    file_mayor: UploadFile = File(...),
    file_gateway: UploadFile = File(...),
    processor: str = Form(...),
    tenant: dict = Depends(get_current_tenant)
):
    task_id = f"task_{uuid.uuid4().hex[:8]}"
    
    TASKS_DB[task_id] = {
        "company_id": tenant["company_id"],
        "status": "pending",
        "progress": 0,
        "processor": processor,
        "created_at": datetime.now().isoformat()
    }
async def procesar_conciliacion(
    background_tasks: BackgroundTasks,
    file_mayor: UploadFile = File(...),
    # ... resto de tus parámetros ...
):
    # 1. ESTO ES LO QUE ESTABA FUERA. Ahora está aquí adentro y funciona:
    mayor_bytes = await file_mayor.read() 
    df_mayor = pd.read_excel(io.BytesIO(mayor_bytes))
    
    # 2. Aquí va el resto de tu lógica para consolidar extractos...
    # ...
    
    # 3. Y aquí va la lógica de crear el historial en la BD
    nuevo_historial = ReconciliationHistory(
        user_email=current_user.email,
        resumen_json=json.dumps(resultados),
        empresa="Retail"
    )
    # ... guardar en base de datos ...
    
    mayor_bytes = await file_mayor.read()
    gateway_bytes = await file_gateway.read()
    
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
    task = TASKS_DB.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="La tarea solicitada no existe.")
        
    if task["company_id"] != tenant["company_id"]:
        raise HTTPException(status_code=403, detail="No autorizado para ver este recurso.")
        
    return {
        "task_id": task_id,
        "status": task["status"],
        "progress_percentage": task["progress"],
        "results": task.get("results") if task["status"] == "completed" else None,
        "error": task.get("error") if task["failed"] == "failed" else None
    }
    session.add(nuevo_historial)
    session.commit()
 }
# --- Autenticación y Endpoints de Historial ---

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudieron validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Asegúrate de que esta clave coincida con la que usas en tu lógica de login
        payload = jwt.decode(token, "TU_SECRET_KEY", algorithms=["HS256"])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception
    
    with Session(engine) as session:
        user = session.exec(select(User).where(User.email == email)).first()
        if user is None:
            raise credentials_exception
        return user

@app.get("/v1/reconciliations/history")
async def obtener_historial(current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        statement = select(ReconciliationHistory).where(
            ReconciliationHistory.user_email == current_user.email
        )
        resultados = session.exec(statement).all()
        return resultados