import io
import uuid
import jwt
import json
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, status, Form, File, UploadFile, BackgroundTasks, Request
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
    init_db()

    with Session(engine) as session:
        usuarios_a_crear = [
            "fabrigaoli@gmail.com",
            "tu-correo-personal@retail.com.py",
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudieron validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
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

    payload = {
        "company_id": 101,
        "user_id": usuario.id,
        "role": "admin",
        "sub": usuario.email,
        "exp": datetime.utcnow() + timedelta(hours=12)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

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
        "error": task.get("error") if task["status"] == "failed" else None
    }


@app.get("/", response_class=HTMLResponse)
async def servir_dashboard():
    nombre_archivo = "ConciliaAppXX.html"
    with open(nombre_archivo, "r", encoding="utf-8") as f:
        return HTMLResponse(
            content=f.read(),
            headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}
        )


@app.get("/v1/reconciliations/history")
async def obtener_historial(current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        statement = (
            select(ReconciliationHistory)
            .where(ReconciliationHistory.user_email == current_user.email)
            .order_by(ReconciliationHistory.fecha_ejecucion.desc())
        )
        resultados = session.exec(statement).all()
        out = []
        for r in resultados:
            try:
                data = json.loads(r.resumen_json)
            except Exception:
                data = {}
            out.append({"cloud_id": r.id, "fecha_ejecucion": r.fecha_ejecucion.isoformat(), **data})
        return out


@app.post("/v1/history/save")
async def guardar_historial(request: Request, current_user: User = Depends(get_current_user)):
    body = await request.json()
    with Session(engine) as session:
        entrada = ReconciliationHistory(
            user_email=current_user.email,
            resumen_json=json.dumps(body, ensure_ascii=False),
            empresa="SmartMatch"
        )
        session.add(entrada)
        session.commit()
        session.refresh(entrada)
        return {"cloud_id": entrada.id, "status": "saved"}


@app.delete("/v1/history/{entry_id}")
async def eliminar_historial(entry_id: int, current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        entrada = session.get(ReconciliationHistory, entry_id)
        if not entrada:
            raise HTTPException(status_code=404, detail="Entrada no encontrada")
        if entrada.user_email != current_user.email:
            raise HTTPException(status_code=403, detail="No autorizado")
        session.delete(entrada)
        session.commit()
        return {"status": "deleted"}
