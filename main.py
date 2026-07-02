import io
import uuid
import jwt
import json
import time
import base64 as _b64
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, status, Form, File, UploadFile, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from database import init_db, engine, get_session
from models import User, ReconciliationHistory, Candidate
from sqlmodel import Session, select
import os
from reconciliation import concil_infornet, concil_netel, concil_pronet, concil_compras
from jwt import PyJWKClient


@dataclass
class ClerkUser:
    id: int = 0
    email: str = ""


def _clerk_jwks_url(pk: str) -> str:
    try:
        b64 = pk.split('_')[2]
        b64 += '=' * (-len(b64) % 4)
        domain = _b64.b64decode(b64).decode('utf-8').rstrip('$\x00').strip()
        return f"https://{domain}/.well-known/jwks.json"
    except Exception:
        return ""

app = FastAPI()

# ── Secrets desde env vars (setear en Railway Variables) ──────────────
SECRET_KEY           = os.getenv("JWT_SECRET",          "CONCILIA_APP_SUPER_SECRET_KEY_2026")
BOT_SECRET           = os.getenv("BOT_SECRET",          "shift2026recruit")
ADMIN_PASS           = os.getenv("ADMIN_PASSWORD",       "")
CLERK_PUBLISHABLE_KEY = os.getenv("CLERK_PUBLISHABLE_KEY", "")
CLERK_JWKS_URL       = os.getenv("CLERK_JWKS_URL",       _clerk_jwks_url(CLERK_PUBLISHABLE_KEY))
ALLOWED_ORIGINS = [o.strip() for o in os.getenv(
    "ALLOWED_ORIGINS",
    "https://conciliaapp-production.up.railway.app"
).split(",")]

_jwks_client: Optional[PyJWKClient] = None

def _get_clerk_jwks() -> Optional[PyJWKClient]:
    global _jwks_client
    if _jwks_client is None and CLERK_JWKS_URL:
        _jwks_client = PyJWKClient(CLERK_JWKS_URL, cache_keys=True)
    return _jwks_client

def _verify_clerk_token(token: str) -> dict:
    client = _get_clerk_jwks()
    if client is None:
        raise ValueError("Clerk JWKS not configured")
    signing_key = client.get_signing_key_from_jwt(token)
    return jwt.decode(token, signing_key.key, algorithms=["RS256"], options={"verify_aud": False})

ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="v1/auth/login")

# ── Rate limiting login (in-memory, max 10 intentos / 15 min por IP) ──
_login_attempts: dict = defaultdict(list)

def _check_login_rate(ip: str):
    now = time.time()
    window = 900  # 15 min
    _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < window]
    if len(_login_attempts[ip]) >= 10:
        raise HTTPException(status_code=429, detail="Demasiados intentos fallidos. Esperá 15 minutos.")
    _login_attempts[ip].append(now)

@app.on_event("startup")
def on_startup():
    init_db()

    if not ADMIN_PASS:
        print("ADVERTENCIA: ADMIN_PASSWORD no configurado en Railway — no se crean usuarios automáticos.")
        return

    with Session(engine) as session:
        usuarios_a_crear = [
            "fabrigaoli@gmail.com",
        ]
        for email_usuario in usuarios_a_crear:
            statement = select(User).where(User.email == email_usuario)
            usuario_existente = session.exec(statement).first()
            if not usuario_existente:
                nuevo_usuario = User(email=email_usuario)
                nuevo_usuario.set_password(ADMIN_PASS)
                session.add(nuevo_usuario)
                print(f"Usuario {email_usuario} creado.")
        session.commit()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Bot-Secret"],
)

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
    # Try Clerk JWT first (RS256 signed by Clerk)
    if CLERK_JWKS_URL:
        try:
            payload = _verify_clerk_token(token)
            clerk_user_id: str = payload.get("sub", "")
            if clerk_user_id:
                return ClerkUser(id=0, email=clerk_user_id)
        except Exception:
            pass  # fall through to legacy JWT

    # Legacy JWT (HS256, for backward compat during transition)
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=401, detail="Token inválido")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Token inválido",
                            headers={"WWW-Authenticate": "Bearer"})

    with Session(engine) as session:
        user = session.exec(select(User).where(User.email == email)).first()
        if user is None:
            raise HTTPException(status_code=401, detail="Usuario no encontrado")
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
def login(request: Request, formulario_data: dict = None, session: Session = Depends(get_session)):
    client_ip = request.client.host if request.client else "unknown"
    _check_login_rate(client_ip)

    if formulario_data is None:
        return {"error": "El servidor no recibió ningún dato"}

    if not formulario_data:
        raise HTTPException(status_code=401, detail="Formulario vacío.")

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
    with open("ConciliaAppXX.html", "r", encoding="utf-8") as f:
        content = f.read()
    if CLERK_PUBLISHABLE_KEY:
        print(f"[CLERK] Inyectando key: {CLERK_PUBLISHABLE_KEY[:20]}...")
        content = content.replace("__CLERK_KEY__", CLERK_PUBLISHABLE_KEY)
    else:
        print("[CLERK] CLERK_PUBLISHABLE_KEY no seteada")
        content = content.replace('data-clerk-publishable-key="__CLERK_KEY__"', 'data-clerk-publishable-key=""')
    return HTMLResponse(
        content=content,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}
    )


@app.get("/v1/reconciliations/history")
async def obtener_historial(current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        statement = (
            select(ReconciliationHistory)
            .where(ReconciliationHistory.user_email == current_user.email)
            .where(ReconciliationHistory.empresa != "Proveedores")
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


# ═════════════════════════════════════════════════════════════════════
# CONCILIACIÓN DE PROVEEDORES — historial + registro de auditoría
# Reusa ReconciliationHistory con empresa="Proveedores"
# ═════════════════════════════════════════════════════════════════════

@app.get("/v1/proveedores/history")
async def obtener_historial_proveedores(current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        statement = (
            select(ReconciliationHistory)
            .where(ReconciliationHistory.user_email == current_user.email)
            .where(ReconciliationHistory.empresa == "Proveedores")
            .order_by(ReconciliationHistory.fecha_ejecucion.desc())
        )
        resultados = session.exec(statement).all()
        out = []
        for r in resultados:
            try:
                data = json.loads(r.resumen_json)
            except Exception:
                data = {}
            out.append({
                "cloud_id": r.id,
                "fecha_ejecucion": r.fecha_ejecucion.isoformat(),
                "ejecutado_por": r.user_email,
                **data,
            })
        return out


@app.post("/v1/proveedores/history/save")
async def guardar_historial_proveedores(request: Request, current_user: User = Depends(get_current_user)):
    body = await request.json()
    with Session(engine) as session:
        entrada = ReconciliationHistory(
            user_email=current_user.email,
            resumen_json=json.dumps(body, ensure_ascii=False),
            empresa="Proveedores",
        )
        session.add(entrada)
        session.commit()
        session.refresh(entrada)
        return {"cloud_id": entrada.id, "status": "saved", "ejecutado_por": current_user.email}


# ═════════════════════════════════════════════════════════════════════
# 5. CONCILIACIÓN SERVER-SIDE (motores Python protegidos)
# ═════════════════════════════════════════════════════════════════════

@app.post("/v1/reconcile")
async def reconcile(
    canal: str = Form(...),
    portal_file: UploadFile = File(...),
    sys_file: UploadFile = File(...),
    fecha_corte: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
):
    """
    Ejecuta el motor de conciliación server-side para el canal indicado.
    canal: "infonet" | "netel" | "pronet" | "compras"
    """
    portal_bytes = await portal_file.read()
    sys_bytes = await sys_file.read()

    try:
        if canal == "infonet":
            rows = concil_infornet(portal_bytes, sys_bytes)
        elif canal == "netel":
            rows = concil_netel(portal_bytes, sys_bytes)
        elif canal == "pronet":
            rows = concil_pronet(portal_bytes, sys_bytes)
        elif canal == "compras":
            rows = concil_compras(portal_bytes, sys_bytes, fecha_corte)
        else:
            raise HTTPException(status_code=400, detail=f"Canal desconocido: {canal}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en conciliación {canal}: {str(e)}")

    return rows


# ═════════════════════════════════════════════════════════════════════
# 6. RECLUTAMIENTO
# ═════════════════════════════════════════════════════════════════════

def _check_bot(request: Request):
    secret = request.headers.get("X-Bot-Secret", "")
    if secret != BOT_SECRET:
        raise HTTPException(status_code=403, detail="Bot secret inválido")

@app.post("/v1/recruitment/candidates")
async def upsert_candidates(request: Request):
    _check_bot(request)
    body = await request.json()
    candidates = body if isinstance(body, list) else [body]
    saved = 0
    with Session(engine) as session:
        for c in candidates:
            existing = session.exec(
                select(Candidate).where(
                    Candidate.phone == str(c.get("phone","")).strip(),
                    Candidate.linea == c.get("linea", 1)
                )
            ).first()
            if existing:
                for k in ["name","cv_text","score_cc","score_ventas","score_rrss","score_presion","score_perfil","total","canal","prioridad","observaciones","sesion"]:
                    v = c.get(k)
                    if v is not None:
                        setattr(existing, k, v)
                session.add(existing)
            else:
                entry = Candidate(
                    phone=str(c.get("phone","")).strip(),
                    name=c.get("name",""),
                    cv_text=c.get("cv_text"),
                    score_cc=c.get("score_cc"),
                    score_ventas=c.get("score_ventas"),
                    score_rrss=c.get("score_rrss"),
                    score_presion=c.get("score_presion"),
                    score_perfil=c.get("score_perfil"),
                    total=c.get("total"),
                    canal=c.get("canal"),
                    prioridad=c.get("prioridad"),
                    observaciones=c.get("observaciones"),
                    linea=c.get("linea", 1),
                    sesion=c.get("sesion"),
                )
                session.add(entry)
                saved += 1
        session.commit()
    return {"status": "ok", "saved": saved, "total": len(candidates)}

@app.post("/v1/recruitment/candidates/upload")
async def upload_candidates_bulk(request: Request, current_user: User = Depends(get_current_user)):
    body = await request.json()
    candidates = body if isinstance(body, list) else [body]
    saved = 0
    updated = 0
    with Session(engine) as session:
        for c in candidates:
            existing = session.exec(
                select(Candidate).where(
                    Candidate.phone == str(c.get("phone","")).strip(),
                    Candidate.linea == c.get("linea", 1)
                )
            ).first()
            if existing:
                for k in ["name","cv_text","score_cc","score_ventas","score_rrss","score_presion","score_perfil","total","canal","prioridad","observaciones","sesion"]:
                    v = c.get(k)
                    if v is not None:
                        setattr(existing, k, v)
                session.add(existing)
                updated += 1
            else:
                entry = Candidate(
                    phone=str(c.get("phone","")).strip(),
                    name=c.get("name",""),
                    cv_text=c.get("cv_text"),
                    score_cc=c.get("score_cc"),
                    score_ventas=c.get("score_ventas"),
                    score_rrss=c.get("score_rrss"),
                    score_presion=c.get("score_presion"),
                    score_perfil=c.get("score_perfil"),
                    total=c.get("total"),
                    canal=c.get("canal"),
                    prioridad=c.get("prioridad"),
                    observaciones=c.get("observaciones"),
                    linea=c.get("linea", 1),
                    sesion=c.get("sesion"),
                )
                session.add(entry)
                saved += 1
        session.commit()
    return {"status": "ok", "nuevos": saved, "actualizados": updated, "total": len(candidates)}

@app.get("/v1/recruitment/candidates")
async def get_candidates(current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        rows = session.exec(select(Candidate).order_by(Candidate.total.desc())).all()
        return [
            {
                "id": r.id, "phone": r.phone, "name": r.name,
                "score_cc": r.score_cc, "score_ventas": r.score_ventas,
                "score_rrss": r.score_rrss, "score_presion": r.score_presion,
                "score_perfil": r.score_perfil, "total": r.total,
                "canal": r.canal, "prioridad": r.prioridad,
                "observaciones": r.observaciones, "cv_text": r.cv_text,
                "linea": r.linea, "sesion": r.sesion,
                "created_at": r.created_at.isoformat() if r.created_at else None
            }
            for r in rows
        ]

@app.delete("/v1/recruitment/candidates/{candidate_id}")
async def delete_candidate(candidate_id: int, current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        c = session.get(Candidate, candidate_id)
        if not c:
            raise HTTPException(status_code=404, detail="No encontrado")
        session.delete(c)
        session.commit()
        return {"status": "deleted"}

@app.delete("/v1/recruitment/candidates")
async def delete_all_candidates(current_user: User = Depends(get_current_user)):
    with Session(engine) as session:
        rows = session.exec(select(Candidate)).all()
        for r in rows:
            session.delete(r)
        session.commit()
        return {"status": "ok", "deleted": len(rows)}
