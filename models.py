from sqlmodel import SQLModel, Field
from passlib.context import CryptContext
from sqlmodel import SQLModel, Field
from typing import Optional
import datetime

class ReconciliationHistory(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_email: str
    fecha_ejecucion: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    resumen_json: str  # Aquí guardaremos los resultados (discrepancias, totales, etc.)
    empresa: str
# Esto configura el algoritmo para "encriptar" (hashing)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    password_hash: str
    
    # Método para guardar la contraseña de forma segura
    def set_password(self, password: str):
        self.password_hash = pwd_context.hash(password)

    # Método para verificar si la contraseña que escribe el usuario es la correcta
    def verify_password(self, password: str):
        return pwd_context.verify(password, self.password_hash)


class Candidate(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    phone: str = Field(index=True)
    name: str
    cv_text: Optional[str] = None
    score_cc: Optional[int] = None
    score_ventas: Optional[int] = None
    score_rrss: Optional[int] = None
    score_presion: Optional[int] = None
    score_perfil: Optional[int] = None
    total: Optional[int] = None
    canal: Optional[str] = None
    prioridad: Optional[str] = None
    observaciones: Optional[str] = None
    linea: Optional[int] = None
    sesion: Optional[str] = None
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)