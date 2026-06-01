import os
from sqlmodel import create_engine, SQLModel, Session # <- Revisa tener 'Session' aquí
from models import User # <- Dejamos esto aquí para que cree la tabla de usuarios

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, echo=True)

def init_db():
    SQLModel.metadata.create_all(engine)

# !!! ASEGÚRATE DE QUE ESTA FUNCIÓN ESTÉ ESCRITA EXACTAMENTE ASÍ AL FINAL !!!
def get_session():
    with Session(engine) as session:
        yield session