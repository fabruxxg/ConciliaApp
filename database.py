import os
from sqlmodel import create_engine, SQLModel

# Esto busca la dirección en Railway automáticamente
# Si no la encuentra, fallará (lo cual está bien, porque así sabes que falta configurar)
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, echo=True)

def init_db():
    SQLModel.metadata.create_all(engine)