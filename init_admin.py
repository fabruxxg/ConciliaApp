from sqlmodel import Session, select
from database import engine
from models import User

def crear_admin():
    with Session(engine) as session:
        # 1. Verificamos si el usuario ya existe para no duplicarlo
        statement = select(User).where(User.email == "auditor@retail.com.py")
        results = session.exec(statement)
        user = results.first()
        
        if not user:
            # 2. Si no existe, lo creamos
            nuevo_admin = User(email="auditor@retail.com.py")
            nuevo_admin.set_password("Retail2026!") # ¡Aquí usamos el método seguro!
            
            # 3. Guardamos en la base de datos
            session.add(nuevo_admin)
            session.commit()
            print("¡Usuario administrador creado exitosamente!")
        else:
            print("El usuario ya existe, no es necesario crearlo.")

if __name__ == "__main__":
    crear_admin()