from contextlib import contextmanager

from sqlalchemy.orm import Session

from app.database.client_connection import obtener_session_cliente


@contextmanager
def cliente_session(ruc: str):

    db: Session = obtener_session_cliente(ruc)

    try:
        yield db
        db.commit()

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()