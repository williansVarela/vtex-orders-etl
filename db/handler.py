import logging
from abc import ABC, abstractmethod

import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker


class DatabaseStrategy(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def fetch(self, query: str, *args, **kwargs):
        """
        Execute a SQL query and return a list of rows."""
        pass

    @abstractmethod
    def execute(self, query: str, *args, **kwargs) -> None:
        """
        Execute a SQL query that does not return results.
        """
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class PyODBCStrategy(DatabaseStrategy):
    def __init__(self, server: str, port: int, database: str, user: str, password: str):
        self.connection_string: str = self._set_conn_string(
            server, port, database, user, password
        )
        self.session: pyodbc.Cursor = self.connect()

    def _set_conn_string(
        self, server: str, port: int, database: str, user: str, password: str
    ) -> str:
        return f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server},{port};Database={database};UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no"

    def connect(self) -> pyodbc.Cursor:
        self.connection = pyodbc.connect(self.connection_string)
        return self.connection.cursor()

    def fetch(self, query: str, *args, **kwargs):
        result = self.session.execute(query, *args, **kwargs)
        return result.fetchall()

    def execute(self, query: str, *args, **kwargs) -> None:
        self.session.execute(query, *args, **kwargs)

    def __enter__(self):
        self.session = self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            if exc_type:
                logging.error(f"Database error: {exc_val}")
                self.session.rollback()
            else:
                self.session.commit()
            self.session.close()


class SQLAlchemyStrategy(DatabaseStrategy):
    def __init__(self, server: str, port: int, database: str, user: str, password: str):
        self.connection_string = self._set_conn_string(
            server, port, database, user, password
        )
        self.session: Session = self.connect()

    def _set_conn_string(
        self, server: str, port: int, database: str, user: str, password: str
    ) -> str:
        return f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server"

    def connect(self) -> Session:
        self.engine = create_engine(self.connection_string)
        session_maker = sessionmaker()
        session = session_maker(bind=self.engine)
        return session

    def fetch(self, query: str, *args, **kwargs):
        result = self.session.execute(text(query), *args, **kwargs)
        return result.fetchall()

    def execute(self, query: str, *args, **kwargs) -> None:
        self.session.execute(text(query), *args, **kwargs)

    def __enter__(self):
        self.session = self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            if exc_type:
                logging.error(f"Database error: {exc_val}")
                self.session.rollback()
            else:
                self.session.commit()
            self.session.close()


class DatabaseHandler:
    def __init__(self, strategy: DatabaseStrategy):
        self.strategy = strategy

    def set_strategy(self, strategy: DatabaseStrategy):
        self.strategy = strategy

    def fetch(self, query: str, *args, **kwargs):
        """
        Execute a SQL query and return a list of rows.
        """
        with self.strategy as session:
            result = session.fetch(query, *args, **kwargs)
            return result

    def execute(self, query: str, *args, **kwargs):
        """
        Execute a SQL query that does not return results.
        """
        with self.strategy as session:
            session.execute(query, *args, **kwargs)
