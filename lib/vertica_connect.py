from airflow.hooks.base import BaseHook
import vertica_python


class VerticaConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str,  password: str, sslmode: str = "require") -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.password = password
        self.sslmode = sslmode

    def url(self) -> str:
        return {'host': str(self.host), 
                'port': str(self.port),
                'user': str(self.user),       
                'password': str(self.password),
                'database': str(self.db_name)
        }

class ConnectionVerticaBuilder:

    @staticmethod
    def vertica_conn(conn_id: str):
        conn_vertica = BaseHook.get_connection("VERTICA_TRANSACTIONS_CONNECTION")
        sslmode = "require"
        if "sslmode" in conn_vertica.extra_dejson:
            sslmode = conn_vertica.extra_dejson["sslmode"]
        conn_info =  VerticaConnect(str(conn_vertica.host),
                       str(conn_vertica.port),
                       str(conn_vertica.schema),
                       str(conn_vertica.login),
                       str(conn_vertica.password),
                       sslmode)
        vertica_conn = vertica_python.connect(**conn_info.url())
        return vertica_conn
    
    