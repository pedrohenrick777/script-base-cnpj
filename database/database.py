import os

from psycopg2 import connect


class Database:

    def __init__(self, user, passwd, dbname, **kwargs):
        self.__user = user
        self.__passwd = passwd
        self.__dbname = dbname
        self.__host = kwargs.get('host')
        self.__port = kwargs.get('port')

        if self.__host is None:
            self.__host = 'localhost'

        if self.__port is None:
            self.__port = 5432

        self.__dbapi = self.generate_dbapi()

    def copy_csv_from_stdin(self, table_name, colunas_inserir, streamer_arquivo):
        with self.__dbapi.cursor() as c:
            c.copy_expert(
                f"COPY {table_name} {colunas_inserir} FROM STDIN "
                + f"WITH CSV DELIMITER ';' ENCODING 'LATIN1' FORCE NULL {colunas_inserir[1:-1]}",
                streamer_arquivo
            )

            self.__dbapi.commit()

    def generate_dbapi(self):
        '''
        Gera uma conexão com o banco
        '''
        return connect(
            host=self.__host,
            port=self.__port,
            database=self.__dbname,
            user=self.__user,
            password=self.__passwd
        )
