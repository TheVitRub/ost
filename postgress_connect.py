import os

import psycopg2
from dotenv import load_dotenv
from pandas import DataFrame

load_dotenv()


class PostConn:
    def __init__(self, db="test"):

        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.user = os.getenv("LOGIN")
        self.password = os.getenv("PASS")
        self.database = os.getenv("DATABASE_NAME")
        self.conn = psycopg2.connect(
            dbname=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def psycopg2_upsert(self, df: DataFrame, table_name="public.inventory_balance"):
        connection = self.conn
        df.rename(
            columns={

                "opening_balance_price": "initial_balance_price",

                # id_product и id_store_rename уже имеют правильные названия
                # order_date уже имеет правильное название
            },
            inplace=True,
        )
        key_columns = ["id_store", "id_product", "order_date", "inn"]

        # Убедимся, что DataFrame содержит все необходимые столбцы
        expected_columns = [
            "opening_balance",
            "initial_balance_price",
            "final_balance",
            "final_balance_price",
            "inn",
            "id_product",
            "id_store",
            "order_date",
        ]

        # Проверяем наличие всех столбцов
        for col in expected_columns:
            if col not in df.columns:
                raise ValueError(f"DataFrame должен содержать столбец '{col}'")

        # Формируем строки для запроса
        columns = ", ".join(df.columns)
        update_set = ", ".join(
            [f"{col} = excluded.{col}" for col in df.columns if col not in key_columns]
        )
        conflict_columns = ", ".join(key_columns)

        # Создаем SQL запрос
        query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES %s
        ON CONFLICT ({conflict_columns})
        DO UPDATE SET {update_set}
        """

        # Получаем данные из DataFrame как список кортежей
        values = [tuple(row) for row in df.to_numpy()]

        # Выполняем запрос
        with connection.cursor() as cursor:
            from psycopg2.extras import execute_values

            execute_values(cursor, query, values)
        connection.commit()

        return len(values)  # Возвращаем количество обработанных строк

    def dataframe_to_db(
        self,
        df: DataFrame,
        table_name: str,
        schema="public",
        if_exists="replace",
        method="insert",
    ):
        """
        Передает DataFrame в указанную таблицу базы данных

        Параметры:
        ----------
        df : DataFrame
            DataFrame для вставки в базу данных
        table_name : str
            Имя таблицы без схемы
        schema : str, default 'public'
            Схема базы данных
        if_exists : str, default 'replace'
            Что делать, если таблица существует:
            - 'fail': вызвать ошибку
            - 'replace': удалить и создать новую таблицу
            - 'append': добавить данные к существующей таблице
        method : str, default 'insert'
            Метод вставки данных:
            - 'insert': использовать INSERT запросы
            - 'copy': использовать PostgreSQL COPY (быстрее для больших объемов)

        Возвращает:
        ----------
        int
            Количество вставленных строк
        """
        # Преобразуем имена столбцов к нижнему регистру для PostgreSQL
        df.columns = [col.lower() for col in df.columns]

        full_table_name = f"{schema}.{table_name}"

        # Проверяем существование таблицы и создаем новую, если необходимо
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT to_regclass('{full_table_name}')")
            table_exists = cursor.fetchone()[0] is not None

            if table_exists:
                if if_exists == "fail":
                    raise ValueError(f"Таблица {full_table_name} уже существует")
                elif if_exists == "replace":
                    cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                    self.conn.commit()
                    table_exists = False

            # Создаем таблицу, если она не существует или была удалена
            if not table_exists:
                columns_with_types = []
                for col in df.columns:
                    dtype = df[col].dtype
                    if "int" in str(dtype):
                        col_type = "INTEGER"
                    elif "float" in str(dtype):
                        col_type = "NUMERIC"
                    elif "datetime" in str(dtype):
                        col_type = "TIMESTAMP"
                    else:
                        col_type = "TEXT"
                    columns_with_types.append(f"{col} {col_type}")

                create_table_sql = (
                    f"CREATE TABLE {full_table_name} ({', '.join(columns_with_types)})"
                )
                cursor.execute(create_table_sql)
                self.conn.commit()

        # Вставляем данные выбранным методом
        if method == "copy":
            # Используем COPY метод
            from io import StringIO
            import csv

            # Создаем буфер для данных
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
            buffer.seek(0)

            # Выполняем COPY
            with self.conn.cursor() as cursor:
                cursor.copy_expert(
                    f"COPY {full_table_name} ({', '.join(df.columns)}) FROM STDIN WITH CSV",
                    buffer,
                )
                self.conn.commit()
        else:
            # Используем INSERT метод
            columns = ", ".join(df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = (
                f"INSERT INTO {full_table_name} ({columns}) VALUES ({placeholders})"
            )

            records = [tuple(row) for row in df.to_numpy()]

            with self.conn.cursor() as cursor:
                from psycopg2.extras import execute_batch

                execute_batch(cursor, insert_query, records)
                self.conn.commit()

        return len(df)

    def fetch_to_dataframe(self, query: str, params=None) -> DataFrame:
        """
        Выполняет SQL-запрос и возвращает результаты в виде pandas DataFrame.

        Параметры:
        ----------
        query : str
            SQL-запрос для выполнения.
        params : tuple или dict, optional
            Параметры для подстановки в SQL-запрос (если есть).

        Возвращает:
        ----------
        DataFrame
            Результаты запроса в виде pandas DataFrame.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            columns = [
                desc[0] for desc in cursor.description
            ]  # Получаем имена столбцов
            data = cursor.fetchall()

        return DataFrame(data, columns=columns)
    def close(self):
        if self.conn:
            self.conn.close()