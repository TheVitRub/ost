import os
import traceback
import numpy as np
import pandas as pd
from clickhouse_driver import Client, errors
from dotenv import load_dotenv

load_dotenv()

if os.getenv("SERVER") == "PROD":
    typeBD = "analytics_db"
else:
    typeBD = os.getenv("CLiCK_TEST")


class ClickHouseConnection:
    _click = None

    def __new__(cls, *args, **kwargs):
        if not cls._click:
            print(f"Инициализация класса {cls.__name__}")
            cls._click = super().__new__(cls)
        return cls._click

    def __init__(self):
        if not hasattr(self, "initialized"):
            self.host = os.getenv("CLICK_HOST")
            self.port = os.getenv("CLICK_PORT")
            self.user = os.getenv("CLIC_USER")
            self.password = os.getenv("CLICK_PASSWORD")
            self.database = typeBD
            self.pool = []

            self.engine = None

            self.initialized = True

    def create_connection(self):
        client = Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        return client

    def get_connection(self):
        if not self.pool:
            self.pool.append(self.create_connection())
        return self.pool.pop(0)

    def return_connection(self, connection):
        self.pool.append(connection)

    def execute(self, query, params=None, with_transaction=False):
        connection = self.get_connection()
        try:
            if with_transaction:
                connection.execute("START TRANSACTION;")
            result = connection.execute(query, params, with_column_types=True)
            if with_transaction:
                connection.execute("COMMIT;")
            self.return_connection(connection)
            return result
        except errors.Error as e:
            if with_transaction:
                connection.execute("ROLLBACK;")
            self.return_connection(connection)
            raise e

    def execute_df(self, query, params=None):
        connection = self.get_connection()
        try:
            result = connection.execute(query, params, with_column_types=True)
            self.return_connection(connection)
            if isinstance(result, tuple):
                result, columns = result
                column_names = [column[0] for column in columns]
                df = pd.DataFrame(result, columns=column_names)
            else:
                df = pd.DataFrame()
            return df
        except Exception as e:
            self.return_connection(connection)
            raise e

    def command_clickhouse(self, query):
        try:
            self.execute(query)
        except Exception as error:
            print(traceback.format_exc())

    def clickhouse_to_dataframe(self, query):
        try:
            result = self.execute(query)
            if isinstance(result, tuple):
                result, columns = result
                column_names = [column[0] for column in columns]
                df = pd.DataFrame(result, columns=column_names)
            else:
                df = pd.DataFrame()
            return df
        except Exception as error:
            print("clickhouse_to_dataframe", traceback.format_exc())
            # _LOG_(type_log='connect', data=f'error clickhouse_to_dataframe {error}')
            return pd.DataFrame()

    def clickhouse_to_scalar(self, query):
        try:
            result = self.execute(query)
            if isinstance(result, tuple):
                result, columns = result
                column_names = [column[0] for column in columns]
                df = pd.DataFrame(result, columns=column_names)
                scalar_value = df.iloc[0, 0]
            else:
                scalar_value = result[0]
            return scalar_value
        except Exception as error:
            print(traceback.format_exc())
            # _LOG_(type_log='connect', data=f'error clickhouse_to_scalar {error}')

    # Не использовать!
    def clickhouse_insert(self, table_name, df, with_transaction=False):
        try:
            for column in df.columns:
                if column not in ["guid_bonus", "guid_discount"]:
                    df[column] = df[column].replace({0: np.nan})
                    df[column] = df[column].replace(
                        {
                            "None": None,
                            "nan": None,
                            "<NA>": None,
                            "NaT": None,
                            "NaN": None,
                            np.nan: None,
                        }
                    )

            data_to_insert = df.to_dict(orient="records")
            self.execute(f"INSERT INTO {table_name} VALUES", data_to_insert)
        except Exception as e:
            print(f"Ошибка загрузки в ClickHouse {table_name}: {str(e)}")
            print(traceback.format_exc())
            # _LOG_(type_log='connect', data=f'error clickhouse_insert {e}')

    def clickhouse_delete_date(self, table_name, date_start, date_end):
        try:
            delete_query = f"ALTER TABLE {table_name} DELETE WHERE toDate(order_date) >= '{date_start}' AND toDate(order_date) <= '{date_end}'"
            self.execute(delete_query)
        except Exception as error:
            print(traceback.format_exc())
        # _LOG_(type_log='connect', data=f'error clickhouse_delete_date {error}')

    # Использовать
    def clickhouse_del_date_on_insert(
        self, table_name, date_start, date_end, delete_columns, df
    ):
        try:
            delete_query = f"ALTER TABLE {table_name} DELETE WHERE toDate(order_date) >= '{date_start}' AND toDate(order_date) <= '{date_end}'"
            self.execute(delete_query, with_transaction=False)
            self.clickhouse_insert(table_name, df, with_transaction=True)
        except Exception as error:
            print(traceback.format_exc())
        # _LOG_(type_log='connect', data=f'error clickhouse_del_date_on_insert {error}')

    def close(self):
        for connection in self.pool:
            connection.disconnect()
