import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
pd.set_option("expand_frame_repr", False)
pd.set_option("display.max_colwidth", None)
from dotenv import load_dotenv
import os

from click_house_connect import ClickHouseConnection
from postgress_connect import PostConn


class WorkForData:
    def __init__(self):
        # Подбираем из окружения данные
        load_dotenv()
        # Записываем в переменные
        self.click_house = ClickHouseConnection()
        # Получаем данные о магазинах
        self.post_conn_analyt = PostConn(db="an")
        print("Получаем данные о магазинах! К-к-арамба!")
        self.store = self.__take_data_for_DB(
            ["search_store", "id_store_rename"], "spr_store_rename", db="not_test"
        )
        self.store_channel = self.__take_data_for_DB(
            ["id_store", "channel"], "spr_store", db="not_test"
        )
        print("Получили данные с магазинов! К-к-арамба!")
        # Получаем данные о продуктах
        print("Получаем данные о продуктах! К-к-арамба!")
        self.product = self.__take_data_for_DB(
            ["id_product_code", "id_product"], "spr_product", db="not_test"
        )
        print("Получили данные с инн! К-к-арамба!")
        self.inn = self.__take_data_for_DB(
            ["search_entity", "inn"], "spr_legal_entity_rename", db="test"
        )
        print("Получили данные с инн! К-к-арамба!")
        # Путь до папки с нашими файлами
        self.path_to_directory = "files"
        # Путь до старого формата
        self.path_to_directory_old = "old_files"

    def first_start(self):
        print("Start!")
        self.__take_all_file()
        self.__take_all_old_file()

    def __take_all_file(self):
        print("Поиск новых файлов...")

        except_file_path = "except.csv"
        try:
            except_df = pd.read_csv(except_file_path)
            excluded_files = except_df.iloc[:, 0].tolist()
        except FileNotFoundError:
            excluded_files = []

        txt_files = [
            f for f in os.listdir(self.path_to_directory)
            if f.endswith(".txt") and f not in excluded_files
        ]

        if not txt_files:
            print("Нет новых файлов для обработки.")
            return

        print(f"Найдено новых файлов: {len(txt_files)}")
        #for text in txt_files:
        #    result = self.__process_new_file(text)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(self.__process_new_file, txt_files))


    def __take_all_old_file(self):
        print("Поиск старых файлов...")

        except_file_path = "except.csv"
        try:
            except_df = pd.read_csv(except_file_path)
            excluded_files = except_df.iloc[:, 0].tolist()
        except FileNotFoundError:
            excluded_files = []

        txt_files = [
            f for f in os.listdir(self.path_to_directory_old)
            if f.endswith(".txt") and f not in excluded_files
        ]

        if not txt_files:
            print("Нет старых файлов для обработки.")
            return

        print(f"Найдено старых файлов: {len(txt_files)}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(self.__process_old_file, txt_files))


    def __data_to_DB(self, df):
        """Это типо подключение к бд"""
        df.rename(
            columns={
                "Начальный остаток": "opening_balance",
                "Начальный остаток себестоимость": "opening_balance_price",
                "Конечный остаток": "final_balance",
                "Конечный остаток себестоимость": "final_balance_price",
                "id_store_rename": "id_store",
                "search_entity": "inn",
                # id_product и id_store_rename уже имеют правильные названия
                # order_date уже имеет правильное название
            },
            inplace=True,
        )
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
        date_start = date_end = df['order_date'].iloc[0]
        df['order_date'] = pd.to_datetime(df['order_date'])

        df['opening_balance'] = df['opening_balance'].astype('float64')
        df['opening_balance_price'] = df['opening_balance_price'].astype('float64')
        df['final_balance'] = df['final_balance'].astype('float64')
        df['final_balance_price'] = df['final_balance_price'].astype('float64')




        table_name = 'remnants_of_products'


        key_columns = ["id_product", "id_store", "inn", "order_date"]

        # Группируем по ключевым столбцам и суммируем остальные
        df = df.groupby(key_columns, as_index=False).sum()
        df = df[df["id_product"] != 0]
        columns = ['opening_balance', 'opening_balance_price', 'final_balance_price', 'final_balance']

        df['inn'] = df['inn'].astype('Int64')
        df['id_store'] = df['id_store'].astype('Int64')
        df['id_product'] = df['id_product'].astype('UInt64')

        for i in columns:
            df[i] = df[i].astype('float').round(3)


        self.click_house.clickhouse_del_date_on_insert(table_name=table_name,
                                                       date_start=date_start,
                                                       date_end=date_end,
                                                       delete_columns='',
                                                       df=df)
        #self.post_conn_analyt.psycopg2_upsert(df)
    def __take_data_for_file(self, file_path):
        """Получаем данные с файла"""
        # Столбцы, которые будут в датасете
        column_names = [
            "Организация",
            "Магазин",
            "Номер магазина",
            "Номенклатура.Код",
            "Начальный остаток",
            "Начальный остаток себестоимость",
            "Конечный остаток",
            "Конечный остаток себестоимость",
        ]

        # Считываем данные из файла, пропуская первые 8 строк
        df = pd.read_csv(
            file_path, sep="\t", skiprows=8, names=column_names, encoding="utf-8"
        )
        # Убираем последнюю, где Итог
        df = df.iloc[:-1]
        # Преобразование столбцов в численные выражения. Первые две это Организация и Магазин. Их пропускаем, как и Номенклатуру
        for column in column_names[2:]:
            if column != "Номенклатура.Код":
                df[column] = self.__column_to_float(df, column)
        return df

    def __take_data_for_file_old(self, file_path):
        """Получаем данные с файла"""
        # Столбцы, которые будут в датасете
        column_names = [
            "Организация",
            "Магазин",
            "Номер магазина",
            "Номенклатура",
            "Код",
            "Магазин.Номер магазина",
            "По дням",
            "Начальный остаток",
            "Начальный остаток себестоимость",
            "Конечный остаток",
            "Конечный остаток себестоимость",
        ]

        # Считываем данные из файла, пропуская первые 8 строк
        df = pd.read_csv(
            file_path,
            sep="\t",
            skiprows=8,
            names=column_names,
            encoding="utf-8",
            skipinitialspace=True,
        )

        # Убираем последнюю, где Итог
        df = df.dropna(subset=["Номенклатура"], how="all")

        # Преобразование столбцов в численные выражения. Первые две это Организация и Магазин. Их пропускаем, как и Номенклатуру
        for column in column_names[2:]:
            if column not in ["Номенклатура", "Код", "По дням"]:
                df[column] = self.__column_to_float(df, column)
        return df

    def __column_to_float(self, df, column_name):
        """Необходимо для преобразования столбцов в int или float"""
        # Убираем пробелы и табуляцию между числами

        df[column_name] = df[column_name].str.replace("\xa0", "").str.replace(" ", "")

        # Если число с остатком меняем , на .. Пример: 1,000 в 1.000
        df[column_name] = df[column_name].str.replace(",", ".")
        # Преобразуем данные в численные
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
        # Проверка, чтобы Номер магазины был целочисленным
        return df[column_name]

    def __full_id_store_merge(self, df):
        """В ф-ии соединяем три датасета: продукты(Номенклатура), магазины(Названия) и наши данные"""
        # Соединяем с продуктами
        merged_df = pd.merge(
            df,
            self.product,
            left_on="Номенклатура.Код",
            right_on="id_product_code",
            how="left",
        )

        # Соединяем с магазинами

        merged_df = pd.merge(
            merged_df,
            self.store,
            left_on="Магазин",
            right_on="search_store",
            how="left",
        )
        merged_df = pd.merge(
            merged_df,
            self.inn,
            left_on="Организация",
            right_on="search_entity",
            how="left",
        )

        merged_df = pd.merge(
            merged_df,
            self.store_channel,
            left_on="id_store_rename",
            right_on="id_store",
            how="left",
        )
        words = [ 'Франшиза в аренду', 'Франшиза инвестиционная', 'ФРС']
        merged_df = merged_df[merged_df['channel'].isin(words)]
        # Заполняем пустые значения. Без этого не сможем преобразовать в целочисленный столбец
        merged_df["id_product"] = merged_df["id_product"].fillna(0)
        md = merged_df[merged_df["id_product"] == 0]['Номенклатура.Код']
        err_nomun = pd.read_csv('Номенклатура_не_в_бд.csv')


        err_nomun = pd.concat([err_nomun, md])
        err_nomun.to_csv('Номенклатура_не_в_бд.csv')
        # Этих двух магазинов нет в БД, поэтому вручную заполняю данные
        merged_df["id_product"] = merged_df["id_product"].astype(int)

        # print(merged_df[merged_df['id_product'] == 0])

        # Убираем лишние столбцы
        try:
            merged_df = merged_df.drop(
                [
                    "Номер магазина",
                    "search_store",
                    "Номенклатура.Код",
                    "id_product_code",
                    "Организация",
                    "search_entity",
                    "id_store",
                    'channel'
                ],
                axis=1,
            )
        except KeyError:
            merged_df = merged_df.drop(
                [
                    "Номер магазина",
                    "search_store",
                    "Код",
                    "Организация",
                    "id_product_code",
                    "search_entity",
                ],
                axis=1,
            )

        return merged_df

    def __full_id_store_merge_old(self, df):
        """В ф-ии соединяем три датасета: продукты(Номенклатура), магазины(Названия) и наши данные"""
        # Соединяем с продуктами

        merged_df = pd.merge(
            df, self.product, left_on="Код", right_on="id_product_code", how="left"
        )

        # Соединяем с магазинами
        merged_df = pd.merge(
            merged_df,
            self.store,
            left_on="Магазин",
            right_on="search_store",
            how="left",
        )
        merged_df = pd.merge(
            merged_df,
            self.inn,
            left_on="Организация",
            right_on="search_entity",
            how="left",
        )
        merged_df = pd.merge(
            merged_df,
            self.store_channel,
            left_on="id_store_rename",
            right_on="id_store",
            how="left",
        )
        words = ['Франшиза в аренду', 'Франшиза инвестиционная', 'ФРС']
        merged_df = merged_df[merged_df['channel'].isin(words)]
        # Заполняем пустые значения. Без этого не сможем преобразовать в целочисленный столбец
        merged_df["id_product"] = merged_df["id_product"].fillna(0)
        md = merged_df[merged_df["id_product"] == 0]['Номенклатура.Код']
        err_nomun = pd.read_csv('Номенклатура_не_в_бд.csv')

        err_nomun = pd.concat([err_nomun, md])
        err_nomun.to_csv('Номенклатура_не_в_бд.csv')

        # Убираем лишние столбцы
        merged_df = merged_df.drop(
            [
                "Номенклатура",
                "Магазин.Номер магазина",
                "Номер магазина",
                "search_store",
                "Номенклатура",
                "id_product_code",
                "Код",
                "Организация",
                "search_entity",
            ],
            axis=1,
        )

        return merged_df

    def __take_data_for_DB(self, columns_list, table, db):
        column = ''
        for i in columns_list:
            column = column + i + ','
        query = f"""SELECT {column[:-1]} FROM {table}"""

        # Создаём новое подключение в каждом вызове (или в каждом потоке)
        conn = PostConn(db=db)
        df = conn.fetch_to_dataframe(query)
        conn.close()  # Закрываем соединение после использования

        return df

    def __error_data(self, df_err):
        try:
            # Путь к вашему текстовому файлу
            file_path = "NON_DB_ERROR.txt"  # Замените на фактический путь

            # Считываем существующие строки из файла в множество
            existing_lines = set()
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as file:
                    existing_lines = set(list(file.read().splitlines()))

            # Создаем пустое множество для хранения новых уникальных строк
            new_unique_lines = set()

            line = "; ".join(df_err)

            # Проверяем, уникальна ли строка
            if line not in existing_lines:
                new_unique_lines.add(line)

            # Открываем файл в режиме добавления
            with open(file_path, "a", encoding="utf-8") as file:
                for line in new_unique_lines:
                    file.write(line + "\n")

            # Создаем пустое множество для хранения уникальных значений
            unique_values = set()

            # Чтение данных из текстового файла
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    # Убираем пробелы и разбиваем строку по запятой
                    addresses = line.split(";")
                    # Добавляем адреса в множество для уникальности
                    unique_values.update(addresses)

            # Запись уникальных значений обратно в текстовый файл
            with open(file_path, "w", encoding="utf-8") as file:
                for value in unique_values:
                    file.write(value + "\n")
        except TypeError as e:
            print(f"Вышла ошибочка: {e}")

    def __add_except(self, filename):
        file_path = "except.csv"  # Замените на фактический путь

        # Считывание данных из CSV-файла
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"Файл '{file_path}' не найден.")
            df = pd.DataFrame(columns=["Исключения"])

        # Создание DataFrame для новой строки
        new_row = pd.DataFrame({"Исключения": [filename]})

        # Объединение DataFrames
        df = pd.concat([df, new_row], ignore_index=True)

        # Запись обратно в CSV-файл
        df.to_csv(file_path, index=False)

        print("Данные успешно обновлены и сохранены.")

    # -------- ОБРАБОТКА ОДНОГО ФАЙЛА (НОВЫЙ ФОРМАТ) --------
    def __process_new_file(self, file):
        try:
            print(f"[+] Обработка нового файла: {file}")
            df = self.__take_data_for_file(os.path.join(self.path_to_directory, file))
            df = self.__full_id_store_merge(df)

            df_err = df[df["id_store_rename"] == 0]["Магазин"].unique()
            df = df.drop(["Магазин"], axis=1)
            self.__error_data(df_err)

            try:
                df["order_date"] = pd.to_datetime(file.rsplit(".", 1)[0], format="%d.%m.%Y")
            except ValueError:
                df["order_date"] = pd.to_datetime(file.rsplit(".", 1)[0], format="%d.%m.%y")


            self.__data_to_DB(df)
            self.__add_except(file)

            print(f"[✓] Готово: {file}")
            return df  # можно вернуть DataFrame, если нужно потом объединить
        except Exception as e:
            print(f"[!] Ошибка в файле {file}: {e}")
            raise e

    # -------- ОБРАБОТКА ОДНОГО ФАЙЛА (СТАРЫЙ ФОРМАТ) --------
    def __process_old_file(self, file):
        try:
            print(f"[+] Обработка старого файла: {file}")
            df = self.__take_data_for_file_old(os.path.join(self.path_to_directory_old, file))
            df = self.__full_id_store_merge_old(df)

            df_err = df[df["id_store_rename"] == 0]["Магазин"].unique()
            df = df.drop(["Магазин"], axis=1)
            self.__error_data(df_err)

            df.rename(columns={"По дням": "order_date"}, inplace=True)

            try:
                df["order_date"] = pd.to_datetime(file.rsplit(".", 1)[0], format="%d.%m.%Y")
            except ValueError:
                df["order_date"] = pd.to_datetime(file.rsplit(".", 1)[0], format="%d.%m.%y")

            for col in [
                "Начальный остаток себестоимость",
                "Начальный остаток",
                "Конечный остаток себестоимость",
                "Конечный остаток",
            ]:
                df[col] = df[col].fillna(0)

            df["id_product"] = pd.to_numeric(df["id_product"], errors="coerce").astype("Int64")
            df["id_store_rename"] = pd.to_numeric(df["id_store_rename"], errors="coerce").astype("Int64")

            self.__data_to_DB(df)
            self.__add_except(file)

            print(f"[✓] Готово: {file}")
            return df
        except Exception as e:
            print(f"[!] Ошибка в файле {file}: {e}")
            return None

