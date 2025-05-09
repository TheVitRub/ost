# Ост – Система обработки данных

Этот проект предназначен для обработки файлов с данными остатков продукции, их агрегации и загрузки в базы данных ClickHouse и PostgreSQL. Скрипты осуществляют:

- Чтение файлов (нового и старого форматов) из указанных директорий (`files` и `old_files`);
- Преобразование и агрегацию данных, включая группировку по ключевым столбцам (например, `id_product`, `id_store`, `inn`, `order_date`);
- Объединение данных с дополнительной информацией (каталоги магазинов, товаров, ИНН юридических лиц) из PostgreSQL;
- Загрузку обработанных данных в ClickHouse (с возможностью удаления данных по диапазону дат перед вставкой).

Проект использует многопоточность (ThreadPoolExecutor) для параллельной обработки файлов.

---

## Структура репозитория

- **requirements.txt**  
  Содержит список необходимых зависимостей, которые требуются для работы проекта.

- **itog_main.py**  
  Главный скрипт запуска, который инициализирует класс обработки данных (`WorkForData`) и запускает процесс обработки.

- **click_house_connect.py**  
  Модуль для подключения к базе данных ClickHouse, выполнения запросов, загрузки данных и предоставления DataFrame из результата запросов.

- **postgress_connect.py**  
  Модуль для подключения к PostgreSQL, реализации функций загрузки (INSERT, UPSERT) и извлечения данных в виде pandas DataFrame.

- **work_data_itog.py**  
  Основной модуль, содержащий класс `WorkForData`, который:
  - Считывает метаданные (данные магазинов, продуктов, ИНН) из PostgreSQL для последующего объединения с данными из файлов,
  - Обрабатывает файлы с новыми и старыми данными,
  - Приводит данные к необходимым типам (числовые, даты),
  - Выполняет агрегацию данных и загрузку их в ClickHouse (а также, опционально, в PostgreSQL).

---

## Установка

1. **Клонируйте репозиторий**:
   ```bash
   git clone <URL_вашего_репозитория>
   cd ost
