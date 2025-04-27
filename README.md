# pr_08
## Практическая работа 8. Анализ метода загрузки данных
## Цель:
Определить наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL, сравнивая время выполнения для методов: pandas.to_sql(), psycopg2.copy_expert() (с файлом и с io.StringIO), и пакетная вставка (psycopg2.extras.execute_values).
## Задачи:
1. Подключиться к предоставленной базе данных PostgreSQL.
2. Проанализировать структуру исходных CSV-файлов (upload_test_data.csv ,  upload_test_data_big.csv).
3. Создать эскизы ER-диаграмм для таблиц, соответствующих структуре CSV-файлов.
4. Реализовать три различных метода загрузки данных в PostgreSQL(pandas.to_sql(), copy_expert(), io.StringIO).
5. Измерить время, затраченное каждым методом на загрузку данных из малого файла (upload_test_data.csv).
6. Измерить время, затраченное каждым методом на загрузку данных из большого файла (upload_test_data_big.csv).
7. Визуализировать результаты сравнения времени загрузки с помощью гистограммы (matplotlib).
8. Сделать выводы об эффективности каждого метода для разных объемов данных.
## Выполнение работы
Перед началом работы выполним подключение к обновленным билбиотекам
````
%pip install psycopg2-binary pandas sqlalchemy matplotlib numpy
````
Получаем результат:

![image](https://github.com/user-attachments/assets/2184d486-0de0-4d43-a65b-0712155b1870)

Теперь подключаем несколько библиотек для работы с базами данных, анализа данных и визуализации
````
import psycopg2
from psycopg2 import Error
from psycopg2 import extras # For execute_values
import pandas as pd
from sqlalchemy import create_engine
import io # For StringIO
import time
import matplotlib.pyplot as plt
import numpy as np
import os # To check file existence
````
Далее указываем путь к файлам csv и подключимся к БД
````
small_csv_path = r'"C:\Users\Пользователь\Desktop\SQL\upload_test_data.csv"'
````
````
small_csv_path = r'"C:\Users\Пользователь\Desktop\SQL\upload_test_data_big.csv"'
````
````
!ls
````
````
# @markdown Установка и импорт необходимых библиотек.


print("Libraries installed and imported successfully.")

# Database connection details (replace with your actual credentials if different)
DB_USER = "postgres"
DB_PASSWORD = "26102006"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "upload_test_data"

# CSV File Paths (Ensure these files are uploaded to your Colab environment)
small_csv_path = 'upload_test_data.csv'
big_csv_path = 'upload_test_data_big.csv' # Corrected filename

# Table name in PostgreSQL
table_name = 'sales_data'
````
Проведем сравнение скорости загрузки данных в PostgreSQL разными методами (pandas.to_sql, copy_expert из файла и StringIO) для маленького и большого CSV-файлов, с замером времени выполнения.
````
# @title # 6. Data Loading Methods Implementation and Timing
# @markdown Реализация и измерение времени для каждого метода загрузки.

# Dictionary to store timing results
timing_results = {
    'small_file': {},
    'big_file': {}
}

# Check if files exist before proceeding
if not os.path.exists(small_csv_path):
    print(f"ERROR: Small CSV file not found: {small_csv_path}. Upload it and restart.")
elif not os.path.exists(big_csv_path):
    print(f"ERROR: Big CSV file not found: {big_csv_path}. Upload it and restart.")
elif not connection or not cursor or not engine:
    print("ERROR: Database connection not ready. Cannot proceed.")
else:
    # --- Method 1: pandas.to_sql() ---
    def load_with_pandas_to_sql(eng, df, tbl_name, chunk_size=1000):
        """Loads data using pandas.to_sql() and returns time taken."""
        start_time = time.perf_counter()
        try:
            # Using method='multi' might be faster for some DBs/data
            # Chunksize helps manage memory for large files
            df.to_sql(tbl_name, eng, if_exists='append', index=False, method='multi', chunksize=chunk_size)
        except Exception as e:
             print(f"Error in pandas.to_sql: {e}")
             # Note: No explicit transaction management here, relies on SQLAlchemy/DBAPI defaults or engine settings.
             # For critical data, wrap in a try/except with explicit rollback if needed.
             raise # Re-raise the exception to signal failure
        end_time = time.perf_counter()
        return end_time - start_time

    # --- Method 2: psycopg2.copy_expert() with CSV file ---
    def load_with_copy_expert_file(conn, cur, tbl_name, file_path):
        """Loads data using psycopg2.copy_expert() directly from file and returns time taken."""
        start_time = time.perf_counter()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Skip header row using COPY options
                sql_copy = f"""
                COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
                """
                cur.copy_expert(sql=sql_copy, file=f)
            conn.commit() # Commit transaction after successful COPY
        except (Exception, Error) as error:
            print(f"Error in copy_expert (file): {error}")
            conn.rollback() # Rollback on error
            raise
        end_time = time.perf_counter()
        return end_time - start_time

    # --- Method 3: psycopg2.copy_expert() with io.StringIO ---
    def load_with_copy_expert_stringio(conn, cur, df, tbl_name):
        """Loads data using psycopg2.copy_expert() from an in-memory StringIO buffer and returns time taken."""
        start_time = time.perf_counter()
        buffer = io.StringIO()
        # Write dataframe to buffer as CSV, including header
        df.to_csv(buffer, index=False, header=True, sep=',')
        buffer.seek(0) # Rewind buffer to the beginning
        try:
            sql_copy = f"""
            COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """
            cur.copy_expert(sql=sql_copy, file=buffer)
            conn.commit() # Commit transaction after successful COPY
        except (Exception, Error) as error:
            print(f"Error in copy_expert (StringIO): {error}")
            conn.rollback() # Rollback on error
            raise
        finally:
            buffer.close() # Ensure buffer is closed
        end_time = time.perf_counter()
        return end_time - start_time


    # --- Timing Execution ---
    print("\n--- Starting Data Loading Tests ---")

    # Load DataFrames (only once)
    print("Loading CSV files into Pandas DataFrames...")
    try:
        df_small = pd.read_csv(small_csv_path)
        # The big file might be too large to load fully into Colab memory.
        # If memory errors occur, consider processing it in chunks for methods
        # that support it (like pandas.to_sql with chunksize, or modify batch insert).
        # For COPY methods, memory isn't usually an issue as they stream.
        df_big = pd.read_csv(big_csv_path)
        print(f"Loaded {len(df_small)} rows from {small_csv_path}")
        print(f"Loaded {len(df_big)} rows from {big_csv_path}")
    except MemoryError:
        print("\nERROR: Not enough RAM to load the large CSV file into a Pandas DataFrame.")
        print("Some methods (pandas.to_sql, StringIO, Batch Insert) might fail or be inaccurate.")
        print("The copy_expert (file) method should still work.")
        # We can try to proceed, but note the limitation
        df_big = None # Indicate that the big dataframe couldn't be loaded
    except Exception as e:
        print(f"Error loading CSVs into DataFrames: {e}")
        df_small, df_big = None, None # Stop execution if loading fails


    if df_small is not None: # Proceed only if small DF loaded
        # --- Small File Tests ---
        print(f"\n--- Testing with Small File ({small_csv_path}) ---")

        # Test pandas.to_sql
        try:
            reset_table(connection, cursor, table_name)
            print("Running pandas.to_sql...")
            t = load_with_pandas_to_sql(engine, df_small, table_name)
            timing_results['small_file']['pandas.to_sql'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"pandas.to_sql failed for small file.")

        # Test copy_expert (file)
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (file)...")
            t = load_with_copy_expert_file(connection, cursor, table_name, small_csv_path)
            timing_results['small_file']['copy_expert (file)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (file) failed for small file.")

        # Test copy_expert (StringIO)
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (StringIO)...")
            t = load_with_copy_expert_stringio(connection, cursor, df_small, table_name)
            timing_results['small_file']['copy_expert (StringIO)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (StringIO) failed for small file.")



    # --- Big File Tests ---
    print(f"\n--- Testing with Big File ({big_csv_path}) ---")

    # Test pandas.to_sql (if df_big loaded)
    if df_big is not None:
        try:
            reset_table(connection, cursor, table_name)
            print("Running pandas.to_sql...")
            t = load_with_pandas_to_sql(engine, df_big, table_name, chunk_size=10000) # Larger chunksize for big file
            timing_results['big_file']['pandas.to_sql'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"pandas.to_sql failed for big file.")
    else:
        print("Skipping pandas.to_sql for big file (DataFrame not loaded).")


    # Test copy_expert (file) - This should work even if df_big didn't load
    try:
        reset_table(connection, cursor, table_name)
        print("Running copy_expert (file)...")
        t = load_with_copy_expert_file(connection, cursor, table_name, big_csv_path)
        timing_results['big_file']['copy_expert (file)'] = t
        print(f"Finished in {t:.4f} seconds.")
    except Exception as e: print(f"copy_expert (file) failed for big file.")


    # Test copy_expert (StringIO) (if df_big loaded)
    if df_big is not None:
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (StringIO)...")
            t = load_with_copy_expert_stringio(connection, cursor, df_big, table_name)
            timing_results['big_file']['copy_expert (StringIO)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (StringIO) failed for big file.")
    else:
        print("Skipping copy_expert (StringIO) for big file (DataFrame not loaded).")



    print("\n--- Data Loading Tests Finished ---")

# Final check of results dictionary
print("\nTiming Results Summary:")
import json
print(json.dumps(timing_results, indent=2))
````
Получаем следующий результат:

![image](https://github.com/user-attachments/assets/34cb1a62-da70-4690-b218-56e4ee6cec43)

Визуализация результатов сравнения времени загрузки:

![image](https://github.com/user-attachments/assets/730d4cde-0ea6-441e-bd53-bf4b5f149f53)

# Вариант 3
## Индивидуальные задания
1. Настройка таблиц. Создать таблицы sales_small, sales_big.
2. Загрузка малых данных. Метод: copy_expert (file)
3. Загрузка больших данных. Метод: copy_expert (file)
4. SQL Анализ. SQL: Подсчитать записи в sales_big, где cost < 1.00.
5. Python/Colab Анализ и Визуализация. Python: Рассчитать основные статистики (describe()) для total_revenue из sales_big (LIMIT 50000).

Перед тем как приступить к выполнению заданий убедимся, что все вспомогательные функции из шаблона выполняются, сделаем это перед запуском кода варианта

Получаем результат:

![image](https://github.com/user-attachments/assets/22f693f9-acef-4f43-aa5e-76c2f33c307f)

Предворительно прописав базовый код с импортами, константами, функциями, переходим к выполнению задания.

## Задание 1. Настройка таблиц. Создать таблицы sales_small, sales_big.
````
print("\n--- Задача 1: Создание таблиц ---")
create_table(small_table_name)
create_table(big_table_name)
````
Получаем результат:

![image](https://github.com/user-attachments/assets/3dc3934d-5fe9-404e-aa1e-6bac717558d3)

Проверяем наличие созданных таблиц в pgAdmin:

![image](https://github.com/user-attachments/assets/d5ed4302-b8f8-4cbe-9766-36892da7e3d9)

## Задание 2. Загрузка малых данных. Метод: copy_expert (file)
````
print(f"\n--- Задача 2: Загрузка данных из '{small_csv_path}' в '{small_table_name}' ---")
    if os.path.exists(small_csv_path):
        load_via_copy_file(small_csv_path, small_table_name)
    else:
        print(f"ОШИБКА: Файл '{small_csv_path}' не найден.")
````
Получаем следующий результат:

![image](https://github.com/user-attachments/assets/b73634e7-a5b7-464e-96a2-682c73609f83)

## Задание 3. Загрузка больших данных. Метод: copy_expert (file)
````
print(f"\n--- Задача 3: Загрузка данных из '{big_csv_path}' в '{big_table_name}' (метод file) ---")
    if os.path.exists(big_csv_path):
        load_via_copy_file(big_csv_path, big_table_name)
    else:
        print(f"ОШИБКА: Файл '{big_csv_path}' не найден. Загрузка не выполнена.")
````
Получаем результат:

![image](https://github.com/user-attachments/assets/38b6cf2f-db25-46f1-bf66-236b4d350bc6)

## Задание 4. SQL Анализ. SQL: Подсчитать записи в sales_big, где cost < 1.00
````
print("\n--- Задача 4: SQL Анализ таблицы sales_small ---")
    sql_query_task4 = f"""
    SELECT COUNT(*) 
    FROM sales_big 
    WHERE cost < 1.00;
    """
    print("Выполнение SQL запроса:")
    print(sql_query_task4)
    results_task4 = execute_sql(sql_query_task4, fetch=True)

    if results_task4 is not None:
        print("\nРезультаты запроса (id, total_revenue):")
        if results_task4:
            for row in results_task4:
                print(row)
        else:
            print("Запрос успешно выполнен, но не вернул строк.")
    else:
        print("Ошибка выполнения SQL запроса.")
````
Получаем результат:

![image](https://github.com/user-attachments/assets/31f1120c-480d-40f2-9716-ee1651e21d5d)

## Задание 5. Python/Colab Анализ и Визуализация. Python: Рассчитать основные статистики (describe()) для total_revenue из sales_big (LIMIT 50000).
````
print("\n--- Задача 5: Анализ и визуализация данных из sales_big с помощью Python ---")
    sql_query_task5 = f"SELECT total_revenue FROM {big_table_name} LIMIT 50000;"
    print(f"Получение данных для анализа и визуализации: {sql_query_task5}")

    df_plot_data = load_df_from_sql(sql_query_task5)

    if df_plot_data is not None and not df_plot_data.empty:
        print(f"Загружено {len(df_plot_data)} строк для анализа и визуализации данных.")
    
        
        print("\nОсновные статистики для total_revenue:")
        print(df_plot_data['total_revenue'].describe())
    
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(df_plot_data['total_revenue'], bins=50, color='skyblue', edgecolor='black', alpha=0.7)
        ax.set_xlabel('Общая выручка (Total Revenue)')
        ax.set_ylabel('Количество записей')
        ax.set_title('Гистограмма: Распределение общей выручки из sales_big (LIMIT 50000)')
        ax.grid(True, linestyle='--', alpha=0.6)
        plt.show()

    elif df_plot_data is not None and df_plot_data.empty:
        print("Запрос выполнен, но данные из sales_big для анализа и визуализации не получены.")
    else:
        print("Не удалось загрузить данные из sales_big для анализа и визуализации.")

````
Получаем результат:

![image](https://github.com/user-attachments/assets/bff0fbe2-de4d-43b2-93a7-1a094b1938be)
![image](https://github.com/user-attachments/assets/307fd468-093c-44aa-a049-10da42a40434)

# Вывод
В ходе данной работы мы определили наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL, провели анализ данных и визуализировали их.
