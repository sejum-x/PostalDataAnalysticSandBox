"""
Модуль для отримання СИРИХ даних з Data Warehouse
"""

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
import warnings

warnings.filterwarnings('ignore')
sys.path.append('..')

from config.database_config import DatabaseConfig
from data_extraction.sql_queries import DWQueries

class DataWarehouseExtractor:
    def __init__(self):
        self.config = DatabaseConfig()
        self.queries = DWQueries()

    def test_connection(self):
        """Тестує підключення до Data Warehouse"""
        print("🔍 Тестування підключення до Data Warehouse...")

        try:
            print(f"🔗 Підключення до: {self.config.SERVER}")
            print(f"📊 Data Warehouse: {self.config.DATABASE}")

            connection = pyodbc.connect(self.config.CONNECTION_STRING)
            cursor = connection.cursor()

            # Тестовий запит
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            print(f"✅ Підключення успішне!")
            print(f"📋 Версія SQL Server: {version[:50]}...")

            # Перевіряємо наявність таблиць DW
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND (TABLE_NAME LIKE '%Fact' OR TABLE_NAME LIKE '%Dim')
                ORDER BY TABLE_NAME
            """)

            tables = [row[0] for row in cursor.fetchall()]
            print(f"📊 Знайдено DW таблиць: {len(tables)}")

            if tables:
                print("📋 Таблиці Data Warehouse:")
                fact_tables = [t for t in tables if 'Fact' in t]
                dim_tables = [t for t in tables if 'Dim' in t]

                if fact_tables:
                    print("   🎯 Таблиці фактів:")
                    for table in fact_tables:
                        print(f"      • {table}")

                if dim_tables:
                    print("   📐 Таблиці вимірів:")
                    for table in dim_tables:
                        print(f"      • {table}")

            connection.close()
            return True, "Підключення до DW успішне"

        except Exception as e:
            print(f"❌ Помилка підключення до DW: {e}")
            return False, str(e)

    def get_connection(self):
        """Створює підключення до Data Warehouse"""
        try:
            connection = pyodbc.connect(self.config.CONNECTION_STRING)
            return connection
        except Exception as e:
            print(f"❌ Помилка підключення до DW: {e}")
            return None

    def extract_courier_delivery_data(self):
        """Завдання 1: Сирі дані кур'єрської доставки"""
        query = self.queries.get_courier_delivery_data()
        return self._execute_query(query, 'courier_delivery_raw_data')

    def extract_delivery_periodic_data(self):
        """Завдання 2,3,4: Сирі дані з DeliveryPeriodicFact для всіх аналізів"""
        query = self.queries.get_delivery_periodic_data()
        return self._execute_query(query, 'delivery_periodic_raw_data')

    def _execute_query(self, query, filename_prefix):
        """Виконує запит та зберігає СИРІ дані у CSV"""
        connection = self.get_connection()

        if not connection:
            return {'success': False, 'filename': None, 'error': 'Немає підключення до DW'}

        try:
            print(f"🔄 Отримання сирих даних: {filename_prefix}...")
            df = pd.read_sql(query, connection)

            if df.empty:
                print(f"⚠️ Запит {filename_prefix} повернув пусті дані")
                return {'success': False, 'filename': None, 'error': 'Запит повернув пусті дані'}

            # Генеруємо ім'я файлу з часовою міткою
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{filename_prefix}_{timestamp}.csv"
            filepath = os.path.join(self.config.RAW_DATA_PATH, filename)

            # Зберігаємо СИРІ дані у CSV
            df.to_csv(filepath, index=False, encoding='utf-8')

            print(f"✅ {filename_prefix}: {len(df)} записів збережено в {filename}")
            print(f"📊 Колонки: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")

            return {
                'success': True,
                'filename': filename,
                'filepath': filepath,
                'records_count': len(df),
                'columns': list(df.columns)
            }

        except Exception as e:
            print(f"❌ Помилка при отриманні сирих даних {filename_prefix}: {e}")
            return {'success': False, 'filename': None, 'error': str(e)}

        finally:
            connection.close()

    def extract_all_raw_data(self):
        """Отримує всі СИРІ дані з Data Warehouse"""
        print("🚀 Початок отримання СИРИХ даних з Data Warehouse...")

        # Спочатку тестуємо підключення
        success, message = self.test_connection()
        if not success:
            print(f"❌ {message}")
            return {'error': 'Немає підключення до DW'}

        print(f"✅ {message}")

        # Отримуємо сирі дані згідно завдань
        extractors = {
            'courier_delivery': self.extract_courier_delivery_data,
            'delivery_periodic': self.extract_delivery_periodic_data
        }

        results = {}

        for name, extractor in extractors.items():
            print(f"\n📥 Отримання {name} сирих даних...")
            results[name] = extractor()

        # Підсумок
        successful = sum(1 for result in results.values() if result.get('success', False))
        total = len(results)

        print(f"\n📊 Підсумок отримання сирих даних з DW: {successful}/{total} успішно")

        return results