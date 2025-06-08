"""
Конфігурація бази даних та шляхів
"""

import os
from datetime import datetime

class DatabaseConfig:
    def __init__(self):
        # Налаштування бази даних для LocalDB
        self.SERVER = '(localdb)\\Local'  # або '(localdb)\\MSSQLLocalDB'
        self.DATABASE = 'PostDW'  # твоя база даних

        # Варіанти підключення
        self.USE_WINDOWS_AUTH = True  # Використовувати Windows Authentication

        if self.USE_WINDOWS_AUTH:
            # Windows Authentication (Integrated Security)
            self.CONNECTION_STRING = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SERVER};DATABASE={self.DATABASE};Trusted_Connection=yes;"
        else:
            # SQL Server Authentication (якщо потрібно)
            self.USERNAME = 'sa'
            self.PASSWORD = 'your_password'
            self.CONNECTION_STRING = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SERVER};DATABASE={self.DATABASE};UID={self.USERNAME};PWD={self.PASSWORD}"

        # Шляхи до файлів
        self.BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.RAW_DATA_PATH = os.path.join(self.BASE_PATH, 'data', 'raw', '')
        self.PROCESSED_DATA_PATH = os.path.join(self.BASE_PATH, 'data', 'processed', '')
        self.CHARTS_PATH = os.path.join(self.BASE_PATH, 'visualizations', 'output', '')
        self.REPORTS_PATH = os.path.join(self.BASE_PATH, 'reports', 'output', '')

        # Створюємо директорії
        self._create_directories()

    def _create_directories(self):
        """Створює необхідні директорії"""
        directories = [
            self.RAW_DATA_PATH,
            self.PROCESSED_DATA_PATH,
            self.CHARTS_PATH,
            self.REPORTS_PATH
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def get_alternative_connection_strings(self):
        """Повертає альтернативні рядки підключення для тестування"""
        alternatives = [
            # Основний варіант
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SERVER};DATABASE={self.DATABASE};Trusted_Connection=yes;",

            # Альтернативний LocalDB
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=(localdb)\\MSSQLLocalDB;DATABASE={self.DATABASE};Trusted_Connection=yes;",

            # З явним вказанням користувача Windows
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.SERVER};DATABASE={self.DATABASE};Integrated Security=SSPI;",

            # Для старіших версій ODBC Driver
            f"DRIVER={{SQL Server}};SERVER={self.SERVER};DATABASE={self.DATABASE};Trusted_Connection=yes;",
        ]
        return alternatives