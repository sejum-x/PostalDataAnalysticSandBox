"""
Допоміжні функції
"""

import pandas as pd
import numpy as np
import os
import glob
import time
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

def get_latest_csv_file(directory, pattern):
    """Знаходить найновіший CSV файл за патерном"""
    files = glob.glob(f"{directory}{pattern}")
    if not files:
        return None
    return max(files, key=os.path.getctime)

def create_directories():
    """Створює необхідні директорії"""
    directories = [
        'data/raw',
        'data/processed',
        'reports/output',
        'visualizations/output',
        'visualizations/output/interactive'
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def clean_old_files(directory, days_old=7):
    """Видаляє старі файли"""
    if not os.path.exists(directory):
        return

    now = time.time()
    cutoff = now - (days_old * 86400)

    deleted_count = 0

    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            if os.path.getmtime(filepath) < cutoff:
                try:
                    os.remove(filepath)
                    print(f"🗑️ Видалено: {filename}")
                    deleted_count += 1
                except Exception as e:
                    print(f"❌ Не вдалося видалити {filename}: {e}")

    if deleted_count == 0:
        print(f"ℹ️ Файлів для видалення не знайдено в {directory}")
    else:
        print(f"✅ Видалено {deleted_count} файлів з {directory}")

def validate_csv_file(file_path):
    """Перевіряє валідність CSV файлу"""
    try:
        if not os.path.exists(file_path):
            return False, "Файл не існує"

        if os.path.getsize(file_path) == 0:
            return False, "Файл пустий"

        df = pd.read_csv(file_path, nrows=5)

        if len(df) == 0:
            return False, "Файл не містить даних"

        return True, f"Файл валідний, {len(df.columns)} колонок"

    except Exception as e:
        return False, f"Помилка читання файлу: {e}"

def safe_numeric_conversion(series):
    """Безпечна конвертація в числовий тип для NumPy 2.x"""
    return pd.to_numeric(series, errors='coerce')

def calculate_percentiles(data, percentiles=[25, 50, 75, 90, 95]):
    """Розраховує перцентилі для даних"""
    return {f'p{p}': float(np.percentile(data.dropna(), p)) for p in percentiles}

if __name__ == "__main__":
    create_directories()
    print("✅ Директорії створено!")