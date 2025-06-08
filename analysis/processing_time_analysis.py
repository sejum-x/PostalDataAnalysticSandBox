"""
Аналіз часу обробки посилок (Завдання 3)
Працює з delivery_periodic_raw_data
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import sys

sys.path.append('..')
from config.database_config import DatabaseConfig

class ProcessingTimeAnalyzer:
    def __init__(self):
        self.config = DatabaseConfig()
        self.data = None

    def load_data(self, filepath):
        """Завантажує сирі дані періодичних доставок"""
        try:
            print(f"📥 Завантаження даних для аналізу часу обробки з {filepath}")
            self.data = pd.read_csv(filepath)
            print(f"✅ Завантажено {len(self.data)} записів для аналізу часу обробки")
            return True
        except Exception as e:
            print(f"❌ Помилка завантаження даних: {e}")
            return False

    def analyze_processing_times(self, filepath=None):
        """
        Завдання 3: Аналіз часу обробки посилок
        """
        if filepath and not self.load_data(filepath):
            return {'error': 'Не вдалося завантажити дані'}

        if self.data is None or self.data.empty:
            return {'error': 'Немає даних для аналізу'}

        try:
            print("🔄 Аналіз часу обробки посилок...")

            # Конвертуємо числові колонки
            numeric_columns = ['processing_time_hours', 'deliveries_count', 'parcel_max_size', 'parcel_max_weight']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')

            # Заповнюємо NaN значення нулями
            self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

            # Аналіз по типах посилок
            parcel_processing = self.data.groupby(['parcel_type_name', 'parcel_max_size', 'parcel_max_weight']).agg({
                'processing_time_hours': ['mean', 'median', 'std', 'min', 'max'],
                'deliveries_count': 'sum',
                'delivery_id': 'count',
                'department_id': 'nunique'
            }).round(2)

            # Сплющуємо колонки
            parcel_processing.columns = [
                'avg_processing_time', 'median_processing_time', 'std_processing_time',
                'min_processing_time', 'max_processing_time',
                'total_deliveries', 'total_records', 'departments_handling'
            ]

            # Заповнюємо NaN після агрегації
            parcel_processing = parcel_processing.fillna(0)

            # Рейтинг складності обробки
            parcel_processing['complexity_score'] = (
                (parcel_processing['avg_processing_time'] * 0.5) +
                (parcel_processing['std_processing_time'] * 0.3) +
                (parcel_processing['max_processing_time'] * 0.2)
            ).round(2)

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси типів посилок
            parcel_processing_dict = {}
            for (parcel_type, max_size, max_weight), row in parcel_processing.iterrows():
                key = f"{parcel_type}_{max_size}_{max_weight}".replace(' ', '_').replace('*', 'x')
                parcel_processing_dict[key] = {
                    'parcel_type_name': parcel_type,
                    'parcel_max_size': max_size,
                    'parcel_max_weight': max_weight,
                    **row.to_dict()
                }

            # Найскладніші для обробки типи посилок
            complex_parcels = parcel_processing.nlargest(10, 'complexity_score')
            complex_parcels_dict = {}
            for (parcel_type, max_size, max_weight), row in complex_parcels.iterrows():
                key = f"{parcel_type}_{max_size}_{max_weight}".replace(' ', '_').replace('*', 'x')
                complex_parcels_dict[key] = {
                    'parcel_type_name': parcel_type,
                    'parcel_max_size': max_size,
                    'parcel_max_weight': max_weight,
                    **row.to_dict()
                }

            # Аналіз по відділенням
            dept_processing = self.data.groupby(['department_id', 'department_number', 'department_type']).agg({
                'processing_time_hours': ['mean', 'median', 'std'],
                'deliveries_count': 'sum',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            dept_processing.columns = [
                'avg_processing_time', 'median_processing_time', 'std_processing_time',
                'total_deliveries', 'parcel_types_handled'
            ]

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси відділень
            dept_processing_dict = {}
            for (dept_id, dept_number, dept_type), row in dept_processing.iterrows():
                key = f"dept_{dept_id}_{dept_number}"
                dept_processing_dict[key] = {
                    'department_id': dept_id,
                    'department_number': dept_number,
                    'department_type': dept_type,
                    **row.to_dict()
                }

            # Аналіз по регіонах
            region_processing = self.data.groupby('department_region').agg({
                'processing_time_hours': ['mean', 'median', 'std'],
                'deliveries_count': 'sum',
                'department_id': 'nunique',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            region_processing.columns = [
                'avg_processing_time', 'median_processing_time', 'std_processing_time',
                'total_deliveries', 'departments_count', 'parcel_types_count'
            ]

            # Аналіз по періодах
            period_processing = self.data.groupby(['start_year', 'start_month']).agg({
                'processing_time_hours': ['mean', 'median'],
                'deliveries_count': 'sum',
                'delivery_id': 'count'
            }).round(2).fillna(0)

            period_processing.columns = [
                'avg_processing_time', 'median_processing_time',
                'total_deliveries', 'total_records'
            ]

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси періодів
            period_processing_dict = {}
            for (year, month), row in period_processing.iterrows():
                key = f"{year}_{month:02d}"
                period_processing_dict[key] = {
                    'year': year,
                    'month': month,
                    **row.to_dict()
                }

            # Аналіз ефективності (час обробки vs кількість доставок)
            efficiency_analysis = self.data.groupby('department_id').agg({
                'processing_time_hours': 'mean',
                'deliveries_count': 'sum'
            }).fillna(0)

            # Безпечне ділення для ефективності
            efficiency_analysis['efficiency_ratio'] = (
                efficiency_analysis['deliveries_count'] / (efficiency_analysis['processing_time_hours'] + 0.1)
            ).round(2)

            # Топ ефективні відділення
            efficient_departments = efficiency_analysis.nlargest(10, 'efficiency_ratio')

            # Загальна статистика
            general_stats = {
                'total_records': int(len(self.data)),
                'avg_processing_time': float(self.data['processing_time_hours'].mean()),
                'median_processing_time': float(self.data['processing_time_hours'].median()),
                'min_processing_time': float(self.data['processing_time_hours'].min()),
                'max_processing_time': float(self.data['processing_time_hours'].max()),
                'total_parcel_types': int(self.data['parcel_type_name'].nunique()),
                'total_departments': int(self.data['department_id'].nunique()),
                'total_deliveries': int(self.data['deliveries_count'].sum())
            }

            # Збираємо результати з конвертацією типів
            results = {
                'general_stats': self._convert_numpy_types(general_stats),
                'parcel_type_processing': self._convert_numpy_types(parcel_processing_dict),
                'complex_parcels': self._convert_numpy_types(complex_parcels_dict),
                'department_processing': self._convert_numpy_types(dept_processing_dict),
                'region_processing': self._convert_numpy_types(region_processing.to_dict('index')),
                'period_processing': self._convert_numpy_types(period_processing_dict),
                'efficiency_analysis': self._convert_numpy_types(efficiency_analysis.to_dict('index')),
                'efficient_departments': self._convert_numpy_types(efficient_departments.to_dict('index')),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо результати
            self._save_results(results, 'processing_time_analysis')

            print("✅ Аналіз часу обробки посилок завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі часу обробки: {e}")
            return {'error': str(e)}

    def _convert_numpy_types(self, obj):
        """Конвертує numpy типи в Python типи для JSON серіалізації"""
        if isinstance(obj, dict):
            return {str(key): self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        elif isinstance(obj, tuple):
            return str(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        else:
            return obj

    def _save_results(self, results, filename_prefix):
        """Зберігає результати аналізу"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{filename_prefix}_{timestamp}.json"
            filepath = os.path.join(self.config.PROCESSED_DATA_PATH, filename)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

            print(f"💾 Результати збережено: {filename}")

        except Exception as e:
            print(f"❌ Помилка збереження результатів: {e}")