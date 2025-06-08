"""
Аналіз завантажень відділень (Завдання 2)
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

class DepartmentAnalyzer:
    def __init__(self):
        self.config = DatabaseConfig()
        self.data = None

    def load_data(self, filepath):
        """Завантажує сирі дані періодичних доставок"""
        try:
            print(f"📥 Завантаження даних відділень з {filepath}")
            self.data = pd.read_csv(filepath)
            print(f"✅ Завантажено {len(self.data)} записів періодичних доставок")
            return True
        except Exception as e:
            print(f"❌ Помилка завантаження даних: {e}")
            return False

    def analyze_department_workload(self, filepath=None):
        """
        Завдання 2: Аналіз завантажень відділень
        """
        if filepath and not self.load_data(filepath):
            return {'error': 'Не вдалося завантажити дані'}

        if self.data is None or self.data.empty:
            return {'error': 'Немає даних для аналізу'}

        try:
            print("🔄 Аналіз завантажень відділень...")

            # Конвертуємо числові колонки
            numeric_columns = ['deliveries_count', 'processing_time_hours', 'deliveries_share_percentage']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')

            # Заповнюємо NaN значення нулями
            self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

            # Основна статистика по відділенням
            dept_stats = self.data.groupby(['department_id', 'department_number', 'department_address']).agg({
                'delivery_id': 'count',
                'deliveries_count': ['sum', 'mean'],
                'processing_time_hours': ['mean', 'median', 'max'],
                'deliveries_share_percentage': 'mean',
                'parcel_type_id': 'nunique',
                'transport_body_type_id': 'nunique'
            }).round(2)

            # Сплющуємо колонки
            dept_stats.columns = [
                'total_records',
                'total_deliveries', 'avg_deliveries_per_period',
                'avg_processing_time', 'median_processing_time', 'max_processing_time',
                'avg_share_percentage',
                'parcel_types_handled', 'transport_types_used'
            ]

            # Заповнюємо NaN після агрегації
            dept_stats = dept_stats.fillna(0)

            # Рейтинг завантаженості
            dept_stats['workload_score'] = (
                (dept_stats['total_deliveries'] * 0.4) +
                (dept_stats['avg_processing_time'] * 0.3) +
                (dept_stats['avg_share_percentage'] * 0.3)
            ).round(2)

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси відділень
            dept_stats_dict = {}
            for (dept_id, dept_number, dept_address), row in dept_stats.iterrows():
                key = f"dept_{dept_id}_{dept_number}"
                dept_stats_dict[key] = {
                    'department_id': dept_id,
                    'department_number': dept_number,
                    'department_address': dept_address,
                    **row.to_dict()
                }

            # Найзавантаженіші відділення
            busiest_departments = dept_stats.nlargest(10, 'workload_score')
            busiest_dict = {}
            for (dept_id, dept_number, dept_address), row in busiest_departments.iterrows():
                key = f"dept_{dept_id}_{dept_number}"
                busiest_dict[key] = {
                    'department_id': dept_id,
                    'department_number': dept_number,
                    'department_address': dept_address,
                    **row.to_dict()
                }

            # Аналіз по типах відділень
            dept_type_stats = self.data.groupby('department_type').agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            dept_type_stats.columns = ['total_records', 'total_deliveries', 'avg_processing_time', 'departments_count']

            # Аналіз по регіонах
            region_stats = self.data.groupby('department_region').agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            region_stats.columns = ['total_records', 'total_deliveries', 'avg_processing_time', 'departments_count']

            # Аналіз по містах
            city_stats = self.data.groupby(['department_city', 'department_region']).agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            city_stats.columns = ['total_records', 'total_deliveries', 'avg_processing_time', 'departments_count']

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси міст
            city_stats_dict = {}
            for (city_name, region_name), row in city_stats.iterrows():
                key = f"{city_name}_{region_name}".replace(' ', '_')
                city_stats_dict[key] = {
                    'city_name': city_name,
                    'region_name': region_name,
                    **row.to_dict()
                }

            # Аналіз по типах посилок
            parcel_type_stats = self.data.groupby('parcel_type_name').agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            parcel_type_stats.columns = ['total_records', 'total_deliveries', 'avg_processing_time', 'departments_handling']

            # Загальна статистика
            general_stats = {
                'total_departments': int(self.data['department_id'].nunique()),
                'total_records': int(len(self.data)),
                'total_deliveries': int(self.data['deliveries_count'].sum()),
                'avg_processing_time': float(self.data['processing_time_hours'].mean()),
                'total_regions': int(self.data['department_region'].nunique()),
                'total_cities': int(self.data['department_city'].nunique()),
                'parcel_types': int(self.data['parcel_type_name'].nunique()),
                'transport_types': int(self.data['transport_type_name'].nunique())
            }

            # Збираємо результати з конвертацією типів
            results = {
                'general_stats': self._convert_numpy_types(general_stats),
                'department_workload': self._convert_numpy_types(dept_stats_dict),
                'busiest_departments': self._convert_numpy_types(busiest_dict),
                'department_type_analysis': self._convert_numpy_types(dept_type_stats.to_dict('index')),
                'region_analysis': self._convert_numpy_types(region_stats.to_dict('index')),
                'city_analysis': self._convert_numpy_types(city_stats_dict),
                'parcel_type_analysis': self._convert_numpy_types(parcel_type_stats.to_dict('index')),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо результати
            self._save_results(results, 'department_workload_analysis')

            print("✅ Аналіз завантажень відділень завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі відділень: {e}")
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