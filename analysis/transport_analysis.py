"""
Аналіз використання транспорту (Завдання 4)
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

class TransportAnalyzer:
    def __init__(self):
        self.config = DatabaseConfig()
        self.data = None

    def load_data(self, filepath):
        """Завантажує сирі дані періодичних доставок"""
        try:
            print(f"📥 Завантаження даних для аналізу транспорту з {filepath}")
            self.data = pd.read_csv(filepath)
            print(f"✅ Завантажено {len(self.data)} записів для аналізу транспорту")
            return True
        except Exception as e:
            print(f"❌ Помилка завантаження даних: {e}")
            return False

    def analyze_transport_utilization(self, filepath=None):
        """
        Завдання 4: Аналіз використання транспорту
        """
        if filepath and not self.load_data(filepath):
            return {'error': 'Не вдалося завантажити дані'}

        if self.data is None or self.data.empty:
            return {'error': 'Немає даних для аналізу'}

        try:
            print("🔄 Аналіз використання транспорту...")

            # Конвертуємо числові колонки
            numeric_columns = ['deliveries_count', 'processing_time_hours', 'deliveries_share_percentage',
                              'parcel_max_weight', 'parcel_max_size']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')

            # Заповнюємо NaN значення нулями
            self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

            # Основна статистика по типах транспорту
            transport_stats = self.data.groupby(['transport_body_type_id', 'transport_type_name']).agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'deliveries_share_percentage': 'mean',
                'department_id': 'nunique',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            transport_stats.columns = [
                'total_records', 'total_deliveries', 'avg_processing_time',
                'avg_share_percentage', 'departments_served', 'parcel_types_handled'
            ]

            # Рейтинг використання транспорту
            transport_stats['utilization_score'] = (
                (transport_stats['total_deliveries'] * 0.4) +
                (transport_stats['departments_served'] * 0.3) +
                (transport_stats['avg_share_percentage'] * 0.3)
            ).round(2)

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси транспорту
            transport_stats_dict = {}
            for (transport_id, transport_name), row in transport_stats.iterrows():
                key = f"transport_{transport_id}_{transport_name.replace(' ', '_')}"
                transport_stats_dict[key] = {
                    'transport_body_type_id': transport_id,
                    'transport_type_name': transport_name,
                    **row.to_dict()
                }

            # Найбільш використовувані типи транспорту
            most_used_transport = transport_stats.nlargest(10, 'utilization_score')
            most_used_dict = {}
            for (transport_id, transport_name), row in most_used_transport.iterrows():
                key = f"transport_{transport_id}_{transport_name.replace(' ', '_')}"
                most_used_dict[key] = {
                    'transport_body_type_id': transport_id,
                    'transport_type_name': transport_name,
                    **row.to_dict()
                }

            # Аналіз транспорту по типах посилок
            transport_parcel_analysis = self.data.groupby(['transport_type_name', 'parcel_type_name']).agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'parcel_max_weight': 'mean',
                'parcel_max_size': 'mean'
            }).round(2).fillna(0)

            transport_parcel_analysis.columns = [
                'total_records', 'total_deliveries', 'avg_processing_time',
                'avg_parcel_weight', 'avg_parcel_size'
            ]

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси транспорт-посилка
            transport_parcel_dict = {}
            for (transport_name, parcel_name), row in transport_parcel_analysis.iterrows():
                key = f"{transport_name}_{parcel_name}".replace(' ', '_')
                transport_parcel_dict[key] = {
                    'transport_type_name': transport_name,
                    'parcel_type_name': parcel_name,
                    **row.to_dict()
                }

            # Аналіз транспорту по регіонах
            transport_region_analysis = self.data.groupby(['transport_type_name', 'department_region']).agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            transport_region_analysis.columns = [
                'total_records', 'total_deliveries', 'avg_processing_time', 'departments_count'
            ]

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси транспорт-регіон
            transport_region_dict = {}
            for (transport_name, region_name), row in transport_region_analysis.iterrows():
                key = f"{transport_name}_{region_name}".replace(' ', '_')
                transport_region_dict[key] = {
                    'transport_type_name': transport_name,
                    'region_name': region_name,
                    **row.to_dict()
                }

            # Аналіз ефективності транспорту
            transport_efficiency = self.data.groupby('transport_type_name').agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'deliveries_share_percentage': 'mean'
            }).round(2).fillna(0)

            # Безпечне ділення для ефективності
            transport_efficiency['efficiency_ratio'] = (
                transport_efficiency['deliveries_count'] / (transport_efficiency['processing_time_hours'] + 0.1)
            ).round(2)

            # Найефективніші типи транспорту
            efficient_transport = transport_efficiency.nlargest(5, 'efficiency_ratio')

            # Аналіз по періодах
            period_transport_analysis = self.data.groupby(['start_year', 'start_month', 'transport_type_name']).agg({
                'delivery_id': 'count',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean'
            }).round(2).fillna(0)

            period_transport_analysis.columns = ['total_records', 'total_deliveries', 'avg_processing_time']

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси період-транспорт
            period_transport_dict = {}
            for (year, month, transport_name), row in period_transport_analysis.iterrows():
                key = f"{year}_{month:02d}_{transport_name.replace(' ', '_')}"
                period_transport_dict[key] = {
                    'year': year,
                    'month': month,
                    'transport_type_name': transport_name,
                    **row.to_dict()
                }

            # Аналіз завантаженості транспорту по відділенням
            dept_transport_analysis = self.data.groupby(['department_id', 'department_number']).agg({
                'transport_body_type_id': 'nunique',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean'
            }).round(2).fillna(0)

            dept_transport_analysis.columns = ['transport_types_used', 'total_deliveries', 'avg_processing_time']

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси відділень
            dept_transport_dict = {}
            for (dept_id, dept_number), row in dept_transport_analysis.iterrows():
                key = f"dept_{dept_id}_{dept_number}"
                dept_transport_dict[key] = {
                    'department_id': dept_id,
                    'department_number': dept_number,
                    **row.to_dict()
                }

            # Загальна статистика
            general_stats = {
                'total_transport_types': int(self.data['transport_type_name'].nunique()),
                'total_records': int(len(self.data)),
                'total_deliveries': int(self.data['deliveries_count'].sum()),
                'avg_processing_time': float(self.data['processing_time_hours'].mean()),
                'departments_using_transport': int(self.data['department_id'].nunique()),
                'parcel_types_transported': int(self.data['parcel_type_name'].nunique()),
                'regions_served': int(self.data['department_region'].nunique())
            }

            # Збираємо результати з конвертацією типів
            results = {
                'general_stats': self._convert_numpy_types(general_stats),
                'transport_utilization': self._convert_numpy_types(transport_stats_dict),
                'most_used_transport': self._convert_numpy_types(most_used_dict),
                'transport_parcel_analysis': self._convert_numpy_types(transport_parcel_dict),
                'transport_region_analysis': self._convert_numpy_types(transport_region_dict),
                'transport_efficiency': self._convert_numpy_types(transport_efficiency.to_dict('index')),
                'efficient_transport': self._convert_numpy_types(efficient_transport.to_dict('index')),
                'period_analysis': self._convert_numpy_types(period_transport_dict),
                'department_transport_analysis': self._convert_numpy_types(dept_transport_dict),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо результати
            self._save_results(results, 'transport_utilization_analysis')

            print("✅ Аналіз використання транспорту завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі транспорту: {e}")
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