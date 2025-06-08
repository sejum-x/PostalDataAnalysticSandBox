"""
Аналіз продуктивності кур'єрів (Завдання 1)
Працює з courier_delivery_raw_data
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import sys

sys.path.append('..')
from config.database_config import DatabaseConfig

class CourierAnalyzer:
    def __init__(self):
        self.config = DatabaseConfig()
        self.data = None

    def load_data(self, filepath):
        """Завантажує сирі дані кур'єрів"""
        try:
            print(f"📥 Завантаження даних кур'єрів з {filepath}")
            self.data = pd.read_csv(filepath)
            print(f"✅ Завантажено {len(self.data)} записів кур'єрських доставок")
            return True
        except Exception as e:
            print(f"❌ Помилка завантаження даних: {e}")
            return False

    def analyze_courier_performance(self, filepath=None):
        """
        Завдання 1: Аналіз продуктивності кур'єрів
        """
        if filepath and not self.load_data(filepath):
            return {'error': 'Не вдалося завантажити дані'}

        if self.data is None or self.data.empty:
            return {'error': 'Немає даних для аналізу'}

        try:
            print("🔄 Аналіз продуктивності кур'єрів...")

            # Конвертуємо числові колонки
            numeric_columns = ['delivery_time_minutes', 'improvement_minutes', 'parcel_weight', 'parcel_size']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')

            # Заповнюємо NaN значення нулями
            self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

            # Основна статистика по кур'єрам
            courier_stats = self.data.groupby(['courier_id', 'courier_name']).agg({
                'courier_delivery_id': 'count',
                'delivery_time_minutes': ['mean', 'median', 'std', 'min', 'max'],
                'improvement_minutes': ['mean', 'sum'],
                'parcel_weight': ['mean', 'sum'],
                'parcel_size': 'mean'
            }).round(2)

            # Сплющуємо колонки
            courier_stats.columns = [
                'total_deliveries',
                'avg_delivery_time', 'median_delivery_time', 'std_delivery_time',
                'min_delivery_time', 'max_delivery_time',
                'avg_improvement', 'total_improvement',
                'avg_parcel_weight', 'total_weight_delivered',
                'avg_parcel_size'
            ]

            # Заповнюємо NaN після агрегації
            courier_stats = courier_stats.fillna(0)

            # Рейтинг кур'єрів (безпечне ділення)
            courier_stats['efficiency_score'] = (
                (courier_stats['total_deliveries'] * 0.3) +
                (100 / (courier_stats['avg_delivery_time'] + 1) * 0.4) +
                (courier_stats['total_improvement'] * 0.3)
            ).round(2)

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси в строки
            courier_stats_dict = {}
            for (courier_id, courier_name), row in courier_stats.iterrows():
                key = f"courier_{courier_id}_{courier_name.replace(' ', '_')}"
                courier_stats_dict[key] = {
                    'courier_id': courier_id,
                    'courier_name': courier_name,
                    **row.to_dict()
                }

            # Топ кур'єри
            top_couriers = courier_stats.nlargest(10, 'efficiency_score')
            top_couriers_dict = {}
            for (courier_id, courier_name), row in top_couriers.iterrows():
                key = f"courier_{courier_id}_{courier_name.replace(' ', '_')}"
                top_couriers_dict[key] = {
                    'courier_id': courier_id,
                    'courier_name': courier_name,
                    **row.to_dict()
                }

            # Аналіз по регіонах
            region_stats = self.data.groupby('region_name').agg({
                'courier_delivery_id': 'count',
                'delivery_time_minutes': 'mean',
                'courier_id': 'nunique'
            }).round(2).fillna(0)

            region_stats.columns = ['total_deliveries', 'avg_delivery_time', 'unique_couriers']

            # Аналіз по містах
            city_stats = self.data.groupby(['city_name', 'region_name']).agg({
                'courier_delivery_id': 'count',
                'delivery_time_minutes': 'mean',
                'courier_id': 'nunique'
            }).round(2).fillna(0)

            city_stats.columns = ['total_deliveries', 'avg_delivery_time', 'unique_couriers']

            # ✅ ВИПРАВЛЕННЯ: Конвертуємо tuple індекси міст
            city_stats_dict = {}
            for (city_name, region_name), row in city_stats.iterrows():
                key = f"{city_name}_{region_name}".replace(' ', '_')
                city_stats_dict[key] = {
                    'city_name': city_name,
                    'region_name': region_name,
                    **row.to_dict()
                }

            # Загальна статистика
            general_stats = {
                'total_couriers': int(self.data['courier_id'].nunique()),
                'total_deliveries': int(len(self.data)),
                'avg_delivery_time': float(self.data['delivery_time_minutes'].mean()),
                'total_regions': int(self.data['region_name'].nunique()),
                'total_cities': int(self.data['city_name'].nunique()),
                'avg_improvement': float(self.data['improvement_minutes'].mean())
            }

            # Збираємо результати з конвертацією типів
            results = {
                'general_stats': self._convert_numpy_types(general_stats),
                'courier_performance': self._convert_numpy_types(courier_stats_dict),
                'top_couriers': self._convert_numpy_types(top_couriers_dict),
                'region_analysis': self._convert_numpy_types(region_stats.to_dict('index')),
                'city_analysis': self._convert_numpy_types(city_stats_dict),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо результати
            self._save_results(results, 'courier_performance_analysis')

            print("✅ Аналіз продуктивності кур'єрів завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі кур'єрів: {e}")
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