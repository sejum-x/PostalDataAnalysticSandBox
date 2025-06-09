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

            # Конвертуємо MultiIndex в словник
            courier_stats_dict = self._convert_multiindex_to_dict(courier_stats)

            # Топ кур'єри
            top_couriers = courier_stats.nlargest(10, 'efficiency_score')
            top_couriers_dict = self._convert_multiindex_to_dict(top_couriers)

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
            city_stats_dict = self._convert_multiindex_to_dict(city_stats)

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
                'analysis_type': 'courier_performance_analysis',
                'general_stats': self._convert_numpy_types(general_stats),
                'courier_performance': self._convert_numpy_types(courier_stats_dict),
                'top_couriers': self._convert_numpy_types(top_couriers_dict),
                'region_analysis': self._convert_numpy_types(region_stats.to_dict('index')),
                'city_analysis': self._convert_numpy_types(city_stats_dict),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо результати в окремий файл
            self._save_results(results, 'courier_performance_analysis')

            print("✅ Аналіз продуктивності кур'єрів завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі кур'єрів: {e}")
            return {'error': str(e)}

    def _convert_multiindex_to_dict(self, df):
        """Конвертує MultiIndex DataFrame в словник"""
        result = {}
        for idx, row in df.iterrows():
            if isinstance(idx, tuple):
                key = "_".join([str(i).replace(' ', '_').replace('/', '_') for i in idx])
            else:
                key = str(idx).replace(' ', '_').replace('/', '_')

            # Додаємо інформацію про індекси
            row_dict = row.to_dict()
            if isinstance(idx, tuple):
                index_names = df.index.names if hasattr(df.index, 'names') else []
                for i, index_name in enumerate(index_names):
                    if index_name and i < len(idx):
                        row_dict[index_name] = idx[i]

            result[key] = self._convert_numpy_types(row_dict)

        return result

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
        """Зберігає результати аналізу в окремі файли по категоріях"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Створюємо директорію якщо не існує
            os.makedirs(self.config.PROCESSED_DATA_PATH, exist_ok=True)

            saved_files = []

            # 1. Загальна статистика
            general_stats_file = f"courier_general_stats_{timestamp}.json"
            general_stats_path = os.path.join(self.config.PROCESSED_DATA_PATH, general_stats_file)
            with open(general_stats_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'courier_general_stats',
                    'data': results['general_stats'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(general_stats_file)

            # 2. Продуктивність кур'єрів
            courier_performance_file = f"courier_performance_{timestamp}.json"
            courier_performance_path = os.path.join(self.config.PROCESSED_DATA_PATH, courier_performance_file)
            with open(courier_performance_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'courier_performance',
                    'data': results['courier_performance'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(courier_performance_file)

            # 3. Топ кур'єри
            top_couriers_file = f"courier_top_performers_{timestamp}.json"
            top_couriers_path = os.path.join(self.config.PROCESSED_DATA_PATH, top_couriers_file)
            with open(top_couriers_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'courier_top_performers',
                    'data': results['top_couriers'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(top_couriers_file)

            # 4. Аналіз по регіонах
            region_analysis_file = f"courier_region_analysis_{timestamp}.json"
            region_analysis_path = os.path.join(self.config.PROCESSED_DATA_PATH, region_analysis_file)
            with open(region_analysis_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'courier_region_analysis',
                    'data': results['region_analysis'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(region_analysis_file)

            # 5. Аналіз по містах
            city_analysis_file = f"courier_city_analysis_{timestamp}.json"
            city_analysis_path = os.path.join(self.config.PROCESSED_DATA_PATH, city_analysis_file)
            with open(city_analysis_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'courier_city_analysis',
                    'data': results['city_analysis'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(city_analysis_file)

            print(f"💾 Збережено {len(saved_files)} файлів: {', '.join(saved_files)}")
            return saved_files

        except Exception as e:
            print(f"❌ Помилка збереження результатів: {e}")
            return None