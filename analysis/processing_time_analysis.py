"""
Аналіз часу обробки посилок в розрізі періодів (Завдання 3)
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

    def analyze_processing_times_by_periods(self, filepath=None):
        """
        Завдання 3: Аналіз часу обробки посилок в розрізі періодів
        """
        if filepath and not self.load_data(filepath):
            return {'error': 'Не вдалося завантажити дані'}

        if self.data is None or self.data.empty:
            return {'error': 'Немає даних для аналізу'}

        try:
            print("🔄 Аналіз часу обробки посилок по періодах...")

            # Створюємо колонку періоду
            self.data['period'] = self.data['start_year'].astype(str) + '-' + \
                                 self.data['start_month'].astype(str).str.zfill(2)

            # Конвертуємо числові колонки
            numeric_columns = ['processing_time_hours', 'deliveries_count', 'parcel_max_size', 'parcel_max_weight']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
            self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

            # 📊 АНАЛІЗ ЧАСУ ОБРОБКИ ПО ПЕРІОДАХ І ТИПАХ ПОСИЛОК
            period_parcel_processing = self.data.groupby([
                'period', 'parcel_type_name', 'parcel_max_size', 'parcel_max_weight'
            ]).agg({
                'processing_time_hours': ['mean', 'median', 'std', 'min', 'max'],
                'deliveries_count': 'sum',
                'department_id': 'nunique',
                'delivery_id': 'count'
            }).round(2)

            # Сплющуємо колонки
            period_parcel_processing.columns = [
                'avg_processing_time', 'median_processing_time', 'std_processing_time',
                'min_processing_time', 'max_processing_time',
                'total_deliveries', 'departments_handling', 'total_records'
            ]

            period_parcel_processing = period_parcel_processing.fillna(0)

            # Індекс складності обробки по періодах
            period_parcel_processing['period_complexity_score'] = (
                (period_parcel_processing['avg_processing_time'] * 0.5) +
                (period_parcel_processing['std_processing_time'] * 0.3) +
                (period_parcel_processing['max_processing_time'] * 0.2)
            ).round(2)

            # 📈 ТРЕНДИ ЧАСУ ОБРОБКИ ПО ТИПАХ ПОСИЛОК
            processing_trends = self.data.groupby(['parcel_type_name', 'period']).agg({
                'processing_time_hours': 'mean',
                'deliveries_count': 'sum',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            processing_trends.columns = ['avg_processing_time', 'total_deliveries', 'departments_handling']

            # 📊 ПОРІВНЯННЯ ПЕРІОДІВ ПО ЧАСУ ОБРОБКИ
            period_comparison = self.data.groupby('period').agg({
                'processing_time_hours': ['mean', 'median', 'std', 'min', 'max'],
                'deliveries_count': 'sum',
                'parcel_type_id': 'nunique',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            period_comparison.columns = [
                'avg_processing_time', 'median_processing_time', 'std_processing_time',
                'min_processing_time', 'max_processing_time',
                'total_deliveries', 'parcel_types', 'departments'
            ]

            # 🏆 НАЙСКЛАДНІШІ ПОСИЛКИ ПО ПЕРІОДАХ
            complex_parcels_by_period = {}
            for period in self.data['period'].unique():
                period_data = period_parcel_processing.loc[
                    period_parcel_processing.index.get_level_values(0) == period
                ]
                if not period_data.empty:
                    top_complex = period_data.nlargest(5, 'period_complexity_score')
                    complex_parcels_by_period[period] = self._convert_multiindex_to_dict(top_complex)

            # 📊 АНАЛІЗ ЕФЕКТИВНОСТІ ВІДДІЛЕНЬ ПО ПЕРІОДАХ
            dept_efficiency_by_period = self.data.groupby(['period', 'department_id', 'department_number']).agg({
                'processing_time_hours': 'mean',
                'deliveries_count': 'sum',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            dept_efficiency_by_period.columns = [
                'avg_processing_time', 'total_deliveries', 'parcel_types_handled'
            ]

            # Рахуємо ефективність (безпечне ділення)
            dept_efficiency_by_period['efficiency_ratio'] = (
                dept_efficiency_by_period['total_deliveries'] /
                (dept_efficiency_by_period['avg_processing_time'] + 0.1)
            ).round(2)

            # 🏆 НАЙЕФЕКТИВНІШІ ВІДДІЛЕННЯ ПО ПЕРІОДАХ
            efficient_departments_by_period = {}
            for period in self.data['period'].unique():
                period_data = dept_efficiency_by_period.loc[
                    dept_efficiency_by_period.index.get_level_values(0) == period
                ]
                if not period_data.empty:
                    top_efficient = period_data.nlargest(5, 'efficiency_ratio')
                    efficient_departments_by_period[period] = self._convert_multiindex_to_dict(top_efficient)

            # 📊 АНАЛІЗ ПО РЕГІОНАХ І ПЕРІОДАХ
            region_processing_by_period = self.data.groupby(['period', 'department_region']).agg({
                'processing_time_hours': ['mean', 'median', 'std'],
                'deliveries_count': 'sum',
                'department_id': 'nunique',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            region_processing_by_period.columns = [
                'avg_processing_time', 'median_processing_time', 'std_processing_time',
                'total_deliveries', 'departments_count', 'parcel_types_count'
            ]

            # 📈 ДИНАМІКА ЗМІН ПО ПЕРІОДАХ
            period_changes = {}
            periods = sorted(self.data['period'].unique())
            for i in range(1, len(periods)):
                prev_period = periods[i-1]
                curr_period = periods[i]

                prev_data = period_comparison.loc[prev_period] if prev_period in period_comparison.index else None
                curr_data = period_comparison.loc[curr_period] if curr_period in period_comparison.index else None

                if prev_data is not None and curr_data is not None:
                    changes = {
                        'previous_period': prev_period,
                        'current_period': curr_period,
                        'avg_processing_time_change': float(curr_data['avg_processing_time'] - prev_data['avg_processing_time']),
                        'deliveries_change': int(curr_data['total_deliveries'] - prev_data['total_deliveries']),
                        'departments_change': int(curr_data['departments'] - prev_data['departments']),
                        'improvement_percentage': float(
                            ((prev_data['avg_processing_time'] - curr_data['avg_processing_time']) /
                             (prev_data['avg_processing_time'] + 0.1)) * 100
                        )
                    }
                    period_changes[f"{prev_period}_to_{curr_period}"] = changes

            # Загальна статистика
            general_stats = {
                'total_periods': int(self.data['period'].nunique()),
                'total_records': int(len(self.data)),
                'avg_processing_time': float(self.data['processing_time_hours'].mean()),
                'median_processing_time': float(self.data['processing_time_hours'].median()),
                'min_processing_time': float(self.data['processing_time_hours'].min()),
                'max_processing_time': float(self.data['processing_time_hours'].max()),
                'total_parcel_types': int(self.data['parcel_type_name'].nunique()),
                'total_departments': int(self.data['department_id'].nunique()),
                'total_deliveries': int(self.data['deliveries_count'].sum()),
                'total_regions': int(self.data['department_region'].nunique())
            }

            # Збираємо результати з конвертацією типів
            results = {
                'analysis_type': 'processing_time_by_periods',
                'general_stats': self._convert_numpy_types(general_stats),
                'period_parcel_processing': self._convert_multiindex_to_dict(period_parcel_processing),
                'processing_trends': self._convert_multiindex_to_dict(processing_trends),
                'period_comparison': self._convert_numpy_types(period_comparison.to_dict('index')),
                'complex_parcels_by_period': self._convert_numpy_types(complex_parcels_by_period),
                'department_efficiency_by_period': self._convert_multiindex_to_dict(dept_efficiency_by_period),
                'efficient_departments_by_period': self._convert_numpy_types(efficient_departments_by_period),
                'region_processing_by_period': self._convert_multiindex_to_dict(region_processing_by_period),
                'period_changes': self._convert_numpy_types(period_changes),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо в окремий файл
            self._save_results(results, 'processing_time_by_periods')

            print("✅ Аналіз часу обробки по періодах завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі часу обробки по періодах: {e}")
            return {'error': str(e)}

    def _convert_multiindex_to_dict(self, df):
        """Конвертує MultiIndex DataFrame в словник"""
        result = {}
        for idx, row in df.iterrows():
            if isinstance(idx, tuple):
                key = "_".join([str(i).replace(' ', '_').replace('/', '_').replace('*', 'x') for i in idx])
            else:
                key = str(idx).replace(' ', '_').replace('/', '_').replace('*', 'x')

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
        """Зберігає результати аналізу в окремі файли"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{filename_prefix}_{timestamp}.json"
            filepath = os.path.join(self.config.PROCESSED_DATA_PATH, filename)

            # Створюємо директорію якщо не існує
            os.makedirs(self.config.PROCESSED_DATA_PATH, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

            print(f"💾 Результати збережено: {filename}")
            return filepath

        except Exception as e:
            print(f"❌ Помилка збереження результатів: {e}")
            return None