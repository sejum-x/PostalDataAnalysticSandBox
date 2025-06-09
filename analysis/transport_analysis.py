"""
Аналіз використання транспорту в розрізі періодів (Завдання 4)
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

    def analyze_transport_utilization_by_periods(self, filepath=None):
        """
        Завдання 4: Аналіз використання транспорту в розрізі періодів
        """
        if filepath and not self.load_data(filepath):
            return {'error': 'Не вдалося завантажити дані'}

        if self.data is None or self.data.empty:
            return {'error': 'Немає даних для аналізу'}

        try:
            print("🔄 Аналіз використання транспорту по періодах...")

            # Створюємо колонку періоду
            self.data['period'] = self.data['start_year'].astype(str) + '-' + \
                                 self.data['start_month'].astype(str).str.zfill(2)

            # Конвертуємо числові колонки
            numeric_columns = ['deliveries_count', 'processing_time_hours', 'deliveries_share_percentage',
                              'parcel_max_weight', 'parcel_max_size']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
            self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

            # 📊 ВИКОРИСТАННЯ ТРАНСПОРТУ ПО ПЕРІОДАХ
            period_transport_usage = self.data.groupby([
                'period', 'transport_body_type_id', 'transport_type_name'
            ]).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'deliveries_share_percentage': 'mean',
                'department_id': 'nunique',
                'parcel_type_id': 'nunique',
                'delivery_id': 'count'
            }).round(2).fillna(0)

            period_transport_usage.columns = [
                'total_deliveries', 'avg_processing_time', 'avg_share_percentage',
                'departments_served', 'parcel_types_handled', 'total_records'
            ]

            # Рейтинг використання по періодах
            period_transport_usage['period_utilization_score'] = (
                (period_transport_usage['total_deliveries'] * 0.4) +
                (period_transport_usage['departments_served'] * 0.3) +
                (period_transport_usage['avg_share_percentage'] * 0.3)
            ).round(2)

            # 📈 ТРЕНДИ ВИКОРИСТАННЯ ТРАНСПОРТУ
            transport_trends = self.data.groupby(['transport_type_name', 'period']).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'deliveries_share_percentage': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            transport_trends.columns = [
                'total_deliveries', 'avg_processing_time',
                'avg_share_percentage', 'departments_served'
            ]

            # 📊 ЕФЕКТИВНІСТЬ ТРАНСПОРТУ ПО ПЕРІОДАХ
            transport_efficiency_by_period = self.data.groupby([
                'period', 'transport_type_name'
            ]).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'deliveries_share_percentage': 'mean'
            }).round(2).fillna(0)

            transport_efficiency_by_period.columns = [
                'total_deliveries', 'avg_processing_time', 'avg_share_percentage'
            ]

            # Безпечне ділення для ефективності
            transport_efficiency_by_period['efficiency_ratio'] = (
                transport_efficiency_by_period['total_deliveries'] /
                (transport_efficiency_by_period['avg_processing_time'] + 0.1)
            ).round(2)

            # 🏆 НАЙЕФЕКТИВНІШИЙ ТРАНСПОРТ ПО ПЕРІОДАХ
            efficient_transport_by_period = {}
            for period in self.data['period'].unique():
                period_data = transport_efficiency_by_period.loc[
                    transport_efficiency_by_period.index.get_level_values(0) == period
                ]
                if not period_data.empty:
                    top_efficient = period_data.nlargest(3, 'efficiency_ratio')
                    efficient_transport_by_period[period] = self._convert_multiindex_to_dict(top_efficient)

            # 📊 АНАЛІЗ ТРАНСПОРТУ ПО ТИПАХ ПОСИЛОК І ПЕРІОДАХ
            transport_parcel_period_analysis = self.data.groupby([
                'period', 'transport_type_name', 'parcel_type_name'
            ]).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'parcel_max_weight': 'mean',
                'parcel_max_size': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            transport_parcel_period_analysis.columns = [
                'total_deliveries', 'avg_processing_time',
                'avg_parcel_weight', 'avg_parcel_size', 'departments_served'
            ]

            # 📊 АНАЛІЗ ТРАНСПОРТУ ПО РЕГІОНАХ І ПЕРІОДАХ
            transport_region_period_analysis = self.data.groupby([
                'period', 'transport_type_name', 'department_region'
            ]).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            transport_region_period_analysis.columns = [
                'total_deliveries', 'avg_processing_time',
                'departments_count', 'parcel_types_handled'
            ]

            # 📊 ЗАВАНТАЖЕНІСТЬ ТРАНСПОРТУ ПО ВІДДІЛЕННЯМ І ПЕРІОДАХ
            dept_transport_period_analysis = self.data.groupby([
                'period', 'department_id', 'department_number'
            ]).agg({
                'transport_body_type_id': 'nunique',
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            dept_transport_period_analysis.columns = [
                'transport_types_used', 'total_deliveries',
                'avg_processing_time', 'parcel_types_handled'
            ]

            # 📈 ДИНАМІКА ЗМІН ВИКОРИСТАННЯ ТРАНСПОРТУ
            transport_changes = {}
            periods = sorted(self.data['period'].unique())

            # Загальні зміни по періодах
            period_transport_summary = self.data.groupby('period').agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'transport_body_type_id': 'nunique',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            for i in range(1, len(periods)):
                prev_period = periods[i-1]
                curr_period = periods[i]

                prev_data = period_transport_summary.loc[prev_period] if prev_period in period_transport_summary.index else None
                curr_data = period_transport_summary.loc[curr_period] if curr_period in period_transport_summary.index else None

                if prev_data is not None and curr_data is not None:
                    changes = {
                        'previous_period': prev_period,
                        'current_period': curr_period,
                        'deliveries_change': int(curr_data['deliveries_count'] - prev_data['deliveries_count']),
                        'processing_time_change': float(curr_data['processing_time_hours'] - prev_data['processing_time_hours']),
                        'transport_types_change': int(curr_data['transport_body_type_id'] - prev_data['transport_body_type_id']),
                        'departments_change': int(curr_data['department_id'] - prev_data['department_id'])
                    }
                    transport_changes[f"{prev_period}_to_{curr_period}"] = changes

            # 🏆 НАЙБІЛЬШ ВИКОРИСТОВУВАНІ ТИПИ ТРАНСПОРТУ ПО ПЕРІОДАХ
            most_used_transport_by_period = {}
            for period in self.data['period'].unique():
                period_data = period_transport_usage.loc[
                    period_transport_usage.index.get_level_values(0) == period
                ]
                if not period_data.empty:
                    top_used = period_data.nlargest(5, 'period_utilization_score')
                    most_used_transport_by_period[period] = self._convert_multiindex_to_dict(top_used)

            # Загальна статистика
            general_stats = {
                'total_periods': int(self.data['period'].nunique()),
                'total_transport_types': int(self.data['transport_type_name'].nunique()),
                'total_records': int(len(self.data)),
                'total_deliveries': int(self.data['deliveries_count'].sum()),
                'avg_processing_time': float(self.data['processing_time_hours'].mean()),
                'departments_using_transport': int(self.data['department_id'].nunique()),
                'parcel_types_transported': int(self.data['parcel_type_name'].nunique()),
                'regions_served': int(self.data['department_region'].nunique()),
                'avg_share_percentage': float(self.data['deliveries_share_percentage'].mean())
            }

            # Збираємо результати з конвертацією типів
            results = {
                'analysis_type': 'transport_utilization_by_periods',
                'general_stats': self._convert_numpy_types(general_stats),
                'period_transport_usage': self._convert_multiindex_to_dict(period_transport_usage),
                'transport_trends': self._convert_multiindex_to_dict(transport_trends),
                'transport_efficiency_by_period': self._convert_multiindex_to_dict(transport_efficiency_by_period),
                'efficient_transport_by_period': self._convert_numpy_types(efficient_transport_by_period),
                'transport_parcel_period_analysis': self._convert_multiindex_to_dict(transport_parcel_period_analysis),
                'transport_region_period_analysis': self._convert_multiindex_to_dict(transport_region_period_analysis),
                'department_transport_period_analysis': self._convert_multiindex_to_dict(dept_transport_period_analysis),
                'most_used_transport_by_period': self._convert_numpy_types(most_used_transport_by_period),
                'transport_changes': self._convert_numpy_types(transport_changes),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо в окремий файл
            self._save_results(results, 'transport_utilization_by_periods')

            print("✅ Аналіз використання транспорту по періодах завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі транспорту по періодах: {e}")
            return {'error': str(e)}

    def _convert_multiindex_to_dict(self, df):
        """Конвертує MultiIndex DataFrame в словник"""
        result = {}
        for idx, row in df.iterrows():
            if isinstance(idx, tuple):
                key = "_".join([str(i).replace(' ', '_').replace('/', '_').replace('-', '_') for i in idx])
            else:
                key = str(idx).replace(' ', '_').replace('/', '_').replace('-', '_')

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