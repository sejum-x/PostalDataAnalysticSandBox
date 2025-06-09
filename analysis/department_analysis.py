"""
Аналіз завантажень відділень в розрізі періодів (Завдання 2)
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

    def analyze_department_workload_by_periods(self, filepath=None):
        """
        Завдання 2: Аналіз завантажень відділень в розрізі періодів
        """
        if filepath and not self.load_data(filepath):
            return {'error': 'Не вдалося завантажити дані'}

        if self.data is None or self.data.empty:
            return {'error': 'Немає даних для аналізу'}

        try:
            print("🔄 Аналіз завантажень відділень по періодах...")

            # Створюємо колонку періоду
            self.data['period'] = self.data['start_year'].astype(str) + '-' + \
                                 self.data['start_month'].astype(str).str.zfill(2)

            # ✅ ВИПРАВЛЕНО: Правильне створення дат з існуючими колонками
            try:
                # Заповнюємо NaN значення
                date_columns = ['start_year', 'start_month', 'start_day', 'end_year', 'end_month', 'end_day']
                for col in date_columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce').fillna(1)

                # Створюємо DataFrame з датами для pd.to_datetime
                start_dates_df = self.data[['start_year', 'start_month', 'start_day']].copy()
                start_dates_df.columns = ['year', 'month', 'day']

                end_dates_df = self.data[['end_year', 'end_month', 'end_day']].copy()
                end_dates_df.columns = ['year', 'month', 'day']

                # Конвертуємо в дати
                start_dates = pd.to_datetime(start_dates_df)
                end_dates = pd.to_datetime(end_dates_df)

                # Рахуємо тривалість періоду
                self.data['period_duration_days'] = (end_dates - start_dates).dt.days + 1

                print(f"✅ Успішно створено дати. Середня тривалість періоду: {self.data['period_duration_days'].mean():.1f} днів")

            except Exception as date_error:
                print(f"⚠️ Помилка створення дат: {date_error}")
                print("Використовуємо фіксовану тривалість періоду")
                self.data['period_duration_days'] = 30

            # Конвертуємо числові колонки
            numeric_columns = ['deliveries_count', 'processing_time_hours', 'deliveries_share_percentage']
            for col in numeric_columns:
                if col in self.data.columns:
                    self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
            self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

            # 📊 АНАЛІЗ ПО ПЕРІОДАХ І ВІДДІЛЕННЯМ
            period_dept_analysis = self.data.groupby([
                'period', 'department_id', 'department_number', 'department_type'
            ]).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'deliveries_share_percentage': 'mean',
                'parcel_type_id': 'nunique',
                'period_duration_days': 'first'
            }).round(2).fillna(0)

            period_dept_analysis.columns = [
                'total_deliveries', 'avg_processing_time', 'avg_share_percentage',
                'parcel_types_handled', 'period_days'
            ]

            # Рахуємо доставки на день
            period_dept_analysis['deliveries_per_day'] = (
                period_dept_analysis['total_deliveries'] /
                period_dept_analysis['period_days'].replace(0, 1)
            ).round(2)

            # Індекс завантаженості по періодах
            period_dept_analysis['period_workload_score'] = (
                (period_dept_analysis['deliveries_per_day'] * 0.4) +
                (period_dept_analysis['avg_processing_time'] * 0.3) +
                (period_dept_analysis['avg_share_percentage'] * 0.3)
            ).round(2)

            # 📈 ТРЕНДИ ПО ВІДДІЛЕННЯМ
            dept_trends = self.data.groupby(['department_id', 'department_number', 'period']).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'deliveries_share_percentage': 'mean'
            }).round(2).fillna(0)

            dept_trends.columns = ['total_deliveries', 'avg_processing_time', 'avg_share_percentage']

            # 📊 ЗАГАЛЬНА СТАТИСТИКА ПО ПЕРІОДАХ
            period_summary = self.data.groupby('period').agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique',
                'parcel_type_id': 'nunique',
                'transport_body_type_id': 'nunique'
            }).round(2).fillna(0)

            period_summary.columns = [
                'total_deliveries', 'avg_processing_time',
                'active_departments', 'parcel_types', 'transport_types'
            ]

            # 🏆 ТОП ЗАВАНТАЖЕНІ ВІДДІЛЕННЯ ПО ПЕРІОДАХ
            top_busy_by_period = {}
            for period in self.data['period'].unique():
                period_data = period_dept_analysis.loc[
                    period_dept_analysis.index.get_level_values(0) == period
                ]
                if not period_data.empty:
                    top_5 = period_data.nlargest(5, 'period_workload_score')
                    top_busy_by_period[period] = self._convert_multiindex_to_dict(top_5)

            # 📊 АНАЛІЗ ПО ТИПАХ ВІДДІЛЕНЬ І ПЕРІОДАХ
            dept_type_period_analysis = self.data.groupby(['period', 'department_type']).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            dept_type_period_analysis.columns = [
                'total_deliveries', 'avg_processing_time', 'departments_count'
            ]

            # 📊 АНАЛІЗ ПО РЕГІОНАХ І ПЕРІОДАХ
            region_period_analysis = self.data.groupby(['period', 'department_region']).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique',
                'parcel_type_id': 'nunique'
            }).round(2).fillna(0)

            region_period_analysis.columns = [
                'total_deliveries', 'avg_processing_time',
                'departments_count', 'parcel_types'
            ]

            # 📈 ПОРІВНЯННЯ ПЕРІОДІВ
            period_comparison = {}
            periods = sorted(self.data['period'].unique())
            for i in range(1, len(periods)):
                prev_period = periods[i-1]
                curr_period = periods[i]

                prev_data = period_summary.loc[prev_period] if prev_period in period_summary.index else None
                curr_data = period_summary.loc[curr_period] if curr_period in period_summary.index else None

                if prev_data is not None and curr_data is not None:
                    comparison = {
                        'previous_period': prev_period,
                        'current_period': curr_period,
                        'deliveries_change': float(curr_data['total_deliveries'] - prev_data['total_deliveries']),
                        'processing_time_change': float(curr_data['avg_processing_time'] - prev_data['avg_processing_time']),
                        'departments_change': int(curr_data['active_departments'] - prev_data['active_departments'])
                    }
                    period_comparison[f"{prev_period}_to_{curr_period}"] = comparison

            # 📊 АНАЛІЗ ПО МІСТАХ І ПЕРІОДАХ
            city_period_analysis = self.data.groupby(['period', 'department_city', 'department_region']).agg({
                'deliveries_count': 'sum',
                'processing_time_hours': 'mean',
                'department_id': 'nunique'
            }).round(2).fillna(0)

            city_period_analysis.columns = [
                'total_deliveries', 'avg_processing_time', 'departments_count'
            ]

            # Загальна статистика
            general_stats = {
                'total_periods': int(self.data['period'].nunique()),
                'total_departments': int(self.data['department_id'].nunique()),
                'total_records': int(len(self.data)),
                'total_deliveries': int(self.data['deliveries_count'].sum()),
                'avg_processing_time': float(self.data['processing_time_hours'].mean()),
                'total_regions': int(self.data['department_region'].nunique()),
                'total_cities': int(self.data['department_city'].nunique()),
                'parcel_types': int(self.data['parcel_type_name'].nunique()),
                'transport_types': int(self.data['transport_type_name'].nunique()),
                'department_types': int(self.data['department_type'].nunique()),
                'avg_period_duration': float(self.data['period_duration_days'].mean())
            }

            # Збираємо результати з конвертацією типів
            results = {
                'analysis_type': 'department_workload_by_periods',
                'general_stats': self._convert_numpy_types(general_stats),
                'period_department_analysis': self._convert_multiindex_to_dict(period_dept_analysis),
                'period_summary': self._convert_numpy_types(period_summary.to_dict('index')),
                'department_trends': self._convert_multiindex_to_dict(dept_trends),
                'top_busy_departments_by_period': self._convert_numpy_types(top_busy_by_period),
                'department_type_period_analysis': self._convert_multiindex_to_dict(dept_type_period_analysis),
                'region_period_analysis': self._convert_multiindex_to_dict(region_period_analysis),
                'city_period_analysis': self._convert_multiindex_to_dict(city_period_analysis),
                'period_comparison': self._convert_numpy_types(period_comparison),
                'analysis_timestamp': datetime.now().isoformat()
            }

            # Зберігаємо в окремий файл
            self._save_results(results, 'department_workload_by_periods')

            print("✅ Аналіз завантажень відділень по періодах завершено!")
            return results

        except Exception as e:
            print(f"❌ Помилка при аналізі відділень по періодах: {e}")
            import traceback
            traceback.print_exc()
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
        """Зберігає результати аналізу в окремі файли по категоріях"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Створюємо директорію якщо не існує
            os.makedirs(self.config.PROCESSED_DATA_PATH, exist_ok=True)

            saved_files = []

            # 1. Загальна статистика
            general_stats_file = f"department_general_stats_{timestamp}.json"
            general_stats_path = os.path.join(self.config.PROCESSED_DATA_PATH, general_stats_file)
            with open(general_stats_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_general_stats',
                    'data': results['general_stats'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(general_stats_file)

            # 2. Аналіз відділень по періодах
            period_dept_file = f"department_period_analysis_{timestamp}.json"
            period_dept_path = os.path.join(self.config.PROCESSED_DATA_PATH, period_dept_file)
            with open(period_dept_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_period_analysis',
                    'data': results['period_department_analysis'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(period_dept_file)

            # 3. Підсумки по періодах
            period_summary_file = f"department_period_summary_{timestamp}.json"
            period_summary_path = os.path.join(self.config.PROCESSED_DATA_PATH, period_summary_file)
            with open(period_summary_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_period_summary',
                    'data': results['period_summary'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(period_summary_file)

            # 4. Тренди відділень
            dept_trends_file = f"department_trends_{timestamp}.json"
            dept_trends_path = os.path.join(self.config.PROCESSED_DATA_PATH, dept_trends_file)
            with open(dept_trends_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_trends',
                    'data': results['department_trends'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(dept_trends_file)

            # 5. Топ завантажені відділення
            top_busy_file = f"department_top_busy_{timestamp}.json"
            top_busy_path = os.path.join(self.config.PROCESSED_DATA_PATH, top_busy_file)
            with open(top_busy_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_top_busy',
                    'data': results['top_busy_departments_by_period'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(top_busy_file)

            # 6. Аналіз типів відділень
            dept_type_file = f"department_type_analysis_{timestamp}.json"
            dept_type_path = os.path.join(self.config.PROCESSED_DATA_PATH, dept_type_file)
            with open(dept_type_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_type_analysis',
                    'data': results['department_type_period_analysis'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(dept_type_file)

            # 7. Аналіз по регіонах
            region_analysis_file = f"department_region_analysis_{timestamp}.json"
            region_analysis_path = os.path.join(self.config.PROCESSED_DATA_PATH, region_analysis_file)
            with open(region_analysis_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_region_analysis',
                    'data': results['region_period_analysis'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(region_analysis_file)

            # 8. Аналіз по містах
            city_analysis_file = f"department_city_analysis_{timestamp}.json"
            city_analysis_path = os.path.join(self.config.PROCESSED_DATA_PATH, city_analysis_file)
            with open(city_analysis_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_city_analysis',
                    'data': results['city_period_analysis'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(city_analysis_file)

            # 9. Порівняння періодів
            period_comparison_file = f"department_period_comparison_{timestamp}.json"
            period_comparison_path = os.path.join(self.config.PROCESSED_DATA_PATH, period_comparison_file)
            with open(period_comparison_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'analysis_type': 'department_period_comparison',
                    'data': results['period_comparison'],
                    'analysis_timestamp': results['analysis_timestamp']
                }, f, ensure_ascii=False, indent=2)
            saved_files.append(period_comparison_file)

            print(f"💾 Збережено {len(saved_files)} файлів: {', '.join(saved_files)}")
            return saved_files

        except Exception as e:
            print(f"❌ Помилка збереження результатів: {e}")
            return None