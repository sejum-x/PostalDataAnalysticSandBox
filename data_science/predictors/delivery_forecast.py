# data_science/predictors/delivery_forecast.py
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
from datetime import datetime, timedelta
import sys

sys.path.append('..')
from data_science.base_model import BaseMLModel
from utils.helpers import get_latest_csv_file


class DeliveryForecast(BaseMLModel):
    """Прогнозування кількості доставок на наступний місяць"""

    def __init__(self):
        super().__init__("delivery_forecast")
        self.model = RandomForestRegressor(n_estimators=100, random_state=42)

    def load_periodic_data(self):
        """Завантаження періодичних даних доставок"""
        print("📥 Завантаження періодичних даних...")

        delivery_file = get_latest_csv_file(self.config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
        if not delivery_file:
            raise FileNotFoundError("Файл delivery_periodic_raw_data не знайдено")

        data = pd.read_csv(delivery_file)
        print(f"✅ Завантажено {len(data)} записів періодичних доставок")

        return data

    def prepare_forecast_features(self, data):
        """Підготовка ознак для прогнозування"""
        print("🔧 Підготовка ознак для прогнозування...")

        # Створюємо додаткові ознаки на основі періодів
        data['period_duration'] = data['end_period_id'] - data['start_period_id'] + 1

        # Ознаки на основі типу відділення
        data['is_main_office'] = (data['department_type'] == 'Main Office').astype(int)
        data['is_local_branch'] = (data['department_type'] == 'Local Branch').astype(int)

        # Ознаки на основі розміру посилок - виправляємо обробку
        try:
            # Спробуємо витягти перше число з рядка розміру
            data['parcel_volume'] = data['parcel_max_size'].str.extract('(\d+)').astype(float)
        except:
            # Якщо не вдається, використовуємо вагу як проксі
            data['parcel_volume'] = data['parcel_max_weight']

        # Заповнюємо пропущені значення
        data['parcel_volume'] = data['parcel_volume'].fillna(data['parcel_max_weight'])

        # Історичні тренди по відділенню
        dept_history = data.groupby('department_id').agg({
            'deliveries_count': ['mean', 'std', 'sum'],
            'processing_time_hours': 'mean',
            'deliveries_share_percentage': 'mean'
        })

        dept_history.columns = [
            'dept_avg_deliveries', 'dept_std_deliveries', 'dept_total_deliveries',
            'dept_avg_processing_time', 'dept_avg_share'
        ]

        data = data.merge(dept_history, left_on='department_id', right_index=True, how='left')

        # Сезонні ознаки
        data['is_winter'] = data['start_month'].isin([12, 1, 2]).astype(int)
        data['is_spring'] = data['start_month'].isin([3, 4, 5]).astype(int)
        data['is_summer'] = data['start_month'].isin([6, 7, 8]).astype(int)
        data['is_autumn'] = data['start_month'].isin([9, 10, 11]).astype(int)

        # Тренди по регіонах
        region_stats = data.groupby('department_region').agg({
            'deliveries_count': 'mean',
            'processing_time_hours': 'mean'
        })
        region_stats.columns = ['region_avg_deliveries', 'region_avg_processing']

        data = data.merge(region_stats, left_on='department_region', right_index=True, how='left')

        # Заповнюємо пропущені значення
        numeric_columns = ['dept_std_deliveries', 'dept_avg_deliveries', 'dept_avg_processing_time',
                           'dept_avg_share', 'region_avg_deliveries', 'region_avg_processing']

        for col in numeric_columns:
            if col in data.columns:
                data[col] = data[col].fillna(data[col].mean())

        return data

    def train_forecast_model(self):
        """Навчання моделі прогнозування"""
        print("🎯 Навчання моделі прогнозування доставок...")

        data = self.load_periodic_data()
        data = self.prepare_forecast_features(data)

        # Вибираємо ознаки для моделі
        feature_columns = [
            'department_id', 'parcel_type_id', 'transport_body_type_id',
            'start_month', 'period_duration', 'parcel_max_weight',
            'parcel_volume', 'processing_time_hours',
            'is_main_office', 'is_local_branch',
            'is_winter', 'is_spring', 'is_summer', 'is_autumn',
            'dept_avg_deliveries', 'dept_avg_processing_time', 'dept_avg_share',
            'region_avg_deliveries', 'region_avg_processing'
        ]

        # Перевіряємо, які колонки існують
        existing_features = [col for col in feature_columns if col in data.columns]
        print(f"📊 Використовуємо {len(existing_features)} ознак з {len(feature_columns)} запланованих")

        # Підготовка даних
        X, y = self.prepare_data(data, 'deliveries_count', existing_features)

        # Навчання моделі
        metrics = self.train_model(X, y)

        # Збереження моделі
        model_path = self.save_model()

        return {
            'model_metrics': metrics,
            'model_path': model_path,
            'feature_importance': self.get_feature_importance(),
            'training_data_size': len(X)
        }

    def forecast_next_month(self):
        """Прогнозування доставок на наступний місяць"""
        print("🔮 Прогнозування доставок на наступний місяць...")

        data = self.load_periodic_data()
        data = self.prepare_forecast_features(data)

        # Визначаємо наступний місяць
        current_date = datetime.now()
        next_month = current_date.month + 1 if current_date.month < 12 else 1
        next_year = current_date.year if current_date.month < 12 else current_date.year + 1

        print(f"📅 Прогнозування на {next_month}/{next_year}")

        # Створюємо шаблон для прогнозування на основі останніх даних
        latest_data = data.groupby(['department_id', 'parcel_type_id', 'transport_body_type_id']).last().reset_index()

        # Оновлюємо дані для наступного місяця
        forecast_data = latest_data.copy()
        forecast_data['start_month'] = next_month
        forecast_data['start_year'] = next_year

        # Оновлюємо сезонні ознаки
        forecast_data['is_winter'] = (next_month in [12, 1, 2]) * 1
        forecast_data['is_spring'] = (next_month in [3, 4, 5]) * 1
        forecast_data['is_summer'] = (next_month in [6, 7, 8]) * 1
        forecast_data['is_autumn'] = (next_month in [9, 10, 11]) * 1

        # Використовуємо ті ж ознаки, що й при навчанні
        feature_columns = self.feature_names

        X_forecast = forecast_data[feature_columns].fillna(0)

        # Обробка категоріальних змінних
        for column in X_forecast.columns:
            if column in self.label_encoders:
                try:
                    X_forecast[column] = self.label_encoders[column].transform(X_forecast[column].astype(str))
                except ValueError:
                    # Якщо є нові категорії, використовуємо найчастіше значення
                    X_forecast[column] = 0

        predictions = self.predict(X_forecast)

        # Формуємо результати прогнозу
        forecast_results = []
        for i, pred in enumerate(predictions):
            if i < len(forecast_data):
                forecast_results.append({
                    'department_id': int(forecast_data.iloc[i]['department_id']),
                    'department_number': str(forecast_data.iloc[i].get('department_number',
                                                                       f'DEPT-{forecast_data.iloc[i]["department_id"]}')),
                    'department_city': str(forecast_data.iloc[i].get('department_city', 'Unknown')),
                    'department_region': str(forecast_data.iloc[i].get('department_region', 'Unknown')),
                    'parcel_type_name': str(forecast_data.iloc[i].get('parcel_type_name', 'Unknown')),
                    'transport_type_name': str(forecast_data.iloc[i].get('transport_type_name', 'Unknown')),
                    'predicted_deliveries': max(0, round(pred)),
                    'forecast_month': next_month,
                    'forecast_year': next_year,
                    'confidence': 'medium' if pred > 0 else 'low'
                })

        # Агрегуємо прогнози
        total_predicted = sum(result['predicted_deliveries'] for result in forecast_results)

        # Прогнози по регіонах
        region_forecasts = {}
        for result in forecast_results:
            region = result['department_region']
            if region not in region_forecasts:
                region_forecasts[region] = 0
            region_forecasts[region] += result['predicted_deliveries']

        # Прогнози по типах посилок
        parcel_type_forecasts = {}
        for result in forecast_results:
            parcel_type = result['parcel_type_name']
            if parcel_type not in parcel_type_forecasts:
                parcel_type_forecasts[parcel_type] = 0
            parcel_type_forecasts[parcel_type] += result['predicted_deliveries']

        summary = {
            'forecast_period': f"{next_month}/{next_year}",
            'total_predicted_deliveries': total_predicted,
            'total_departments': len(forecast_results),
            'region_forecasts': region_forecasts,
            'parcel_type_forecasts': parcel_type_forecasts,
            'top_departments': sorted(forecast_results,
                                      key=lambda x: x['predicted_deliveries'],
                                      reverse=True)[:10]
        }

        # Збереження прогнозів
        self.save_predictions({
            'summary': summary,
            'detailed_forecasts': forecast_results
        }, f"next_month_{next_month}_{next_year}")

        return {
            'summary': summary,
            'detailed_forecasts': forecast_results
        }

    def get_feature_importance(self):
        """Отримання важливості ознак"""
        if hasattr(self.model, 'feature_importances_') and self.feature_names:
            importance_df = pd.DataFrame({
                'feature': self.feature_names,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)

            return importance_df.to_dict('records')
        else:
            return None