# data_science/ds_controller.py
import pandas as pd
import numpy as np
from datetime import datetime
import sys

sys.path.append('..')

from data_science.predictors.delivery_forecast import DeliveryForecast
from data_science.analyzers.efficiency_analyzer import EfficiencyAnalyzer
from utils.helpers import get_latest_csv_file
from config.database_config import DatabaseConfig


class DataScienceController:
    """Головний контролер для всіх Data Science операцій"""

    def __init__(self):
        self.config = DatabaseConfig()
        self.delivery_forecast = DeliveryForecast()
        self.efficiency_analyzer = EfficiencyAnalyzer()

    def run_full_analysis(self):
        """Запуск повного Data Science аналізу"""
        print("🧠 Запуск повного Data Science аналізу...")

        results = {
            'analysis_timestamp': datetime.now().isoformat(),
            'components': {}
        }

        try:
            # 1. Навчання моделі прогнозування
            print("\n1️⃣ Навчання моделі прогнозування доставок...")
            forecast_training = self.delivery_forecast.train_forecast_model()
            results['components']['forecast_model_training'] = forecast_training

        except Exception as e:
            print(f"❌ Помилка в навчанні моделі: {e}")
            results['components']['forecast_model_training'] = {'error': str(e)}

        try:
            # 2. Прогнозування на наступний місяць
            print("\n2️⃣ Прогнозування доставок на наступний місяць...")
            next_month_forecast = self.delivery_forecast.forecast_next_month()
            results['components']['next_month_forecast'] = next_month_forecast

        except Exception as e:
            print(f"❌ Помилка в прогнозуванні: {e}")
            results['components']['next_month_forecast'] = {'error': str(e)}

        try:
            # 3. Аналіз ефективності відділень
            print("\n3️⃣ Аналіз ефективності відділень...")
            dept_performance = self.efficiency_analyzer.analyze_department_performance()
            results['components']['department_performance'] = dept_performance.to_dict('records')

        except Exception as e:
            print(f"❌ Помилка в аналізі відділень: {e}")
            results['components']['department_performance'] = {'error': str(e)}

        try:
            # 4. Аналіз ефективності транспорту
            print("\n4️⃣ Аналіз ефективності транспорту...")
            transport_efficiency = self.efficiency_analyzer.analyze_transport_efficiency()
            results['components']['transport_efficiency'] = transport_efficiency.to_dict('records')

        except Exception as e:
            print(f"❌ Помилка в аналізі транспорту: {e}")
            results['components']['transport_efficiency'] = {'error': str(e)}

        try:
            # 5. Аналіз сезонних патернів
            print("\n5️⃣ Аналіз сезонних патернів...")
            seasonal_patterns = self.efficiency_analyzer.analyze_seasonal_patterns()
            results['components']['seasonal_patterns'] = seasonal_patterns

        except Exception as e:
            print(f"❌ Помилка в сезонному аналізі: {e}")
            results['components']['seasonal_patterns'] = {'error': str(e)}

        try:
            # 6. Генерація рекомендацій
            print("\n6️⃣ Генерація рекомендацій...")
            recommendations = self.efficiency_analyzer.generate_improvement_recommendations()
            results['components']['recommendations'] = recommendations

        except Exception as e:
            print(f"❌ Помилка в генерації рекомендацій: {e}")
            results['components']['recommendations'] = {'error': str(e)}

        # Підсумок
        successful_components = sum(1 for comp in results['components'].values()
                                    if not isinstance(comp, dict) or 'error' not in comp)
        total_components = len(results['components'])

        results['summary'] = {
            'successful_components': successful_components,
            'total_components': total_components,
            'success_rate': successful_components / total_components if total_components > 0 else 0,
            'status': 'completed' if successful_components > 0 else 'failed'
        }

        print(f"\n✅ Data Science аналіз завершено!")
        print(f"📊 Успішно виконано: {successful_components}/{total_components} компонентів")

        return results

    def get_quick_forecast(self, department_id=None):
        """Швидкий прогноз для конкретного відділення або загальний"""
        try:
            forecast_data = self.delivery_forecast.forecast_next_month()

            if department_id:
                dept_forecast = [
                    f for f in forecast_data['detailed_forecasts']
                    if f['department_id'] == department_id
                ]
                return dept_forecast if dept_forecast else {'error': 'Відділення не знайдено'}

            return forecast_data['summary']

        except Exception as e:
            return {'error': f'Помилка прогнозування: {str(e)}'}