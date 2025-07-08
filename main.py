"""
Головний файл для запуску всього процесу аналізу Data Warehouse з інтерактивним інтерфейсом
NumPy 2.x compatible + Data Science моделі
"""

import os
import sys
from datetime import datetime
import glob
import warnings

# Налаштування для NumPy 2.x
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# Додаємо шляхи для імпорту
sys.path.append('.')

from data_extraction.data_extractor import DataWarehouseExtractor
from analysis.courier_analysis import CourierAnalyzer
from analysis.department_analysis import DepartmentAnalyzer
from analysis.processing_time_analysis import ProcessingTimeAnalyzer
from analysis.transport_analysis import TransportAnalyzer
from visualizations.charts import DWChartGenerator
from reports.report_generator import DWReportGenerator
from config.database_config import DatabaseConfig
from utils.helpers import get_latest_csv_file, create_directories, clean_old_files

# 🧠 Data Science імпорти
from data_science.ds_controller import DataScienceController

class PostDWAnalyticsSystem:
    def __init__(self):
        self.config = DatabaseConfig()
        self.extractor = DataWarehouseExtractor()
        self.courier_analyzer = CourierAnalyzer()
        self.department_analyzer = DepartmentAnalyzer()
        self.processing_analyzer = ProcessingTimeAnalyzer()
        self.transport_analyzer = TransportAnalyzer()
        self.chart_generator = DWChartGenerator()
        self.report_generator = DWReportGenerator()

        # 🧠 Data Science контролер
        self.ds_controller = DataScienceController()

        create_directories()

    def show_main_menu(self):
        """Показує головне меню"""
        print("\n" + "="*70)
        print("🏢 СИСТЕМА АНАЛІЗУ DATA WAREHOUSE ПОШТОВОЇ СЛУЖБИ")
        print("="*70)
        print("1. 📥 Вивантажити дані з Data Warehouse")
        print("2. 📊 Провести аналіз даних")
        print("3. 📋 Згенерувати звіти")
        print("4. 📈 Створити графіки та візуалізації")
        print("5. 🧠 Data Science аналіз та прогнозування")  # 🆕 НОВИЙ ПУНКТ
        print("6. 🔄 Повний цикл (вивантаження + аналіз + звіти + графіки)")
        print("7. 🧹 Очистити старі файли")
        print("8. 📁 Показати статус файлів")
        print("9. 🔍 Тестувати підключення до DW")
        print("0. ❌ Вихід")
        print("="*70)

    def show_analysis_menu(self):
        """Показує меню аналізу"""
        print("\n" + "-"*60)
        print("📊 МЕНЮ АНАЛІЗУ DATA WAREHOUSE")
        print("-"*60)
        print("1. 🚚 Аналіз продуктивності кур'єрів (Завдання 1)")
        print("2. 🏢 Аналіз завантажень відділень по періодах (Завдання 2)")
        print("3. ⏱️ Аналіз часу обробки посилок по періодах (Завдання 3)")
        print("4. 🚛 Аналіз використання транспорту по періодах (Завдання 4)")
        print("5. 🎯 Провести всі аналізи")
        print("0. ⬅️ Повернутися до головного меню")
        print("-"*60)

    def show_charts_menu(self):
        """Показує меню створення графіків"""
        print("\n" + "-"*50)
        print("📈 МЕНЮ СТВОРЕННЯ ГРАФІКІВ")
        print("-"*50)
        print("1. 🚚 Графіки продуктивності кур'єрів")
        print("2. 🏢 Графіки завантаження відділень")
        print("3. 🚛 Графіки використання транспорту")
        print("4. 🎨 Створити всі графіки")
        print("0. ⬅️ Повернутися до головного меню")
        print("-"*50)

    def show_reports_menu(self):
        """Показує меню генерації звітів"""
        print("\n" + "-"*50)
        print("📋 МЕНЮ ГЕНЕРАЦІЇ ЗВІТІВ")
        print("-"*50)
        print("1. 📄 Виконавчий звіт")
        print("2. 📊 Детальний звіт")
        print("3. 📋 Згенерувати всі звіти")
        print("0. ⬅️ Повернутися до головного меню")
        print("-"*50)

    # 🧠 НОВИЙ МЕТОД: Меню Data Science
    def show_data_science_menu(self):
        """Показує меню Data Science аналізу"""
        print("\n" + "-"*60)
        print("🧠 МЕНЮ DATA SCIENCE АНАЛІЗУ ТА ПРОГНОЗУВАННЯ")
        print("-"*60)
        print("1. 🎯 Повний Data Science аналіз")
        print("2. 📈 Прогнозування доставок на наступний місяць")
        print("3. 🏢 Аналіз ефективності відділень")
        print("4. 🚛 Аналіз ефективності транспорту")
        print("5. 📅 Аналіз сезонних патернів")
        print("6. 💡 Генерація рекомендацій для оптимізації")
        print("7. 🔮 Швидкий прогноз для відділення")
        print("8. 📊 Показати важливість ознак моделі")
        print("0. ⬅️ Повернутися до головного меню")
        print("-"*60)

    def test_dw_connection(self):
        """Тестує підключення до Data Warehouse"""
        print("\n🔍 ТЕСТУВАННЯ ПІДКЛЮЧЕННЯ ДО DATA WAREHOUSE")
        print("-" * 50)

        success, message = self.extractor.test_connection()

        if success:
            print(f"✅ {message}")
        else:
            print(f"❌ {message}")
            print("\n💡 Рекомендації:")
            print("• Перевірте, чи запущена LocalDB")
            print("• Переконайтеся, що база даних PostDW існує")
            print("• Перевірте права доступу")

        return success

    def extract_data(self):
        """Вивантаження даних з Data Warehouse"""
        print("\n🔄 ПОЧАТОК ВИВАНТАЖЕННЯ ДАНИХ З DATA WAREHOUSE")
        print("=" * 60)

        try:
            results = self.extractor.extract_all_raw_data()

            if isinstance(results, dict) and 'error' in results:
                print(f"❌ {results['error']}")
                return False

            successful = sum(1 for result in results.values() if result.get('success', False))
            total = len(results)

            print(f"\n📊 ПІДСУМОК ВИВАНТАЖЕННЯ: {successful}/{total} успішно")

            for name, result in results.items():
                if result.get('success'):
                    print(f"✅ {name}: {result.get('records_count', 0)} записів")
                else:
                    print(f"❌ {name}: {result.get('error', 'Невідома помилка')}")

            return successful > 0

        except Exception as e:
            print(f"❌ Помилка при вивантаженні: {e}")
            return False

    def get_available_files(self):
        """Отримує список доступних файлів для аналізу"""
        files = {}

        patterns = {
            'courier_delivery': 'courier_delivery_raw_data_*.csv',
            'delivery_periodic': 'delivery_periodic_raw_data_*.csv'
        }

        for key, pattern in patterns.items():
            file_path = get_latest_csv_file(self.config.RAW_DATA_PATH, pattern)
            files[key] = file_path

        return files

    def run_courier_analysis(self):
        """Запуск аналізу кур'єрів (Завдання 1)"""
        files = self.get_available_files()

        if not files['courier_delivery']:
            print("❌ Файл courier_delivery_raw_data не знайдено.")
            print("💡 Спочатку вивантажте дані з Data Warehouse (пункт 1)")
            return False

        try:
            print("🔄 Аналіз продуктивності кур'єрів...")
            results = self.courier_analyzer.analyze_courier_performance(files['courier_delivery'])

            if 'error' in results:
                print(f"❌ {results['error']}")
                return False

            print("✅ Аналіз кур'єрів завершено!")
            print(f"📊 Проаналізовано {results['general_stats']['total_couriers']} кур'єрів")
            return True
        except Exception as e:
            print(f"❌ Помилка при аналізі кур'єрів: {e}")
            return False

    def run_department_analysis(self):
        """Запуск аналізу відділень по періодах (Завдання 2)"""
        files = self.get_available_files()

        if not files['delivery_periodic']:
            print("❌ Файл delivery_periodic_raw_data не знайдено.")
            print("💡 Спочатку вивантажте дані з Data Warehouse (пункт 1)")
            return False

        try:
            print("🔄 Аналіз завантажень відділень по періодах...")
            results = self.department_analyzer.analyze_department_workload_by_periods(files['delivery_periodic'])

            if 'error' in results:
                print(f"❌ {results['error']}")
                return False

            print("✅ Аналіз відділень по періодах завершено!")
            print(f"📊 Проаналізовано {results['general_stats']['total_departments']} відділень")
            print(f"📅 За {results['general_stats']['total_periods']} періодів")
            return True
        except Exception as e:
            print(f"❌ Помилка при аналізі відділень: {e}")
            return False

    def run_processing_analysis(self):
        """Запуск аналізу часу обробки по періодах (Завдання 3)"""
        files = self.get_available_files()

        if not files['delivery_periodic']:
            print("❌ Файл delivery_periodic_raw_data не знайдено.")
            print("💡 Спочатку вивантажте дані з Data Warehouse (пункт 1)")
            return False

        try:
            print("🔄 Аналіз часу обробки посилок по періодах...")
            results = self.processing_analyzer.analyze_processing_times_by_periods(files['delivery_periodic'])

            if 'error' in results:
                print(f"❌ {results['error']}")
                return False

            print("✅ Аналіз часу обробки по періодах завершено!")
            print(f"📊 Проаналізовано {results['general_stats']['total_parcel_types']} типів посилок")
            print(f"📅 За {results['general_stats']['total_periods']} періодів")
            return True
        except Exception as e:
            print(f"❌ Помилка при аналізі часу обробки: {e}")
            return False

    def run_transport_analysis(self):
        """Запуск аналізу транспорту по періодах (Завдання 4)"""
        files = self.get_available_files()

        if not files['delivery_periodic']:
            print("❌ Файл delivery_periodic_raw_data не знайдено.")
            print("💡 Спочатку вивантажте дані з Data Warehouse (пункт 1)")
            return False

        try:
            print("🔄 Аналіз використання транспорту по періодах...")
            results = self.transport_analyzer.analyze_transport_utilization_by_periods(files['delivery_periodic'])

            if 'error' in results:
                print(f"❌ {results['error']}")
                return False

            print("✅ Аналіз транспорту по періодах завершено!")
            print(f"📊 Проаналізовано {results['general_stats']['total_transport_types']} типів транспорту")
            print(f"📅 За {results['general_stats']['total_periods']} періодів")
            return True
        except Exception as e:
            print(f"❌ Помилка при аналізі транспорту: {e}")
            return False

    def run_all_analysis(self):
        """Запуск всіх аналізів"""
        print("\n🚀 ЗАПУСК ВСІХ АНАЛІЗІВ DATA WAREHOUSE")
        print("=" * 50)

        analyses = [
            ("🚚 Продуктивність кур'єрів", self.run_courier_analysis),
            ("🏢 Завантаження відділень", self.run_department_analysis),
            ("⏱️ Час обробки посилок", self.run_processing_analysis),
            ("🚛 Використання транспорту", self.run_transport_analysis)
        ]

        results = {}

        for name, analysis_func in analyses:
            print(f"\n🔄 {name}...")
            try:
                results[name] = analysis_func()
            except Exception as e:
                print(f"❌ Помилка в {name}: {e}")
                results[name] = False

        successful = sum(results.values())
        total = len(results)

        print(f"\n📊 ПІДСУМОК АНАЛІЗІВ: {successful}/{total} успішно завершено")
        print("=" * 50)

        for analysis_name, success in results.items():
            status = "✅" if success else "❌"
            print(f"{status} {analysis_name}")

        return successful > 0

    # 🧠 НОВІ МЕТОДИ DATA SCIENCE

    def run_full_data_science_analysis(self):
        """Запуск повного Data Science аналізу"""
        print("\n🧠 ЗАПУСК ПОВНОГО DATA SCIENCE АНАЛІЗУ")
        print("=" * 50)

        try:
            results = self.ds_controller.run_full_analysis()

            if results['summary']['status'] == 'completed':
                print(f"✅ Data Science аналіз завершено!")
                print(f"📊 Успішно: {results['summary']['successful_components']}/{results['summary']['total_components']} компонентів")

                # Показуємо короткий підсумок
                if 'next_month_forecast' in results['components'] and 'error' not in results['components']['next_month_forecast']:
                    forecast = results['components']['next_month_forecast']['summary']
                    print(f"🔮 Прогноз на наступний місяць: {forecast['total_predicted_deliveries']} доставок")

                return True
            else:
                print("❌ Data Science аналіз завершено з помилками")
                return False

        except Exception as e:
            print(f"❌ Помилка в Data Science аналізі: {e}")
            return False

    def forecast_next_month(self):
        """Прогнозування на наступний місяць"""
        print("\n📈 ПРОГНОЗУВАННЯ ДОСТАВОК НА НАСТУПНИЙ МІСЯЦЬ")
        print("-" * 50)

        try:
            # Спочатку навчаємо модель
            print("🎯 Навчання моделі прогнозування...")
            training_results = self.ds_controller.delivery_forecast.train_forecast_model()

            if 'error' in training_results:
                print(f"❌ Помилка навчання: {training_results['error']}")
                return False

            print(f"✅ Модель навчена! R² = {training_results['model_metrics']['test_r2']:.3f}")

            # Робимо прогноз
            print("🔮 Створення прогнозу...")
            forecast_results = self.ds_controller.delivery_forecast.forecast_next_month()

            if 'error' in forecast_results:
                print(f"❌ Помилка прогнозування: {forecast_results['error']}")
                return False

            # Показуємо результати
            summary = forecast_results['summary']
            print(f"\n📊 ПРОГНОЗ НА {summary['forecast_period']}:")
            print(f"🎯 Загальна кількість доставок: {summary['total_predicted_deliveries']}")
            print(f"🏢 Кількість відділень: {summary['total_departments']}")

            print(f"\n🌍 ТОП-5 РЕГІОНІВ:")
            sorted_regions = sorted(summary['region_forecasts'].items(), key=lambda x: x[1], reverse=True)
            for i, (region, count) in enumerate(sorted_regions[:5], 1):
                print(f"  {i}. {region}: {count} доставок")

            print(f"\n📦 ТОП-3 ТИПИ ПОСИЛОК:")
            sorted_parcels = sorted(summary['parcel_type_forecasts'].items(), key=lambda x: x[1], reverse=True)
            for i, (parcel_type, count) in enumerate(sorted_parcels[:3], 1):
                print(f"  {i}. {parcel_type}: {count} доставок")

            return True

        except Exception as e:
            print(f"❌ Помилка прогнозування: {e}")
            return False

    def analyze_department_efficiency(self):
        """Аналіз ефективності відділень"""
        print("\n🏢 АНАЛІЗ ЕФЕКТИВНОСТІ ВІДДІЛЕНЬ")
        print("-" * 40)

        try:
            dept_performance = self.ds_controller.efficiency_analyzer.analyze_department_performance()

            print(f"📊 Проаналізовано {len(dept_performance)} відділень")

            # Топ-5 найефективніших
            top_depts = dept_performance.nlargest(5, 'efficiency_score')
            print(f"\n🏆 ТОП-5 НАЙЕФЕКТИВНІШИХ ВІДДІЛЕНЬ:")
            for i, (_, dept) in enumerate(top_depts.iterrows(), 1):
                print(f"  {i}. {dept['department_number']} ({dept['department_city']})")
                print(f"     Ефективність: {dept['efficiency_score']:.3f}, Доставок: {dept['total_deliveries']}")

            # Відділення, що потребують покращення
            low_performance = dept_performance[dept_performance['performance_category'] == 'Needs_Improvement']
            if len(low_performance) > 0:
                print(f"\n⚠️ ВІДДІЛЕННЯ, ЩО ПОТРЕБУЮТЬ ПОКРАЩЕННЯ: {len(low_performance)}")
                for _, dept in low_performance.head(3).iterrows():
                    print(f"  • {dept['department_number']} ({dept['department_city']})")
                    print(f"    Ефективність: {dept['efficiency_score']:.3f}")

            # Аномальні відділення
            anomalies = dept_performance[dept_performance['is_anomaly'] == True]
            if len(anomalies) > 0:
                print(f"\n🔍 ВИЯВЛЕНО АНОМАЛІЇ: {len(anomalies)} відділень")
                for _, dept in anomalies.iterrows():
                    print(f"  ⚡ {dept['department_number']} ({dept['department_city']}) - потребує перевірки")

            return True

        except Exception as e:
            print(f"❌ Помилка аналізу ефективності: {e}")
            return False

    def analyze_transport_efficiency(self):
        """Аналіз ефективності транспорту"""
        print("\n🚛 АНАЛІЗ ЕФЕКТИВНОСТІ ТРАНСПОРТУ")
        print("-" * 40)

        try:
            transport_analysis = self.ds_controller.efficiency_analyzer.analyze_transport_efficiency()

            print(f"📊 Проаналізовано {len(transport_analysis)} типів транспорту")

            # Сортуємо по ефективності
            transport_sorted = transport_analysis.sort_values('transport_efficiency_score', ascending=False)

            print(f"\n🏆 РЕЙТИНГ ЕФЕКТИВНОСТІ ТРАНСПОРТУ:")
            for i, (_, transport) in enumerate(transport_sorted.iterrows(), 1):
                print(f"  {i}. {transport['transport_type_name']}")
                print(f"     Ефективність: {transport['transport_efficiency_score']:.3f}")
                print(f"     Використовується в {transport['departments_using']} відділеннях")
                print(f"     Середня вага: {transport['avg_max_weight']:.1f} кг")

            return True

        except Exception as e:
            print(f"❌ Помилка аналізу транспорту: {e}")
            return False

    def analyze_seasonal_patterns(self):
        """Аналіз сезонних патернів"""
        print("\n📅 АНАЛІЗ СЕЗОННИХ ПАТЕРНІВ")
        print("-" * 35)

        try:
            seasonal_analysis = self.ds_controller.efficiency_analyzer.analyze_seasonal_patterns()

            # Місячні паттерни
            monthly_patterns = seasonal_analysis['monthly_patterns']
            print("📊 АКТИВНІСТЬ ПО МІСЯЦЯХ:")

            months_names = {
                1: 'Січень', 2: 'Лютий', 3: 'Березень', 4: 'Квітень',
                5: 'Травень', 6: 'Червень', 7: 'Липень', 8: 'Серпень',
                9: 'Вересень', 10: 'Жовтень', 11: 'Листопад', 12: 'Грудень'
            }

            for month, data in monthly_patterns.items():
                month_name = months_names.get(month, f"Місяць {month}")
                print(f"  {month_name}: {data['total_deliveries']} доставок, "
                      f"час обробки: {data['avg_processing_time']:.1f} год")

            # Сезонні паттерни
            seasonal_patterns = seasonal_analysis['seasonal_patterns']
            if seasonal_patterns:
                print(f"\n🌍 СЕЗОННА АКТИВНІСТЬ:")
                season_names = {'Winter': 'Зима', 'Spring': 'Весна', 'Summer': 'Літо', 'Autumn': 'Осінь'}

                for season, data in seasonal_patterns.items():
                    season_name = season_names.get(season, season)
                    print(f"  {season_name}: {data['total_deliveries']} доставок, "
                          f"{data['active_departments']} активних відділень")

            return True

        except Exception as e:
            print(f"❌ Помилка сезонного аналізу: {e}")
            return False

    def generate_optimization_recommendations(self):
        """Генерація рекомендацій для оптимізації"""
        print("\n💡 ГЕНЕРАЦІЯ РЕКОМЕНДАЦІЙ ДЛЯ ОПТИМІЗАЦІЇ")
        print("-" * 45)

        try:
            recommendations = self.ds_controller.efficiency_analyzer.generate_improvement_recommendations()

            if not recommendations:
                print("✅ Рекомендацій не знайдено - система працює оптимально!")
                return True

            print(f"📋 Згенеровано {len(recommendations)} рекомендацій:")

            # Групуємо по пріоритету
            high_priority = [r for r in recommendations if r['priority'] == 'high']
            medium_priority = [r for r in recommendations if r['priority'] == 'medium']

            if high_priority:
                print(f"\n🔥 ВИСОКИЙ ПРІОРИТЕТ ({len(high_priority)}):")
                for i, rec in enumerate(high_priority, 1):
                    print(f"  {i}. {rec['target']}")
                    print(f"     Тип: {rec['type']}")
                    for issue in rec['issues']:
                        print(f"     ⚠️ {issue}")
                    print(f"     💡 Рекомендації:")
                    for suggestion in rec['suggestions']:
                        print(f"       • {suggestion}")
                    print()

            if medium_priority:
                print(f"\n📋 СЕРЕДНІЙ ПРІОРИТЕТ ({len(medium_priority)}):")
                for i, rec in enumerate(medium_priority, 1):
                    print(f"  {i}. {rec['target']}")
                    print(f"     💡 {rec['suggestions'][0] if rec['suggestions'] else 'Немає конкретних рекомендацій'}")

            return True

        except Exception as e:
            print(f"❌ Помилка генерації рекомендацій: {e}")
            return False

    def quick_department_forecast(self):
        """Швидкий прогноз для конкретного відділення"""
        print("\n🔮 ШВИДКИЙ ПРОГНОЗ ДЛЯ ВІДДІЛЕННЯ")
        print("-" * 35)

        try:
            dept_id = input("Введіть ID відділення (або Enter для загального прогнозу): ").strip()

            if dept_id:
                try:
                    dept_id = int(dept_id)
                    forecast = self.ds_controller.get_quick_forecast(dept_id)
                except ValueError:
                    print("❌ Невірний формат ID відділення!")
                    return False
            else:
                forecast = self.ds_controller.get_quick_forecast()

            if 'error' in forecast:
                print(f"❌ {forecast['error']}")
                return False

            if isinstance(forecast, list):
                # Прогноз для конкретного відділення
                if forecast:
                    dept_forecast = forecast[0]
                    print(f"🏢 Відділення: {dept_forecast['department_number']} ({dept_forecast['department_city']})")
                    print(f"📈 Прогноз доставок: {dept_forecast['predicted_deliveries']}")
                    print(f"📅 Період: {dept_forecast['forecast_month']}/{dept_forecast['forecast_year']}")
                    print(f"🎯 Впевненість: {dept_forecast['confidence']}")
                else:
                    print("❌ Прогноз для цього відділення недоступний")
            else:
                # Загальний прогноз
                print(f"🎯 Загальний прогноз на {forecast['forecast_period']}:")
                print(f"📦 Всього доставок: {forecast['total_predicted_deliveries']}")
                print(f"🏢 Відділень: {forecast['total_departments']}")

            return True

        except Exception as e:
            print(f"❌ Помилка швидкого прогнозу: {e}")
            return False

    def show_model_feature_importance(self):
        """Показує важливість ознак моделі"""
        print("\n📊 ВАЖЛИВІСТЬ ОЗНАК МОДЕЛІ ПРОГНОЗУВАННЯ")
        print("-" * 45)

        try:
            # Спочатку навчаємо модель, якщо не навчена
            if not hasattr(self.ds_controller.delivery_forecast, 'model') or self.ds_controller.delivery_forecast.model is None:
                print("🎯 Навчання моделі...")
                training_results = self.ds_controller.delivery_forecast.train_forecast_model()
                if 'error' in training_results:
                    print(f"❌ Помилка навчання: {training_results['error']}")
                    return False

            # Отримуємо важливість ознак
            importance = self.ds_controller.delivery_forecast.get_feature_importance()

            if not importance:
                print("❌ Важливість ознак недоступна для цієї моделі")
                return False

            print("🏆 ТОП-10 НАЙВАЖЛИВІШИХ ОЗНАК:")
            for i, feature_info in enumerate(importance[:10], 1):
                feature_name = feature_info['feature']
                importance_score = feature_info['importance']

                # Переклад назв ознак
                feature_translations = {
                    'department_id': 'ID відділення',
                    'parcel_type_id': 'Тип посилки',
                    'start_month': 'Місяць',
                    'processing_time_hours': 'Час обробки',
                    'parcel_max_weight': 'Максимальна вага',
                    'dept_avg_deliveries': 'Середні доставки відділення',
                    'region_avg_deliveries': 'Середні доставки регіону',
                    'is_winter': 'Зимовий сезон',
                    'is_spring': 'Весняний сезон'
                }

                translated_name = feature_translations.get(feature_name, feature_name)
                bar_length = int(importance_score * 50)  # Візуальна шкала
                bar = "█" * bar_length + "░" * (50 - bar_length)

                print(f"  {i:2d}. {translated_name:<25} {importance_score:.3f} |{bar}|")

            return True

        except Exception as e:
            print(f"❌ Помилка отримання важливості ознак: {e}")
            return False

    def create_charts(self):
        """Створення графіків"""
        print("\n🎨 СТВОРЕННЯ ГРАФІКІВ...")

        try:
            self.chart_generator.create_all_charts()
            return True
        except Exception as e:
            print(f"❌ Помилка при створенні графіків: {e}")
            return False

    def generate_reports(self):
        """Генерація звітів"""
        print("\n📋 ГЕНЕРАЦІЯ ЗВІТІВ...")

        try:
            reports = self.report_generator.generate_all_reports()
            print(f"✅ Згенеровано {len(reports)} звітів")
            return True
        except Exception as e:
            print(f"❌ Помилка при генерації звітів: {e}")
            return False

    def show_file_status(self):
        """Показує статус файлів"""
        print("\n📁 СТАТУС ФАЙЛІВ DATA WAREHOUSE")
        print("-" * 50)

        files = self.get_available_files()

        print("📥 ВИХІДНІ ДАНІ:")
        for file_type, file_path in files.items():
            if file_path and os.path.exists(file_path):
                file_size = os.path.getsize(file_path) / 1024
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                print(f"  ✅ {file_type}: {os.path.basename(file_path)}")
                print(f"     📊 Розмір: {file_size:.1f} KB, 🕒 {mod_time.strftime('%d.%m.%Y %H:%M')}")
            else:
                print(f"  ❌ {file_type}: файл не знайдено")

        # Статистика по директоріях
        directories = [
            ("📊 Оброблені дані", self.config.PROCESSED_DATA_PATH, "*.json"),
            ("📈 Графіки", self.config.CHARTS_PATH, "*.png"),
            ("📋 Звіти", self.config.REPORTS_PATH, "*.txt"),
            ("🧠 ML моделі", os.path.join(self.config.PROCESSED_DATA_PATH, 'models'), "*.joblib")  # 🆕
        ]

        print(f"\n📂 ЗГЕНЕРОВАНІ ФАЙЛИ:")
        for name, path, pattern in directories:
            if os.path.exists(path):
                files_count = len(glob.glob(os.path.join(path, pattern)))
                print(f"  {name}: {files_count} файлів")
            else:
                print(f"  {name}: директорія не існує")

    def clean_old_files_menu(self):
        """Меню очищення старих файлів"""
        print("\n🧹 ОЧИЩЕННЯ СТАРИХ ФАЙЛІВ")
        print("-" * 30)
        print("Скільки днів зберігати файли?")
        print("1. 1 день")
        print("2. 3 дні")
        print("3. 7 днів (рекомендовано)")
        print("4. 30 днів")
        print("0. Скасувати")

        choice = input("\nВаш вибір: ").strip()

        days_map = {'1': 1, '2': 3, '3': 7, '4': 30}

        if choice in days_map:
            days = days_map[choice]
            try:
                directories = [
                    self.config.RAW_DATA_PATH,
                    self.config.PROCESSED_DATA_PATH,
                    self.config.CHARTS_PATH,
                    self.config.REPORTS_PATH
                ]

                for directory in directories:
                    clean_old_files(directory, days)

                print(f"✅ Файли старші {days} днів видалено!")
            except Exception as e:
                print(f"❌ Помилка при очищенні: {e}")
        elif choice == '0':
            return
        else:
            print("❌ Невірний вибір!")

    def full_cycle(self):
        """Повний цикл обробки"""
        print("\n🚀 ЗАПУСК ПОВНОГО ЦИКЛУ DATA WAREHOUSE АНАЛІЗУ")
        print("=" * 60)

        steps = [
            ("📥 Вивантаження даних з DW", self.extract_data),
            ("📊 Аналіз даних", self.run_all_analysis),
            ("🧠 Data Science аналіз", self.run_full_data_science_analysis),  # 🆕
            ("📋 Генерація звітів", self.generate_reports),
            ("📈 Створення графіків", self.create_charts)
        ]

        results = []

        for step_name, step_function in steps:
            print(f"\n🔄 {step_name}...")
            try:
                result = step_function()
                results.append(result)
                if result:
                    print(f"✅ {step_name} завершено успішно!")
                else:
                    print(f"❌ {step_name} завершено з помилками!")
            except Exception as e:
                print(f"❌ Помилка в {step_name}: {e}")
                results.append(False)

        successful_steps = sum(results)
        total_steps = len(results)

        print(f"\n📊 ПІДСУМОК ПОВНОГО ЦИКЛУ")
        print("=" * 40)
        print(f"Успішно завершено: {successful_steps}/{total_steps} кроків")

        for i, (step_name, _) in enumerate(steps):
            status = "✅" if results[i] else "❌"
            print(f"{status} {step_name}")

        if successful_steps == total_steps:
            print("\n🎉 Повний цикл завершено успішно!")
        else:
            print("\n⚠️ Повний цикл завершено з помилками!")

    def handle_analysis_menu(self):
        """Обробка меню аналізу"""
        while True:
            self.show_analysis_menu()
            choice = input("\nВаш вибір: ").strip()

            if choice == '1':
                self.run_courier_analysis()
            elif choice == '2':
                self.run_department_analysis()
            elif choice == '3':
                self.run_processing_analysis()
            elif choice == '4':
                self.run_transport_analysis()
            elif choice == '5':
                self.run_all_analysis()
            elif choice == '0':
                break
            else:
                print("❌ Невірний вибір! Спробуйте ще раз.")

            input("\nНатисніть Enter для продовження...")

    def handle_charts_menu(self):
        """Обробка меню графіків"""
        while True:
            self.show_charts_menu()
            choice = input("\nВаш вибір: ").strip()

            if choice == '1':
                self.chart_generator.create_courier_performance_charts()
            elif choice == '2':
                self.chart_generator.create_department_workload_charts()
            elif choice == '3':
                self.chart_generator.create_transport_utilization_charts()
            elif choice == '4':
                self.create_charts()
            elif choice == '0':
                break
            else:
                print("❌ Невірний вибір! Спробуйте ще раз.")

            input("\nНатисніть Enter для продовження...")

    def handle_reports_menu(self):
        """Обробка меню звітів"""
        while True:
            self.show_reports_menu()
            choice = input("\nВаш вибір: ").strip()

            if choice == '1':
                self.report_generator.generate_executive_summary()
            elif choice == '2':
                self.report_generator.generate_detailed_report()
            elif choice == '3':
                self.generate_reports()
            elif choice == '0':
                break
            else:
                print("❌ Невірний вибір! Спробуйте ще раз.")

            input("\nНатисніть Enter для продовження...")

    # 🧠 НОВИЙ МЕТОД: Обробка меню Data Science
    def handle_data_science_menu(self):
        """Обробка меню Data Science"""
        while True:
            self.show_data_science_menu()
            choice = input("\nВаш вибір: ").strip()

            if choice == '1':
                self.run_full_data_science_analysis()
            elif choice == '2':
                self.forecast_next_month()
            elif choice == '3':
                self.analyze_department_efficiency()
            elif choice == '4':
                self.analyze_transport_efficiency()
            elif choice == '5':
                self.analyze_seasonal_patterns()
            elif choice == '6':
                self.generate_optimization_recommendations()
            elif choice == '7':
                self.quick_department_forecast()
            elif choice == '8':
                self.show_model_feature_importance()
            elif choice == '0':
                break
            else:
                print("❌ Невірний вибір! Спробуйте ще раз.")

            input("\nНатисніть Enter для продовження...")

    def run(self):
        """Головний цикл програми"""
        print("🎉 Ласкаво просимо до системи аналізу Data Warehouse поштової служби!")
        print("🧠 Тепер з підтримкою Data Science та Machine Learning!")

        while True:
            try:
                self.show_main_menu()
                choice = input("\nВаш вибір: ").strip()

                if choice == '1':
                    self.extract_data()
                elif choice == '2':
                    self.handle_analysis_menu()
                elif choice == '3':
                    self.handle_reports_menu()
                elif choice == '4':
                    self.handle_charts_menu()
                elif choice == '5':  # 🆕 Data Science меню
                    self.handle_data_science_menu()
                elif choice == '6':
                    self.full_cycle()
                elif choice == '7':
                    self.clean_old_files_menu()
                elif choice == '8':
                    self.show_file_status()
                elif choice == '9':
                    self.test_dw_connection()
                elif choice == '0':
                    print("\n👋 До побачення!")
                    break
                else:
                    print("❌ Невірний вибір! Спробуйте ще раз.")

                if choice != '0':
                    input("\nНатисніть Enter для продовження...")

            except KeyboardInterrupt:
                print("\n\n👋 Програма перервана користувачем. До побачення!")
                break
            except Exception as e:
                print(f"\n❌ Неочікувана помилка: {e}")
                input("Натисніть Enter для продовження...")

def main():
    """Головна функція"""
    system = PostDWAnalyticsSystem()
    system.run()

if __name__ == "__main__":
    main()