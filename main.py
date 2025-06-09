"""
Головний файл для запуску всього процесу аналізу Data Warehouse з інтерактивним інтерфейсом
NumPy 2.x compatible
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
        print("5. 🔄 Повний цикл (вивантаження + аналіз + звіти + графіки)")
        print("6. 🧹 Очистити старі файли")
        print("7. 📁 Показати статус файлів")
        print("8. 🔍 Тестувати підключення до DW")
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
            'delivery_periodic': 'delivery_periodic_raw_data_*.csv'  # Один файл для завдань 2,3,4
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
            # ✅ ВИПРАВЛЕНО: викликаємо правильний метод з періодами
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
            # ✅ ВИПРАВЛЕНО: викликаємо правильний метод з періодами
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
            # ✅ ВИПРАВЛЕНО: викликаємо правильний метод з періодами
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
            ("📋 Звіти", self.config.REPORTS_PATH, "*.txt")
        ]

        print(f"\n📂 ЗГЕНЕРОВАНІ ФАЙЛИ:")
        for name, path, pattern in directories:
            files_count = len(glob.glob(os.path.join(path, pattern)))
            print(f"  {name}: {files_count} файлів")

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

    def run(self):
        """Головний цикл програми"""
        print("🎉 Ласкаво просимо до системи аналізу Data Warehouse поштової служби!")

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
                elif choice == '5':
                    self.full_cycle()
                elif choice == '6':
                    self.clean_old_files_menu()
                elif choice == '7':
                    self.show_file_status()
                elif choice == '8':
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