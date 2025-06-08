"""
Генератор звітів для аналізу Data Warehouse
Працює з новою структурою даних
"""

import json
import os
import sys
from datetime import datetime
import glob

sys.path.append('..')
from config.database_config import DatabaseConfig

class DWReportGenerator:
    def __init__(self):
        self.config = DatabaseConfig()

    def get_latest_analysis_files(self):
        """Отримує найновіші файли аналізів"""
        patterns = {
            'courier': 'courier_performance_analysis_*.json',
            'department': 'department_workload_analysis_*.json',
            'processing': 'processing_time_analysis_*.json',
            'transport': 'transport_utilization_analysis_*.json'
        }

        files = {}
        for key, pattern in patterns.items():
            file_pattern = os.path.join(self.config.PROCESSED_DATA_PATH, pattern)
            matching_files = glob.glob(file_pattern)
            if matching_files:
                files[key] = max(matching_files, key=os.path.getctime)
            else:
                files[key] = None

        return files

    def load_analysis_data(self, filepath):
        """Завантажує дані аналізу з JSON"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Помилка завантаження {filepath}: {e}")
            return None

    def generate_executive_summary(self):
        """Генерує виконавчий звіт"""
        print("📋 Генерація виконавчого звіту...")

        files = self.get_latest_analysis_files()

        # Завантажуємо дані
        data = {}
        for key, filepath in files.items():
            if filepath:
                data[key] = self.load_analysis_data(filepath)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(self.config.REPORTS_PATH, f'executive_summary_{timestamp}.txt')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ВИКОНАВЧИЙ ЗВІТ - АНАЛІЗ DATA WAREHOUSE ПОШТОВОЇ СЛУЖБИ\n")
            f.write("="*80 + "\n")
            f.write(f"Дата створення: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n")

            # Загальна статистика
            if data.get('courier'):
                courier_stats = data['courier']['general_stats']
                f.write("🚚 КУР'ЄРСЬКА СЛУЖБА:\n")
                f.write(f"   • Всього кур'єрів: {courier_stats.get('total_couriers', 'N/A')}\n")
                f.write(f"   • Всього доставок: {courier_stats.get('total_deliveries', 'N/A')}\n")
                f.write(f"   • Середній час доставки: {courier_stats.get('avg_delivery_time', 'N/A'):.1f} хв\n\n")

            if data.get('department'):
                dept_stats = data['department']['general_stats']
                f.write("🏢 ВІДДІЛЕННЯ:\n")
                f.write(f"   • Всього відділень: {dept_stats.get('total_departments', 'N/A')}\n")
                f.write(f"   • Всього доставок: {dept_stats.get('total_deliveries', 'N/A')}\n")
                f.write(f"   • Середній час обробки: {dept_stats.get('avg_processing_time', 'N/A'):.1f} год\n\n")

            if data.get('transport'):
                transport_stats = data['transport']['general_stats']
                f.write("🚛 ТРАНСПОРТ:\n")
                f.write(f"   • Типів транспорту: {transport_stats.get('total_transport_types', 'N/A')}\n")
                f.write(f"   • Всього доставок: {transport_stats.get('total_deliveries', 'N/A')}\n\n")

            # Рекомендації
            f.write("💡 КЛЮЧОВІ РЕКОМЕНДАЦІЇ:\n")
            f.write("   1. Оптимізувати маршрути найповільніших кур'єрів\n")
            f.write("   2. Перерозподілити навантаження між відділеннями\n")
            f.write("   3. Покращити використання транспорту\n")
            f.write("   4. Впровадити автоматизацію обробки посилок\n\n")

        print(f"✅ Виконавчий звіт збережено: {os.path.basename(report_path)}")
        return report_path

    def generate_detailed_report(self):
        """Генерує детальний звіт"""
        print("📊 Генерація детального звіту...")

        files = self.get_latest_analysis_files()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(self.config.REPORTS_PATH, f'detailed_report_{timestamp}.txt')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*100 + "\n")
            f.write("ДЕТАЛЬНИЙ ЗВІТ - АНАЛІЗ DATA WAREHOUSE ПОШТОВОЇ СЛУЖБИ\n")
            f.write("="*100 + "\n")
            f.write(f"Дата створення: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n")

            # Детальний аналіз по кожному компоненту
            for key, filepath in files.items():
                if filepath:
                    data = self.load_analysis_data(filepath)
                    if data:
                        self._write_detailed_section(f, key, data)

        print(f"✅ Детальний звіт збережено: {os.path.basename(report_path)}")
        return report_path

    def _write_detailed_section(self, file, section_key, data):
        """Записує детальну секцію звіту"""
        section_names = {
            'courier': '🚚 АНАЛІЗ КУР\'ЄРІВ',
            'department': '🏢 АНАЛІЗ ВІДДІЛЕНЬ',
            'processing': '⏱️ АНАЛІЗ ЧАСУ ОБРОБКИ',
            'transport': '🚛 АНАЛІЗ ТРАНСПОРТУ'
        }

        file.write(f"\n{section_names.get(section_key, section_key.upper())}\n")
        file.write("-" * 80 + "\n")

        if 'general_stats' in data:
            file.write("Загальна статистика:\n")
            for key, value in data['general_stats'].items():
                file.write(f"  • {key}: {value}\n")
            file.write("\n")

    def generate_all_reports(self):
        """Генерує всі звіти"""
        reports = []

        try:
            exec_report = self.generate_executive_summary()
            reports.append(exec_report)
        except Exception as e:
            print(f"❌ Помилка генерації виконавчого звіту: {e}")

        try:
            detailed_report = self.generate_detailed_report()
            reports.append(detailed_report)
        except Exception as e:
            print(f"❌ Помилка генерації детального звіту: {e}")

        return reports