"""
Генератор звітів для аналізу Data Warehouse
Працює з новою структурою окремих JSON файлів
"""

import json
import os
import sys
from datetime import datetime
import glob
import numpy as np

sys.path.append('..')
from config.database_config import DatabaseConfig

class DWReportGenerator:
    def __init__(self):
        self.config = DatabaseConfig()

        # Створюємо директорію для звітів якщо не існує
        os.makedirs(self.config.REPORTS_PATH, exist_ok=True)

    def get_latest_files_by_pattern(self, pattern):
        """Отримує найновіші файли за патерном"""
        file_pattern = os.path.join(self.config.PROCESSED_DATA_PATH, pattern)
        matching_files = glob.glob(file_pattern)
        if matching_files:
            return max(matching_files, key=os.path.getctime)
        return None

    def load_json_data(self, filepath):
        """Завантажує дані з JSON файлу"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Помилка завантаження {filepath}: {e}")
            return None

    def get_all_analysis_data(self):
        """Завантажує всі доступні дані аналізів"""
        data = {
            'courier': {},
            'department': {},
            'processing_time': {},
            'transport': {}
        }

        # Кур'єри
        courier_patterns = [
            'courier_general_stats_*.json',
            'courier_performance_*.json',
            'courier_top_performers_*.json',
            'courier_region_analysis_*.json',
            'courier_city_analysis_*.json'
        ]

        for pattern in courier_patterns:
            file_path = self.get_latest_files_by_pattern(pattern)
            if file_path:
                key = pattern.replace('courier_', '').replace('_*.json', '')
                data['courier'][key] = self.load_json_data(file_path)

        # Відділення
        department_patterns = [
            'department_general_stats_*.json',
            'department_period_summary_*.json',
            'department_trends_*.json',
            'department_top_busy_*.json',
            'department_type_analysis_*.json',
            'department_region_analysis_*.json',
            'department_city_analysis_*.json',
            'department_period_comparison_*.json'
        ]

        for pattern in department_patterns:
            file_path = self.get_latest_files_by_pattern(pattern)
            if file_path:
                key = pattern.replace('department_', '').replace('_*.json', '')
                data['department'][key] = self.load_json_data(file_path)

        # Час обробки
        processing_patterns = [
            'processing_time_general_stats_*.json',
            'processing_time_trends_*.json',
            'processing_time_period_comparison_*.json',
            'processing_time_region_analysis_*.json',
            'processing_time_period_changes_*.json'
        ]

        for pattern in processing_patterns:
            file_path = self.get_latest_files_by_pattern(pattern)
            if file_path:
                key = pattern.replace('processing_time_', '').replace('_*.json', '')
                data['processing_time'][key] = self.load_json_data(file_path)

        # Транспорт
        transport_patterns = [
            'transport_general_stats_*.json',
            'transport_period_usage_*.json',
            'transport_trends_*.json',
            'transport_efficiency_*.json',
            'transport_most_used_*.json',
            'transport_region_analysis_*.json'
        ]

        for pattern in transport_patterns:
            file_path = self.get_latest_files_by_pattern(pattern)
            if file_path:
                key = pattern.replace('transport_', '').replace('_*.json', '')
                data['transport'][key] = self.load_json_data(file_path)

        return data

    def generate_executive_summary(self):
        """Генерує виконавчий звіт"""
        print("📋 Генерація виконавчого звіту...")

        data = self.get_all_analysis_data()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(self.config.REPORTS_PATH, f'executive_summary_{timestamp}.txt')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ВИКОНАВЧИЙ ЗВІТ - АНАЛІЗ DATA WAREHOUSE ПОШТОВОЇ СЛУЖБИ\n")
            f.write("="*80 + "\n")
            f.write(f"Дата створення: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n")

            # Загальна статистика кур'єрів
            if data['courier'].get('general_stats') and 'data' in data['courier']['general_stats']:
                courier_stats = data['courier']['general_stats']['data']
                f.write("🚚 КУР'ЄРСЬКА СЛУЖБА:\n")
                f.write(f"   • Всього кур'єрів: {courier_stats.get('total_couriers', 'N/A'):,}\n")
                f.write(f"   • Всього доставок: {courier_stats.get('total_deliveries', 'N/A'):,}\n")
                f.write(f"   • Обслуговується регіонів: {courier_stats.get('total_regions', 'N/A')}\n")
                f.write(f"   • Обслуговується міст: {courier_stats.get('total_cities', 'N/A')}\n")

                if courier_stats.get('avg_delivery_time'):
                    f.write(f"   • Середній час доставки: {courier_stats['avg_delivery_time']:.1f} хв\n")
                f.write("\n")

            # Топ кур'єри
            if data['courier'].get('top_performers') and 'data' in data['courier']['top_performers']:
                top_couriers = data['courier']['top_performers']['data']
                f.write("🏆 ТОП-3 КУР'ЄРИ ЗА ЕФЕКТИВНІСТЮ:\n")

                sorted_couriers = sorted(top_couriers.items(),
                                       key=lambda x: x[1].get('efficiency_score', 0),
                                       reverse=True)[:3]

                for i, (courier_id, stats) in enumerate(sorted_couriers, 1):
                    f.write(f"   {i}. Кур'єр ID {stats.get('courier_id', courier_id)}: ")
                    f.write(f"рейтинг {stats.get('efficiency_score', 0):.1f}, ")
                    f.write(f"доставок {stats.get('total_deliveries', 0)}\n")
                f.write("\n")

            # Загальна статистика відділень
            if data['department'].get('general_stats') and 'data' in data['department']['general_stats']:
                dept_stats = data['department']['general_stats']['data']
                f.write("🏢 ВІДДІЛЕННЯ:\n")
                f.write(f"   • Всього відділень: {dept_stats.get('total_departments', 'N/A'):,}\n")
                f.write(f"   • Всього доставок: {dept_stats.get('total_deliveries', 'N/A'):,}\n")
                f.write(f"   • Регіонів: {dept_stats.get('total_regions', 'N/A')}\n")
                f.write(f"   • Міст: {dept_stats.get('total_cities', 'N/A')}\n")
                f.write(f"   • Періодів аналізу: {dept_stats.get('total_periods', 'N/A')}\n")

                if dept_stats.get('avg_processing_time'):
                    f.write(f"   • Середній час обробки: {dept_stats['avg_processing_time']:.1f} год\n")
                f.write("\n")

            # Час обробки
            if data['processing_time'].get('general_stats') and 'data' in data['processing_time']['general_stats']:
                proc_stats = data['processing_time']['general_stats']['data']
                f.write("⏱️ ЧАС ОБРОБКИ:\n")
                f.write(f"   • Середній час: {proc_stats.get('avg_processing_time', 'N/A'):.1f} год\n")
                f.write(f"   • Медіанний час: {proc_stats.get('median_processing_time', 'N/A'):.1f} год\n")
                f.write(f"   • Мінімальний час: {proc_stats.get('min_processing_time', 'N/A'):.1f} год\n")
                f.write(f"   • Максимальний час: {proc_stats.get('max_processing_time', 'N/A'):.1f} год\n")
                f.write(f"   • Всього записів: {proc_stats.get('total_records', 'N/A'):,}\n\n")

            # Транспорт
            if data['transport'].get('general_stats') and 'data' in data['transport']['general_stats']:
                transport_stats = data['transport']['general_stats']['data']
                f.write("🚛 ТРАНСПОРТ:\n")
                f.write(f"   • Типів транспорту: {transport_stats.get('total_transport_types', 'N/A')}\n")
                f.write(f"   • Всього доставок: {transport_stats.get('total_deliveries', 'N/A'):,}\n")
                f.write(f"   • Відділень використовують: {transport_stats.get('departments_using_transport', 'N/A')}\n")
                f.write(f"   • Регіонів обслуговується: {transport_stats.get('regions_served', 'N/A')}\n\n")

            # Найпопулярніший транспорт
            if data['transport'].get('most_used') and 'data' in data['transport']['most_used']:
                most_used = data['transport']['most_used']['data']
                f.write("🚚 НАЙПОПУЛЯРНІШИЙ ТРАНСПОРТ:\n")

                # Беремо перший період
                first_period = list(most_used.keys())[0] if most_used else None
                if first_period and most_used[first_period]:
                    transport_usage = {}
                    for key, value in most_used[first_period].items():
                        transport_type = value.get('transport_type_name', 'Невідомий')
                        if transport_type not in transport_usage:
                            transport_usage[transport_type] = 0
                        transport_usage[transport_type] += value.get('total_deliveries', 0)

                    sorted_transport = sorted(transport_usage.items(), key=lambda x: x[1], reverse=True)[:3]
                    for i, (transport, deliveries) in enumerate(sorted_transport, 1):
                        f.write(f"   {i}. {transport}: {deliveries:,} доставок\n")
                f.write("\n")

            # Аналіз тенденцій
            f.write("📈 КЛЮЧОВІ ТЕНДЕНЦІЇ:\n")

            # Тенденції відділень
            if data['department'].get('trends') and 'data' in data['department']['trends']:
                trends = data['department']['trends']['data']
                if trends:
                    f.write("   • Відділення:\n")
                    # Аналізуємо тренди (приклад)
                    total_deliveries = sum(dept.get('total_deliveries', 0) for dept in trends.values())
                    avg_deliveries = total_deliveries / len(trends) if trends else 0
                    f.write(f"     - Середня кількість доставок на відділення: {avg_deliveries:.0f}\n")

            # Проблемні зони
            f.write("\n⚠️ ПРОБЛЕМНІ ЗОНИ:\n")

            # Аналіз часу обробки
            if data['processing_time'].get('general_stats') and 'data' in data['processing_time']['general_stats']:
                proc_stats = data['processing_time']['general_stats']['data']
                avg_time = proc_stats.get('avg_processing_time', 0)
                max_time = proc_stats.get('max_processing_time', 0)

                if max_time > avg_time * 3:  # Якщо максимальний час у 3 рази більше середнього
                    f.write(f"   • Критично довгий час обробки: до {max_time:.1f} год\n")

                if avg_time > 24:  # Якщо середній час більше доби
                    f.write(f"   • Середній час обробки перевищує норму: {avg_time:.1f} год\n")

            # Рекомендації
            f.write("\n💡 КЛЮЧОВІ РЕКОМЕНДАЦІЇ:\n")

            # Рекомендації на основі аналізу
            recommendations = []

            # Аналіз кур'єрів
            if data['courier'].get('top_performers') and 'data' in data['courier']['top_performers']:
                top_couriers = data['courier']['top_performers']['data']
                if len(top_couriers) > 0:
                    efficiency_scores = [courier.get('efficiency_score', 0) for courier in top_couriers.values()]
                    if efficiency_scores:
                        min_efficiency = min(efficiency_scores)
                        max_efficiency = max(efficiency_scores)
                        if max_efficiency - min_efficiency > 50:  # Велика різниця в ефективності
                            recommendations.append("Провести навчання для кур'єрів з низькою ефективністю")

            # Аналіз часу обробки
            if data['processing_time'].get('general_stats') and 'data' in data['processing_time']['general_stats']:
                proc_stats = data['processing_time']['general_stats']['data']
                if proc_stats.get('avg_processing_time', 0) > 12:
                    recommendations.append("Оптимізувати процеси обробки посилок")
                if proc_stats.get('max_processing_time', 0) > 72:
                    recommendations.append("Дослідити причини критично довгої обробки")

            # Загальні рекомендації
            recommendations.extend([
                "Впровадити систему моніторингу ефективності в реальному часі",
                "Розробити програму мотивації для кращих кур'єрів",
                "Оптимізувати розподіл навантаження між відділеннями",
                "Покращити логістичні маршрути",
                "Автоматизувати процеси сортування та обробки"
            ])

            for i, rec in enumerate(recommendations, 1):
                f.write(f"   {i}. {rec}\n")

            f.write("\n" + "="*80 + "\n")
            f.write("Звіт згенеровано автоматично системою аналізу Data Warehouse\n")

        print(f"✅ Виконавчий звіт збережено: {os.path.basename(report_path)}")
        return report_path

    def generate_detailed_report(self):
        """Генерує детальний звіт"""
        print("📊 Генерація детального звіту...")

        data = self.get_all_analysis_data()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(self.config.REPORTS_PATH, f'detailed_report_{timestamp}.txt')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*100 + "\n")
            f.write("ДЕТАЛЬНИЙ ЗВІТ - АНАЛІЗ DATA WAREHOUSE ПОШТОВОЇ СЛУЖБИ\n")
            f.write("="*100 + "\n")
            f.write(f"Дата створення: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n")

            # Детальний аналіз кур'єрів
            self._write_courier_detailed_section(f, data['courier'])

            # Детальний аналіз відділень
            self._write_department_detailed_section(f, data['department'])

            # Детальний аналіз часу обробки
            self._write_processing_detailed_section(f, data['processing_time'])

            # Детальний аналіз транспорту
            self._write_transport_detailed_section(f, data['transport'])

        print(f"✅ Детальний звіт збережено: {os.path.basename(report_path)}")
        return report_path

    def _write_courier_detailed_section(self, file, courier_data):
        """Записує детальну секцію аналізу кур'єрів"""
        file.write("\n🚚 ДЕТАЛЬНИЙ АНАЛІЗ КУР'ЄРІВ\n")
        file.write("-" * 80 + "\n")

        # Загальна статистика
        if courier_data.get('general_stats') and 'data' in courier_data['general_stats']:
            stats = courier_data['general_stats']['data']
            file.write("Загальна статистика:\n")
            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    if isinstance(value, float):
                        file.write(f"  • {key}: {value:.2f}\n")
                    else:
                        file.write(f"  • {key}: {value:,}\n")
                else:
                    file.write(f"  • {key}: {value}\n")
            file.write("\n")

        # Топ кур'єри
        if courier_data.get('top_performers') and 'data' in courier_data['top_performers']:
            top_couriers = courier_data['top_performers']['data']
            file.write("Топ-10 кур'єрів за ефективністю:\n")

            sorted_couriers = sorted(top_couriers.items(),
                                   key=lambda x: x[1].get('efficiency_score', 0),
                                   reverse=True)[:10]

            for i, (courier_id, stats) in enumerate(sorted_couriers, 1):
                file.write(f"  {i:2d}. Кур'єр ID {stats.get('courier_id', courier_id):>6}: ")
                file.write(f"рейтинг {stats.get('efficiency_score', 0):>6.1f}, ")
                file.write(f"доставок {stats.get('total_deliveries', 0):>5}, ")
                file.write(f"час {stats.get('avg_delivery_time', 0):>6.1f} хв\n")
            file.write("\n")

        # Аналіз по регіонах
        if courier_data.get('region_analysis') and 'data' in courier_data['region_analysis']:
            regions = courier_data['region_analysis']['data']
            file.write("Аналіз по регіонах (топ-10):\n")

            sorted_regions = sorted(regions.items(),
                                  key=lambda x: x[1].get('total_deliveries', 0),
                                  reverse=True)[:10]

            for i, (region, stats) in enumerate(sorted_regions, 1):
                file.write(f"  {i:2d}. {region[:30]:<30}: ")
                file.write(f"{stats.get('total_deliveries', 0):>6} доставок, ")
                file.write(f"{stats.get('total_couriers', 0):>3} кур'єрів\n")
            file.write("\n")

    def _write_department_detailed_section(self, file, dept_data):
        """Записує детальну секцію аналізу відділень"""
        file.write("\n🏢 ДЕТАЛЬНИЙ АНАЛІЗ ВІДДІЛЕНЬ\n")
        file.write("-" * 80 + "\n")

        # Загальна статистика
        if dept_data.get('general_stats') and 'data' in dept_data['general_stats']:
            stats = dept_data['general_stats']['data']
            file.write("Загальна статистика:\n")
            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    if isinstance(value, float):
                        file.write(f"  • {key}: {value:.2f}\n")
                    else:
                        file.write(f"  • {key}: {value:,}\n")
                else:
                    file.write(f"  • {key}: {value}\n")
            file.write("\n")

        # Підсумки по періодах
        if dept_data.get('period_summary') and 'data' in dept_data['period_summary']:
            periods = dept_data['period_summary']['data']
            file.write("Аналіз по періодах:\n")

            for period, stats in periods.items():
                file.write(f"  • {period}: ")
                file.write(f"{stats.get('total_deliveries', 0):,} доставок, ")
                file.write(f"{stats.get('total_departments', 0)} відділень, ")
                file.write(f"час {stats.get('avg_processing_time', 0):.1f} год\n")
            file.write("\n")

        # Топ завантажені відділення
        if dept_data.get('top_busy') and 'data' in dept_data['top_busy']:
            busy_data = dept_data['top_busy']['data']
            file.write("Найзавантаженіші відділення по періодах:\n")

            for period, departments in busy_data.items():
                file.write(f"  {period}:\n")
                sorted_depts = sorted(departments.items(),
                                    key=lambda x: x[1].get('period_workload_score', 0),
                                    reverse=True)[:5]

                for i, (dept_id, stats) in enumerate(sorted_depts, 1):
                    file.write(f"    {i}. Відділення ID {stats.get('department_id', dept_id)}: ")
                    file.write(f"рейтинг {stats.get('period_workload_score', 0):.1f}\n")
            file.write("\n")

    def _write_processing_detailed_section(self, file, proc_data):
        """Записує детальну секцію аналізу часу обробки"""
        file.write("\n⏱️ ДЕТАЛЬНИЙ АНАЛІЗ ЧАСУ ОБРОБКИ\n")
        file.write("-" * 80 + "\n")

        # Загальна статистика
        if proc_data.get('general_stats') and 'data' in proc_data['general_stats']:
            stats = proc_data['general_stats']['data']
            file.write("Статистика часу обробки:\n")
            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    if 'time' in key.lower():
                        file.write(f"  • {key}: {value:.2f} год\n")
                    else:
                        file.write(f"  • {key}: {value:,}\n")
                else:
                    file.write(f"  • {key}: {value}\n")
            file.write("\n")

        # Порівняння періодів
        if proc_data.get('period_comparison') and 'data' in proc_data['period_comparison']:
            periods = proc_data['period_comparison']['data']
            file.write("Порівняння по періодах:\n")

            for period, stats in periods.items():
                file.write(f"  • {period}: ")
                file.write(f"середній час {stats.get('avg_processing_time', 0):.1f} год, ")
                file.write(f"{stats.get('total_deliveries', 0):,} доставок\n")
            file.write("\n")

        # Аналіз змін
        if proc_data.get('period_changes') and 'data' in proc_data['period_changes']:
            changes = proc_data['period_changes']['data']
            file.write("Зміни між періодами:\n")

            for change_key, change_data in changes.items():
                if isinstance(change_data, dict):
                    file.write(f"  • {change_key}:\n")
                    for metric, value in change_data.items():
                        if isinstance(value, (int, float)):
                            file.write(f"    - {metric}: {value:.2f}\n")
            file.write("\n")

    def _write_transport_detailed_section(self, file, transport_data):
        """Записує детальну секцію аналізу транспорту"""
        file.write("\n🚛 ДЕТАЛЬНИЙ АНАЛІЗ ТРАНСПОРТУ\n")
        file.write("-" * 80 + "\n")

        # Загальна статистика
        if transport_data.get('general_stats') and 'data' in transport_data['general_stats']:
            stats = transport_data['general_stats']['data']
            file.write("Загальна статистика транспорту:\n")
            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    file.write(f"  • {key}: {value:,}\n")
                else:
                    file.write(f"  • {key}: {value}\n")
            file.write("\n")

        # Використання по періодах
        if transport_data.get('period_usage') and 'data' in transport_data['period_usage']:
            usage_data = transport_data['period_usage']['data']
            file.write("Використання транспорту:\n")

            # Групуємо по типах транспорту
            transport_usage = {}
            for key, value in usage_data.items():
                transport_type = value.get('transport_type_name', 'Невідомий')
                if transport_type not in transport_usage:
                    transport_usage[transport_type] = 0
                transport_usage[transport_type] += value.get('total_deliveries', 0)

            sorted_transport = sorted(transport_usage.items(), key=lambda x: x[1], reverse=True)[:10]
            for i, (transport, deliveries) in enumerate(sorted_transport, 1):
                file.write(f"  {i:2d}. {transport[:40]:<40}: {deliveries:>8,} доставок\n")
            file.write("\n")

        # Ефективність транспорту
        if transport_data.get('efficiency') and 'data' in transport_data['efficiency']:
            efficiency_data = transport_data['efficiency']['data']
            file.write("Ефективність транспорту:\n")

            # Групуємо по типах та рахуємо середню ефективність
            transport_efficiency = {}
            for key, value in efficiency_data.items():
                transport_type = value.get('transport_type_name', 'Невідомий')
                if transport_type not in transport_efficiency:
                    transport_efficiency[transport_type] = []
                transport_efficiency[transport_type].append(value.get('efficiency_ratio', 0))

            avg_efficiency = {transport: np.mean(ratios) for transport, ratios in transport_efficiency.items()}
            sorted_efficiency = sorted(avg_efficiency.items(), key=lambda x: x[1], reverse=True)[:10]

            for i, (transport, efficiency) in enumerate(sorted_efficiency, 1):
                file.write(f"  {i:2d}. {transport[:40]:<40}: {efficiency:>6.2f} коеф.\n")
            file.write("\n")

    def generate_performance_report(self):
        """Генерує звіт по продуктивності"""
        print("🎯 Генерація звіту по продуктивності...")

        data = self.get_all_analysis_data()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(self.config.REPORTS_PATH, f'performance_report_{timestamp}.txt')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ЗВІТ ПО ПРОДУКТИВНОСТІ ПОШТОВОЇ СЛУЖБИ\n")
            f.write("="*80 + "\n")
            f.write(f"Дата створення: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n")

            # KPI кур'єрів
            f.write("🚚 KPI КУР'ЄРІВ:\n")
            if data['courier'].get('general_stats') and 'data' in data['courier']['general_stats']:
                stats = data['courier']['general_stats']['data']
                total_couriers = stats.get('total_couriers', 0)
                total_deliveries = stats.get('total_deliveries', 0)

                if total_couriers > 0:
                    avg_deliveries_per_courier = total_deliveries / total_couriers
                    f.write(f"   • Середня кількість доставок на кур'єра: {avg_deliveries_per_courier:.1f}\n")

                if stats.get('avg_delivery_time'):
                    f.write(f"   • Середній час доставки: {stats['avg_delivery_time']:.1f} хв\n")
            f.write("\n")

            # KPI відділень
            f.write("🏢 KPI ВІДДІЛЕНЬ:\n")
            if data['department'].get('general_stats') and 'data' in data['department']['general_stats']:
                stats = data['department']['general_stats']['data']
                total_departments = stats.get('total_departments', 0)
                total_deliveries = stats.get('total_deliveries', 0)

                if total_departments > 0:
                    avg_deliveries_per_dept = total_deliveries / total_departments
                    f.write(f"   • Середня кількість доставок на відділення: {avg_deliveries_per_dept:.1f}\n")

                if stats.get('avg_processing_time'):
                    f.write(f"   • Середній час обробки: {stats['avg_processing_time']:.1f} год\n")
            f.write("\n")

            # Ефективність транспорту
            f.write("🚛 ЕФЕКТИВНІСТЬ ТРАНСПОРТУ:\n")
            if data['transport'].get('general_stats') and 'data' in data['transport']['general_stats']:
                stats = data['transport']['general_stats']['data']
                f.write(f"   • Типів транспорту використовується: {stats.get('total_transport_types', 0)}\n")
                f.write(f"   • Всього доставок транспортом: {stats.get('total_deliveries', 0):,}\n")
            f.write("\n")

        print(f"✅ Звіт по продуктивності збережено: {os.path.basename(report_path)}")
        return report_path

    def generate_all_reports(self):
        """Генерує всі звіти"""
        print("📋 Генерація всіх звітів...")

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

        try:
            performance_report = self.generate_performance_report()
            reports.append(performance_report)
        except Exception as e:
            print(f"❌ Помилка генерації звіту по продуктивності: {e}")

        print(f"✅ Згенеровано {len(reports)} звітів")
        return reports

    def generate_comparison_report(self):
        """Генерує порівняльний звіт між періодами"""
        print("📊 Генерація порівняльного звіту...")

        data = self.get_all_analysis_data()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(self.config.REPORTS_PATH, f'comparison_report_{timestamp}.txt')

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("ПОРІВНЯЛЬНИЙ ЗВІТ ПО ПЕРІОДАХ\n")
            f.write("="*80 + "\n")
            f.write(f"Дата створення: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n")

            # Порівняння відділень по періодах
            if data['department'].get('period_comparison') and 'data' in data['department']['period_comparison']:
                periods = data['department']['period_comparison']['data']
                f.write("📈 ДИНАМІКА ВІДДІЛЕНЬ:\n")

                for period, stats in periods.items():
                    f.write(f"   • {period}:\n")
                    f.write(f"     - Доставок: {stats.get('total_deliveries', 0):,}\n")
                    f.write(f"     - Відділень: {stats.get('active_departments', 0)}\n")
                    f.write(f"     - Час обробки: {stats.get('avg_processing_time', 0):.1f} год\n")
                f.write("\n")

            # Порівняння часу обробки
            if data['processing_time'].get('period_comparison') and 'data' in data['processing_time']['period_comparison']:
                periods = data['processing_time']['period_comparison']['data']
                f.write("⏱️ ДИНАМІКА ЧАСУ ОБРОБКИ:\n")

                for period, stats in periods.items():
                    f.write(f"   • {period}: {stats.get('avg_processing_time', 0):.1f} год ")
                    f.write(f"({stats.get('total_deliveries', 0):,} доставок)\n")
                f.write("\n")

        print(f"✅ Порівняльний звіт збережено: {os.path.basename(report_path)}")
        return report_path