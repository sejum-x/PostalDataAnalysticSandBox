"""
Генератор графіків для аналізу Data Warehouse
Працює з новою структурою даних
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import json
import os
import sys
import glob
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')
plt.style.use('default')
sns.set_palette("husl")

sys.path.append('..')
from config.database_config import DatabaseConfig

class DWChartGenerator:
    def __init__(self):
        self.config = DatabaseConfig()
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10

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

    def create_courier_performance_charts(self):
        """Створює графіки продуктивності кур'єрів"""
        files = self.get_latest_analysis_files()

        if not files['courier']:
            print("❌ Файл аналізу кур'єрів не знайдено")
            return

        data = self.load_analysis_data(files['courier'])
        if not data:
            return

        print("📈 Створення графіків кур'єрів...")

        # Топ кур'єри за ефективністю
        if 'top_couriers' in data:
            top_couriers = data['top_couriers']

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

            # Графік ефективності
            courier_names = [f"Кур'єр {i+1}" for i in range(len(top_couriers))]
            efficiency_scores = [courier['efficiency_score'] for courier in top_couriers.values()]

            ax1.bar(courier_names, efficiency_scores, color='skyblue')
            ax1.set_title('Топ-10 кур\'єрів за ефективністю')
            ax1.set_ylabel('Рейтинг ефективності')
            ax1.tick_params(axis='x', rotation=45)

            # Графік часу доставки
            delivery_times = [courier['avg_delivery_time'] for courier in top_couriers.values()]

            ax2.bar(courier_names, delivery_times, color='lightcoral')
            ax2.set_title('Середній час доставки топ кур\'єрів')
            ax2.set_ylabel('Час доставки (хв)')
            ax2.tick_params(axis='x', rotation=45)

            plt.tight_layout()

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.config.CHARTS_PATH, f'courier_performance_{timestamp}.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✅ Графік кур'єрів збережено: {os.path.basename(chart_path)}")

    def create_department_workload_charts(self):
        """Створює графіки завантаження відділень"""
        files = self.get_latest_analysis_files()

        if not files['department']:
            print("❌ Файл аналізу відділень не знайдено")
            return

        data = self.load_analysis_data(files['department'])
        if not data:
            return

        print("📊 Створення графіків відділень...")

        # Аналіз по регіонах
        if 'region_analysis' in data:
            regions = data['region_analysis']

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

            region_names = list(regions.keys())[:10]  # Топ 10 регіонів
            deliveries = [regions[region]['total_deliveries'] for region in region_names]
            processing_times = [regions[region]['avg_processing_time'] for region in region_names]

            # Кількість доставок по регіонах
            ax1.bar(region_names, deliveries, color='lightgreen')
            ax1.set_title('Кількість доставок по регіонах')
            ax1.set_ylabel('Кількість доставок')
            ax1.tick_params(axis='x', rotation=45)

            # Час обробки по регіонах
            ax2.bar(region_names, processing_times, color='orange')
            ax2.set_title('Середній час обробки по регіонах')
            ax2.set_ylabel('Час обробки (год)')
            ax2.tick_params(axis='x', rotation=45)

            plt.tight_layout()

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.config.CHARTS_PATH, f'department_workload_{timestamp}.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✅ Графік відділень збережено: {os.path.basename(chart_path)}")

    def create_transport_utilization_charts(self):
        """Створює графіки використання транспорту"""
        files = self.get_latest_analysis_files()

        if not files['transport']:
            print("❌ Файл аналізу транспорту не знайдено")
            return

        data = self.load_analysis_data(files['transport'])
        if not data:
            return

        print("🚛 Створення графіків транспорту...")

        # Використання транспорту
        if 'transport_utilization' in data:
            transport_data = data['transport_utilization']

            fig, ax = plt.subplots(figsize=(12, 8))

            transport_names = list(transport_data.keys())[:8]  # Топ 8 типів
            utilization_scores = [transport_data[transport]['utilization_score'] for transport in transport_names]

            # Створюємо горизонтальний bar chart
            y_pos = range(len(transport_names))
            ax.barh(y_pos, utilization_scores, color='purple', alpha=0.7)
            ax.set_yticks(y_pos)
            ax.set_yticklabels([name.replace('_', ' ').title() for name in transport_names])
            ax.set_xlabel('Рейтинг використання')
            ax.set_title('Використання типів транспорту')

            plt.tight_layout()

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.config.CHARTS_PATH, f'transport_utilization_{timestamp}.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✅ Графік транспорту збережено: {os.path.basename(chart_path)}")

    def create_all_charts(self):
        """Створює всі графіки"""
        print("🎨 Створення всіх графіків...")

        charts_created = []

        try:
            self.create_courier_performance_charts()
            charts_created.append("Кур'єри")
        except Exception as e:
            print(f"❌ Помилка створення графіків кур'єрів: {e}")

        try:
            self.create_department_workload_charts()
            charts_created.append("Відділення")
        except Exception as e:
            print(f"❌ Помилка створення графіків відділень: {e}")

        try:
            self.create_transport_utilization_charts()
            charts_created.append("Транспорт")
        except Exception as e:
            print(f"❌ Помилка створення графіків транспорту: {e}")

        print(f"✅ Створено графіки для: {', '.join(charts_created)}")
        return charts_created