"""
Генератор графіків для аналізу Data Warehouse
Працює з новою структурою окремих JSON файлів
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
import numpy as np
import traceback

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

        # Створюємо директорію для графіків якщо не існує
        os.makedirs(self.config.CHARTS_PATH, exist_ok=True)
        print(f"📁 Директорія графіків: {self.config.CHARTS_PATH}")
        print(f"📁 Директорія даних: {self.config.PROCESSED_DATA_PATH}")

    def get_latest_files_by_pattern(self, pattern):
        """Отримує найновіші файли за патерном"""
        file_pattern = os.path.join(self.config.PROCESSED_DATA_PATH, pattern)
        print(f"🔍 Шукаємо файли за патерном: {file_pattern}")

        matching_files = glob.glob(file_pattern)
        print(f"📋 Знайдено файлів: {len(matching_files)}")

        if matching_files:
            latest_file = max(matching_files, key=os.path.getctime)
            print(f"📄 Найновіший файл: {latest_file}")
            return latest_file
        else:
            print(f"❌ Файли не знайдено за патерном: {pattern}")
            return None

    def load_json_data(self, filepath):
        """Завантажує дані з JSON файлу"""
        try:
            if not filepath:
                print("❌ Шлях до файлу не вказано")
                return None

            if not os.path.exists(filepath):
                print(f"❌ Файл не існує: {filepath}")
                return None

            print(f"📖 Завантажуємо файл: {os.path.basename(filepath)}")

            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if isinstance(data, dict) and 'data' in data:
                data_size = len(data['data']) if data['data'] else 0
                print(f"✅ Завантажено записів: {data_size}")
            else:
                print(f"⚠️ Структура даних: {type(data)}")

            return data

        except json.JSONDecodeError as e:
            print(f"❌ Помилка JSON в файлі {filepath}: {e}")
            return None
        except Exception as e:
            print(f"❌ Помилка завантаження {filepath}: {e}")
            return None

    def create_courier_performance_charts(self):
        """Створює графіки продуктивності кур'єрів"""
        print("📈 Створення графіків кур'єрів...")
        print("=" * 50)

        try:
            # Завантажуємо дані топ кур'єрів
            print("🔍 Пошук файлів кур'єрів...")
            top_couriers_file = self.get_latest_files_by_pattern('courier_top_performers_*.json')
            general_stats_file = self.get_latest_files_by_pattern('courier_general_stats_*.json')
            region_analysis_file = self.get_latest_files_by_pattern('courier_region_analysis_*.json')

            if not top_couriers_file:
                print("❌ Файл топ кур'єрів не знайдено")
                # Перевіримо які файли кур'єрів взагалі є
                all_courier_files = glob.glob(os.path.join(self.config.PROCESSED_DATA_PATH, 'courier_*.json'))
                print(f"📋 Всі файли кур'єрів: {[os.path.basename(f) for f in all_courier_files]}")
                return False

            print("📖 Завантаження даних кур'єрів...")
            top_couriers_data = self.load_json_data(top_couriers_file)
            general_stats_data = self.load_json_data(general_stats_file) if general_stats_file else None
            region_data = self.load_json_data(region_analysis_file) if region_analysis_file else None

            if not top_couriers_data:
                print("❌ Не вдалося завантажити дані топ кур'єрів")
                return False

            if 'data' not in top_couriers_data:
                print(f"❌ Немає секції 'data' в файлі. Ключі: {list(top_couriers_data.keys())}")
                return False

            if not top_couriers_data['data']:
                print("❌ Секція 'data' порожня")
                return False

            print(f"✅ Завантажено даних кур'єрів: {len(top_couriers_data['data'])}")

            # Аналізуємо структуру даних
            first_courier_key = list(top_couriers_data['data'].keys())[0]
            first_courier_data = top_couriers_data['data'][first_courier_key]
            print(f"📊 Структура даних кур'єра: {list(first_courier_data.keys())}")

            # Створюємо фігуру з підграфіками
            print("🎨 Створення фігури...")
            fig = plt.figure(figsize=(20, 12))

            # 1. Топ кур'єри за ефективністю
            print("📈 Створення графіка ефективності...")
            ax1 = plt.subplot(2, 3, 1)

            # Беремо топ 10 кур'єрів
            couriers = list(top_couriers_data['data'].items())[:10]
            print(f"👥 Обробляємо {len(couriers)} кур'єрів")

            courier_names = []
            efficiency_scores = []

            for i, (key, courier) in enumerate(couriers):
                courier_id = courier.get('courier_id', i+1)
                efficiency = courier.get('efficiency_score', 0)

                courier_names.append(f"ID {courier_id}")
                efficiency_scores.append(efficiency)

                print(f"  Кур'єр {courier_id}: ефективність = {efficiency}")

            if not any(efficiency_scores):
                print("❌ Всі показники ефективності = 0")
                return False

            print(f"📊 Діапазон ефективності: {min(efficiency_scores)} - {max(efficiency_scores)}")

            bars = ax1.bar(range(len(courier_names)), efficiency_scores, color='skyblue', alpha=0.8)
            ax1.set_title('Топ-10 кур\'єрів за ефективністю', fontsize=12, fontweight='bold')
            ax1.set_ylabel('Рейтинг ефективності')
            ax1.set_xlabel('Кур\'єри')
            ax1.set_xticks(range(len(courier_names)))
            ax1.set_xticklabels(courier_names, rotation=45)

            # Додаємо значення на стовпці
            for bar, score in zip(bars, efficiency_scores):
                if score > 0:
                    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                            f'{score:.1f}', ha='center', va='bottom', fontsize=8)

            # 2. Середній час доставки
            print("⏱️ Створення графіка часу доставки...")
            ax2 = plt.subplot(2, 3, 2)
            delivery_times = []

            for key, courier in couriers:
                delivery_time = courier.get('avg_delivery_time', 0)
                delivery_times.append(delivery_time)
                print(f"  Кур'єр {courier.get('courier_id', 'N/A')}: час доставки = {delivery_time}")

            print(f"📊 Діапазон часу доставки: {min(delivery_times)} - {max(delivery_times)}")

            bars2 = ax2.bar(range(len(courier_names)), delivery_times, color='lightcoral', alpha=0.8)
            ax2.set_title('Середній час доставки топ кур\'єрів', fontsize=12, fontweight='bold')
            ax2.set_ylabel('Час доставки (хв)')
            ax2.set_xlabel('Кур\'єри')
            ax2.set_xticks(range(len(courier_names)))
            ax2.set_xticklabels(courier_names, rotation=45)

            for bar, time in zip(bars2, delivery_times):
                if time > 0:
                    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(delivery_times)*0.01,
                            f'{time:.0f}', ha='center', va='bottom', fontsize=8)

            # 3. Кількість доставок
            print("📦 Створення графіка кількості доставок...")
            ax3 = plt.subplot(2, 3, 3)
            total_deliveries = []

            for key, courier in couriers:
                deliveries = courier.get('total_deliveries', 0)
                total_deliveries.append(deliveries)
                print(f"  Кур'єр {courier.get('courier_id', 'N/A')}: доставок = {deliveries}")

            print(f"📊 Діапазон доставок: {min(total_deliveries)} - {max(total_deliveries)}")

            bars3 = ax3.bar(range(len(courier_names)), total_deliveries, color='lightgreen', alpha=0.8)
            ax3.set_title('Кількість доставок топ кур\'єрів', fontsize=12, fontweight='bold')
            ax3.set_ylabel('Кількість доставок')
            ax3.set_xlabel('Кур\'єри')
            ax3.set_xticks(range(len(courier_names)))
            ax3.set_xticklabels(courier_names, rotation=45)

            for bar, count in zip(bars3, total_deliveries):
                if count > 0:
                    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(total_deliveries)*0.01,
                            f'{count}', ha='center', va='bottom', fontsize=8)

            # 4. Загальна статистика (якщо є)
            if general_stats_data and 'data' in general_stats_data:
                print("📊 Створення графіка загальної статистики...")
                ax4 = plt.subplot(2, 3, 4)
                stats = general_stats_data['data']

                print(f"📈 Загальна статистика: {stats}")

                labels = ['Всього кур\'єрів', 'Всього доставок', 'Регіонів', 'Міст']
                values = [
                    stats.get('total_couriers', 0),
                    stats.get('total_deliveries', 0),
                    stats.get('total_regions', 0),
                    stats.get('total_cities', 0)
                ]

                colors = ['#FF9999', '#66B2FF', '#99FF99', '#FFCC99']
                bars4 = ax4.bar(labels, values, color=colors)
                ax4.set_title('Загальна статистика кур\'єрів', fontsize=12, fontweight='bold')
                ax4.set_ylabel('Кількість')
                ax4.tick_params(axis='x', rotation=45)

                for bar, value in zip(bars4, values):
                    if value > 0:
                        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                                f'{value}', ha='center', va='bottom', fontsize=9)

            # 5. Аналіз по регіонах (якщо є)
            if region_data and 'data' in region_data and region_data['data']:
                print("🗺️ Створення графіка по регіонах...")
                ax5 = plt.subplot(2, 3, 5)
                regions = list(region_data['data'].items())[:8]  # Топ 8 регіонів

                print(f"🌍 Регіонів для відображення: {len(regions)}")

                if regions:
                    region_names = [region[0].replace('_', ' ')[:15] for region in regions]
                    region_deliveries = [region[1].get('total_deliveries', 0) for region in regions]

                    for name, deliveries in zip(region_names, region_deliveries):
                        print(f"  Регіон {name}: {deliveries} доставок")

                    bars5 = ax5.barh(range(len(region_names)), region_deliveries, color='purple', alpha=0.7)
                    ax5.set_title('Доставки по регіонах', fontsize=12, fontweight='bold')
                    ax5.set_xlabel('Кількість доставок')
                    ax5.set_yticks(range(len(region_names)))
                    ax5.set_yticklabels(region_names)

                    for bar, value in zip(bars5, region_deliveries):
                        if value > 0:
                            ax5.text(bar.get_width() + max(region_deliveries)*0.01,
                                    bar.get_y() + bar.get_height()/2,
                                    f'{value}', ha='left', va='center', fontsize=8)

            # 6. Розподіл часу доставки (гістограма)
            print("📊 Створення гістограми часу доставки...")
            ax6 = plt.subplot(2, 3, 6)
            all_delivery_times = []

            for courier in top_couriers_data['data'].values():
                delivery_time = courier.get('avg_delivery_time', 0)
                if delivery_time > 0:
                    all_delivery_times.append(delivery_time)

            print(f"⏱️ Часів доставки для гістограми: {len(all_delivery_times)}")

            if all_delivery_times and len(all_delivery_times) > 1:
                bins_count = min(15, len(all_delivery_times))
                print(f"📊 Кількість bins: {bins_count}")

                ax6.hist(all_delivery_times, bins=bins_count,
                        color='orange', alpha=0.7, edgecolor='black')
                ax6.set_title('Розподіл часу доставки', fontsize=12, fontweight='bold')
                ax6.set_xlabel('Час доставки (хв)')
                ax6.set_ylabel('Кількість кур\'єрів')

                mean_time = np.mean(all_delivery_times)
                ax6.axvline(mean_time, color='red', linestyle='--',
                           label=f'Середнє: {mean_time:.1f} хв')
                ax6.legend()

                print(f"📊 Середній час доставки: {mean_time:.1f} хв")

            print("🎨 Налаштування макету...")
            plt.tight_layout()

            print("💾 Збереження графіка...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.config.CHARTS_PATH, f'courier_performance_{timestamp}.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✅ Графік кур'єрів збережено: {os.path.basename(chart_path)}")
            return True

        except Exception as e:
            print(f"❌ Помилка створення графіків кур'єрів: {e}")
            print(f"🔍 Детальна помилка: {traceback.format_exc()}")
            plt.close('all')  # Закриваємо всі відкриті фігури
            return False

    def create_processing_time_charts(self):
        """Створює графіки часу обробки"""
        print("⏱️ Створення графіків часу обробки...")
        print("=" * 50)

        try:
            print("🔍 Пошук файлів часу обробки...")
            general_stats_file = self.get_latest_files_by_pattern('processing_time_general_stats_*.json')
            trends_file = self.get_latest_files_by_pattern('processing_time_trends_*.json')
            period_comparison_file = self.get_latest_files_by_pattern('processing_time_period_comparison_*.json')
            region_analysis_file = self.get_latest_files_by_pattern('processing_time_region_analysis_*.json')

            if not general_stats_file:
                print("❌ Файли аналізу часу обробки не знайдено")
                # Перевіримо які файли часу обробки взагалі є
                all_processing_files = glob.glob(os.path.join(self.config.PROCESSED_DATA_PATH, 'processing_time_*.json'))
                print(f"📋 Всі файли часу обробки: {[os.path.basename(f) for f in all_processing_files]}")
                return False

            print("📖 Завантаження даних часу обробки...")
            general_data = self.load_json_data(general_stats_file)
            trends_data = self.load_json_data(trends_file) if trends_file else None
            comparison_data = self.load_json_data(period_comparison_file) if period_comparison_file else None
            region_data = self.load_json_data(region_analysis_file) if region_analysis_file else None

            if not general_data:
                print("❌ Не вдалося завантажити загальні дані часу обробки")
                return False

            if 'data' not in general_data:
                print(f"❌ Немає секції 'data' в загальних даних. Ключі: {list(general_data.keys())}")
                return False

            print(f"✅ Завантажено загальних даних: {general_data['data']}")

            # Аналізуємо дані
            if trends_data and 'data' in trends_data:
                print(f"📊 Тренди: {len(trends_data['data'])} записів")
            if comparison_data and 'data' in comparison_data:
                print(f"📊 Порівняння періодів: {len(comparison_data['data'])} записів")
            if region_data and 'data' in region_data:
                print(f"📊 Регіональні дані: {len(region_data['data'])} записів")

            print("🎨 Створення фігури...")
            fig = plt.figure(figsize=(20, 12))

            # 1. Загальна статистика часу обробки
            print("📊 Створення графіка загальної статистики...")
            ax1 = plt.subplot(2, 3, 1)
            stats = general_data['data']

            labels = ['Середній\nчас', 'Медіанний\nчас', 'Мін.\nчас', 'Макс.\nчас']
            values = [
                stats.get('avg_processing_time', 0),
                stats.get('median_processing_time', 0),
                stats.get('min_processing_time', 0),
                stats.get('max_processing_time', 0)
            ]

            print(f"📈 Статистика часу: {dict(zip(labels, values))}")

            colors = ['#FF7675', '#74B9FF', '#00B894', '#FDCB6E']
            bars = ax1.bar(labels, values, color=colors, alpha=0.8)
            ax1.set_title('Статистика часу обробки', fontsize=12, fontweight='bold')
            ax1.set_ylabel('Час (години)')

            for bar, value in zip(bars, values):
                if value > 0:
                    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                            f'{value:.1f}', ha='center', va='bottom', fontsize=9)

            # 2. Тренди по типах посилок
            if trends_data and 'data' in trends_data and trends_data['data']:
                print("📈 Створення графіка трендів...")
                ax2 = plt.subplot(2, 3, 2)

                # Групуємо по типах посилок
                parcel_trends = {}
                for key, value in trends_data['data'].items():
                    parcel_type = value.get('parcel_type_name', 'Невідомий')
                    period = value.get('period', key.split('_')[0] if '_' in key else 'Unknown')

                    if parcel_type not in parcel_trends:
                        parcel_trends[parcel_type] = {'periods': [], 'times': []}

                    parcel_trends[parcel_type]['periods'].append(period)
                    parcel_trends[parcel_type]['times'].append(value.get('avg_processing_time', 0))

                print(f"📊 Типів посилок для трендів: {len(parcel_trends)}")

                if parcel_trends:
                    colors = plt.cm.tab10(np.linspace(0, 1, len(parcel_trends)))
                    plotted = 0
                    for i, (parcel_type, data) in enumerate(list(parcel_trends.items())[:5]):  # Топ 5
                        if data['periods'] and data['times'] and any(data['times']):
                            print(f"  📈 {parcel_type}: {len(data['periods'])} періодів")
                            ax2.plot(data['periods'], data['times'], marker='o',
                                    label=parcel_type[:15], color=colors[i], linewidth=2)
                            plotted += 1

                    if plotted > 0:
                        ax2.set_title('Тренди часу обробки по типах посилок', fontsize=12, fontweight='bold')
                        ax2.set_ylabel('Час обробки (год)')
                        ax2.set_xlabel('Період')
                        ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
                        ax2.tick_params(axis='x', rotation=45)
                        ax2.grid(True, alpha=0.3)

            # 3. Порівняння періодів
            if comparison_data and 'data' in comparison_data and comparison_data['data']:
                print("📊 Створення графіка порівняння періодів...")
                ax3 = plt.subplot(2, 3, 3)
                periods = list(comparison_data['data'].items())

                print(f"📅 Періодів для порівняння: {len(periods)}")

                if periods:
                    period_names = [period[0] for period in periods]
                    avg_times = [period[1].get('avg_processing_time', 0) for period in periods]

                    for name, time in zip(period_names, avg_times):
                        print(f"  📅 {name}: {time:.1f} год")

                    if any(avg_times):
                        bars = ax3.bar(range(len(period_names)), avg_times, color='#A29BFE', alpha=0.8)
                        ax3.set_title('Середній час обробки по періодах', fontsize=12, fontweight='bold')
                        ax3.set_ylabel('Час обробки (год)')
                        ax3.set_xlabel('Період')
                        ax3.set_xticks(range(len(period_names)))
                        ax3.set_xticklabels(period_names, rotation=45)

                        for bar, time in zip(bars, avg_times):
                            if time > 0:
                                ax3.text(bar.get_x() + bar.get_width()/2,
                                        bar.get_height() + max(avg_times)*0.01,
                                        f'{time:.1f}', ha='center', va='bottom', fontsize=8)

            # Продовжуємо з іншими графіками...
            print("🎨 Налаштування макету...")
            plt.tight_layout()

            print("💾 Збереження графіка...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.config.CHARTS_PATH, f'processing_time_analysis_{timestamp}.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✅ Графік часу обробки збережено: {os.path.basename(chart_path)}")
            return True

        except Exception as e:
            print(f"❌ Помилка створення графіків часу обробки: {e}")
            print(f"🔍 Детальна помилка: {traceback.format_exc()}")
            plt.close('all')
            return False

    # Інші методи залишаються без змін...
    def create_department_workload_charts(self):
        """Створює графіки завантаження відділень"""
        print("📊 Створення графіків відділень...")

        try:
            # Завантажуємо різні типи аналізів відділень
            general_stats_file = self.get_latest_files_by_pattern('department_general_stats_*.json')
            period_summary_file = self.get_latest_files_by_pattern('department_period_summary_*.json')
            region_analysis_file = self.get_latest_files_by_pattern('department_region_analysis_*.json')
            type_analysis_file = self.get_latest_files_by_pattern('department_type_analysis_*.json')
            top_busy_file = self.get_latest_files_by_pattern('department_top_busy_*.json')

            if not general_stats_file:
                print("❌ Файли аналізу відділень не знайдено")
                return False

            general_data = self.load_json_data(general_stats_file)
            period_data = self.load_json_data(period_summary_file) if period_summary_file else None
            region_data = self.load_json_data(region_analysis_file) if region_analysis_file else None
            type_data = self.load_json_data(type_analysis_file) if type_analysis_file else None
            busy_data = self.load_json_data(top_busy_file) if top_busy_file else None

            if not general_data or 'data' not in general_data:
                print("❌ Немає даних про відділення")
                return False

            fig = plt.figure(figsize=(20, 12))

            # 1. Загальна статистика відділень
            ax1 = plt.subplot(2, 3, 1)
            stats = general_data['data']

            labels = ['Відділення', 'Доставки', 'Регіони', 'Міста', 'Періоди']
            values = [
                stats.get('total_departments', 0),
                stats.get('total_deliveries', 0),
                stats.get('total_regions', 0),
                stats.get('total_cities', 0),
                stats.get('total_periods', 0)
            ]

            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
            bars = ax1.bar(labels, values, color=colors, alpha=0.8)
            ax1.set_title('Загальна статистика відділень', fontsize=12, fontweight='bold')
            ax1.set_ylabel('Кількість')
            ax1.tick_params(axis='x', rotation=45)

            for bar, value in zip(bars, values):
                if value > 0:
                    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                            f'{value:,}', ha='center', va='bottom', fontsize=9)

            # 2. Доставки по періодах
            if period_data and 'data' in period_data and period_data['data']:
                ax2 = plt.subplot(2, 3, 2)
                periods = list(period_data['data'].items())

                if periods:
                    period_names = [period[0] for period in periods]
                    period_deliveries = [period[1].get('total_deliveries', 0) for period in periods]

                    ax2.plot(period_names, period_deliveries, marker='o', linewidth=2, markersize=6, color='#E17055')
                    ax2.set_title('Динаміка доставок по періодах', fontsize=12, fontweight='bold')
                    ax2.set_ylabel('Кількість доставок')
                    ax2.set_xlabel('Період')
                    ax2.tick_params(axis='x', rotation=45)
                    ax2.grid(True, alpha=0.3)

            # 3. Аналіз по регіонах
            if region_data and 'data' in region_data and region_data['data']:
                ax3 = plt.subplot(2, 3, 3)
                regions = list(region_data['data'].items())[:10]

                if regions:
                    region_names = [region[0].replace('_', ' ')[:15] for region in regions]
                    region_deliveries = [region[1].get('total_deliveries', 0) for region in regions]

                    bars = ax3.barh(range(len(region_names)), region_deliveries, color='#74B9FF', alpha=0.8)
                    ax3.set_title('Доставки по регіонах', fontsize=12, fontweight='bold')
                    ax3.set_xlabel('Кількість доставок')
                    ax3.set_yticks(range(len(region_names)))
                    ax3.set_yticklabels(region_names)

                    for bar, value in zip(bars, region_deliveries):
                        if value > 0:
                            ax3.text(bar.get_width() + max(region_deliveries)*0.01,
                                    bar.get_y() + bar.get_height()/2,
                                    f'{value:,}', ha='left', va='center', fontsize=8)

            # 4. Аналіз по типах відділень
            if type_data and 'data' in type_data and type_data['data']:
                ax4 = plt.subplot(2, 3, 4)

                # Групуємо по типах відділень
                dept_types = {}
                for key, value in type_data['data'].items():
                    dept_type = value.get('department_type', 'Невідомий')
                    if dept_type not in dept_types:
                        dept_types[dept_type] = 0
                    dept_types[dept_type] += value.get('total_deliveries', 0)

                if dept_types:
                    labels = list(dept_types.keys())
                    sizes = list(dept_types.values())
                    colors = plt.cm.Set3(np.linspace(0, 1, len(labels)))

                    wedges, texts, autotexts = ax4.pie(sizes, labels=labels, autopct='%1.1f%%',
                                                      colors=colors, startangle=90)
                    ax4.set_title('Розподіл доставок по типах відділень', fontsize=12, fontweight='bold')

            # 5. Час обробки по регіонах
            if region_data and 'data' in region_data and region_data['data']:
                ax5 = plt.subplot(2, 3, 5)
                regions = list(region_data['data'].items())[:8]

                if regions:
                    region_names = [region[0].replace('_', ' ')[:12] for region in regions]
                    processing_times = [region[1].get('avg_processing_time', 0) for region in regions]

                    if any(processing_times):
                        bars = ax5.bar(range(len(region_names)), processing_times, color='#FD79A8', alpha=0.8)
                        ax5.set_title('Час обробки по регіонах', fontsize=12, fontweight='bold')
                        ax5.set_ylabel('Час обробки (год)')
                        ax5.set_xlabel('Регіон')
                        ax5.set_xticks(range(len(region_names)))
                        ax5.set_xticklabels(region_names, rotation=45)

                        for bar, time in zip(bars, processing_times):
                            if time > 0:
                                ax5.text(bar.get_x() + bar.get_width()/2,
                                        bar.get_height() + max(processing_times)*0.01,
                                        f'{time:.1f}', ha='center', va='bottom', fontsize=8)

            # 6. Топ завантажені відділення (якщо є дані)
            if busy_data and 'data' in busy_data and busy_data['data']:
                ax6 = plt.subplot(2, 3, 6)

                # Беремо перший період з найбільшою кількістю даних
                first_period = list(busy_data['data'].keys())[0] if busy_data['data'] else None
                if first_period and busy_data['data'][first_period]:
                    top_depts = list(busy_data['data'][first_period].items())[:8]

                    if top_depts:
                        dept_names = [f"Відд. {dept[1].get('department_id', i+1)}" for i, dept in enumerate(top_depts)]
                        workload_scores = [dept[1].get('period_workload_score', 0) for dept in top_depts]

                        if any(workload_scores):
                            bars = ax6.barh(range(len(dept_names)), workload_scores, color='#FDCB6E', alpha=0.8)
                            ax6.set_title(f'Топ завантажені відділення ({first_period})', fontsize=12, fontweight='bold')
                            ax6.set_xlabel('Рейтинг завантаженості')
                            ax6.set_yticks(range(len(dept_names)))
                            ax6.set_yticklabels(dept_names)

                            for bar, score in zip(bars, workload_scores):
                                if score > 0:
                                    ax6.text(bar.get_width() + max(workload_scores)*0.01,
                                            bar.get_y() + bar.get_height()/2,
                                            f'{score:.1f}', ha='left', va='center', fontsize=8)

            plt.tight_layout()

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.config.CHARTS_PATH, f'department_workload_{timestamp}.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✅ Графік відділень збережено: {os.path.basename(chart_path)}")
            return True

        except Exception as e:
            print(f"❌ Помилка створення графіків відділень: {e}")
            plt.close('all')
            return False

    def create_transport_utilization_charts(self):
        """Створює розширені графіки використання транспорту"""
        print("🚛 Створення графіків транспорту...")
        print("=" * 50)

        try:
            print("🔍 Пошук файлів транспорту...")
            general_stats_file = self.get_latest_files_by_pattern('transport_general_stats_*.json')
            period_usage_file = self.get_latest_files_by_pattern('transport_period_usage_*.json')
            trends_file = self.get_latest_files_by_pattern('transport_trends_*.json')
            efficiency_file = self.get_latest_files_by_pattern('transport_efficiency_*.json')
            most_used_file = self.get_latest_files_by_pattern('transport_most_used_*.json')
            region_analysis_file = self.get_latest_files_by_pattern('transport_region_analysis_*.json')
            parcel_analysis_file = self.get_latest_files_by_pattern('transport_parcel_analysis_*.json')
            changes_file = self.get_latest_files_by_pattern('transport_changes_*.json')

            if not general_stats_file:
                print("❌ Файли аналізу транспорту не знайдено")
                all_transport_files = glob.glob(os.path.join(self.config.PROCESSED_DATA_PATH, 'transport_*.json'))
                print(f"📋 Всі файли транспорту: {[os.path.basename(f) for f in all_transport_files]}")
                return False

            print("📖 Завантаження даних транспорту...")
            general_data = self.load_json_data(general_stats_file)
            usage_data = self.load_json_data(period_usage_file) if period_usage_file else None
            trends_data = self.load_json_data(trends_file) if trends_file else None
            efficiency_data = self.load_json_data(efficiency_file) if efficiency_file else None
            most_used_data = self.load_json_data(most_used_file) if most_used_file else None
            region_data = self.load_json_data(region_analysis_file) if region_analysis_file else None
            parcel_data = self.load_json_data(parcel_analysis_file) if parcel_analysis_file else None
            changes_data = self.load_json_data(changes_file) if changes_file else None

            if not general_data or 'data' not in general_data:
                print("❌ Немає даних про транспорт")
                return False

            print(f"✅ Завантажено загальних даних: {general_data['data']}")

            # Аналізуємо дані
            if usage_data and 'data' in usage_data:
                print(f"📊 Використання по періодах: {len(usage_data['data'])} записів")
            if trends_data and 'data' in trends_data:
                print(f"📊 Тренди: {len(trends_data['data'])} записів")
            if efficiency_data and 'data' in efficiency_data:
                print(f"📊 Ефективність: {len(efficiency_data['data'])} записів")

            print("🎨 Створення розширеної фігури...")
            fig = plt.figure(figsize=(24, 16))  # Збільшуємо розмір для більше графіків

            # 1. Загальна статистика транспорту
            print("📊 Створення графіка загальної статистики...")
            ax1 = plt.subplot(3, 4, 1)
            stats = general_data['data']

            labels = ['Типи\nтранспорту', 'Всього\nдоставок', 'Відділення', 'Регіони', 'Типи\nпосилок']
            values = [
                stats.get('total_transport_types', 0),
                stats.get('total_deliveries', 0),
                stats.get('departments_using_transport', 0),
                stats.get('regions_served', 0),
                stats.get('parcel_types_transported', 0)
            ]

            print(f"📈 Загальна статистика: {dict(zip(labels, values))}")

            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
            bars = ax1.bar(labels, values, color=colors, alpha=0.8)
            ax1.set_title('Загальна статистика транспорту', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Кількість')
            ax1.tick_params(axis='x', rotation=45)

            for bar, value in zip(bars, values):
                if value > 0:
                    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(values) * 0.01,
                             f'{value:,}', ha='center', va='bottom', fontsize=8)

            # 2. Використання транспорту по періодах
            if usage_data and 'data' in usage_data and usage_data['data']:
                print("📅 Створення графіка використання по періодах...")
                ax2 = plt.subplot(3, 4, 2)

                # Групуємо по періодах
                period_usage = {}
                for key, value in usage_data['data'].items():
                    period = key.split('_')[0] if '_' in key else 'Unknown'
                    if period not in period_usage:
                        period_usage[period] = {'deliveries': 0, 'score': 0, 'count': 0}

                    period_usage[period]['deliveries'] += value.get('total_deliveries', 0)
                    period_usage[period]['score'] += value.get('period_utilization_score', 0)
                    period_usage[period]['count'] += 1

                print(f"📊 Періодів для аналізу: {len(period_usage)}")

                if period_usage:
                    periods = sorted(period_usage.keys())[:8]  # Останні 8 періодів
                    period_deliveries = [period_usage[p]['deliveries'] for p in periods]

                    for period, deliveries in zip(periods, period_deliveries):
                        print(f"  📅 {period}: {deliveries} доставок")

                    bars = ax2.bar(range(len(periods)), period_deliveries, color='#74B9FF', alpha=0.8)
                    ax2.set_title('Доставки транспортом по періодах', fontsize=11, fontweight='bold')
                    ax2.set_ylabel('Кількість доставок')
                    ax2.set_xlabel('Період')
                    ax2.set_xticks(range(len(periods)))
                    ax2.set_xticklabels(periods, rotation=45)

                    for bar, value in zip(bars, period_deliveries):
                        if value > 0:
                            ax2.text(bar.get_x() + bar.get_width() / 2,
                                     bar.get_height() + max(period_deliveries) * 0.01,
                                     f'{value:,}', ha='center', va='bottom', fontsize=8)

            # 3. Тренди використання транспорту
            if trends_data and 'data' in trends_data and trends_data['data']:
                print("📈 Створення графіка трендів...")
                ax3 = plt.subplot(3, 4, 3)

                # Групуємо тренди по типах транспорту
                transport_trends = {}
                for key, value in trends_data['data'].items():
                    parts = key.split('_')
                    if len(parts) >= 2:
                        transport_type = parts[0]
                        period = parts[1] if len(parts) > 1 else 'Unknown'

                        if transport_type not in transport_trends:
                            transport_trends[transport_type] = {'periods': [], 'deliveries': []}

                        transport_trends[transport_type]['periods'].append(period)
                        transport_trends[transport_type]['deliveries'].append(value.get('total_deliveries', 0))

                print(f"📊 Типів транспорту для трендів: {len(transport_trends)}")

                if transport_trends:
                    colors = plt.cm.tab10(np.linspace(0, 1, len(transport_trends)))
                    plotted = 0

                    for i, (transport_type, data) in enumerate(list(transport_trends.items())[:5]):
                        if data['periods'] and data['deliveries'] and any(data['deliveries']):
                            # Сортуємо по періодах
                            sorted_data = sorted(zip(data['periods'], data['deliveries']))
                            periods, deliveries = zip(*sorted_data) if sorted_data else ([], [])

                            print(f"  📈 {transport_type}: {len(periods)} періодів")
                            ax3.plot(range(len(periods)), deliveries, marker='o',
                                     label=transport_type[:12], color=colors[i], linewidth=2)
                            plotted += 1

                    if plotted > 0:
                        ax3.set_title('Тренди використання транспорту', fontsize=11, fontweight='bold')
                        ax3.set_ylabel('Кількість доставок')
                        ax3.set_xlabel('Період')
                        ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
                        ax3.grid(True, alpha=0.3)

            # 4. Ефективність транспорту
            if efficiency_data and 'data' in efficiency_data and efficiency_data['data']:
                print("⚡ Створення графіка ефективності...")
                ax4 = plt.subplot(3, 4, 4)

                # Беремо топ ефективних типів транспорту
                efficiency_scores = []
                transport_names = []

                for key, value in list(efficiency_data['data'].items())[:10]:
                    parts = key.split('_')
                    transport_name = parts[1] if len(parts) > 1 else key[:15]
                    efficiency = value.get('efficiency_ratio', 0)

                    transport_names.append(transport_name)
                    efficiency_scores.append(efficiency)
                    print(f"  ⚡ {transport_name}: ефективність = {efficiency}")

                if efficiency_scores and any(efficiency_scores):
                    bars = ax4.barh(range(len(transport_names)), efficiency_scores, color='#00B894', alpha=0.8)
                    ax4.set_title('Ефективність типів транспорту', fontsize=11, fontweight='bold')
                    ax4.set_xlabel('Коефіцієнт ефективності')
                    ax4.set_yticks(range(len(transport_names)))
                    ax4.set_yticklabels(transport_names)

                    for bar, score in zip(bars, efficiency_scores):
                        if score > 0:
                            ax4.text(bar.get_width() + max(efficiency_scores) * 0.01,
                                     bar.get_y() + bar.get_height() / 2,
                                     f'{score:.1f}', ha='left', va='center', fontsize=8)

            # 5. Розподіл транспорту по регіонах
            if region_data and 'data' in region_data and region_data['data']:
                print("🗺️ Створення графіка по регіонах...")
                ax5 = plt.subplot(3, 4, 5)

                # Групуємо по регіонах
                region_transport = {}
                for key, value in region_data['data'].items():
                    parts = key.split('_')
                    region = parts[2] if len(parts) > 2 else 'Unknown'

                    if region not in region_transport:
                        region_transport[region] = 0
                    region_transport[region] += value.get('total_deliveries', 0)

                print(f"🌍 Регіонів для аналізу: {len(region_transport)}")

                if region_transport:
                    # Топ 8 регіонів
                    top_regions = sorted(region_transport.items(), key=lambda x: x[1], reverse=True)[:8]
                    region_names = [region[0].replace('_', ' ')[:12] for region in top_regions]
                    region_deliveries = [region[1] for region in top_regions]

                    for name, deliveries in zip(region_names, region_deliveries):
                        print(f"  🌍 {name}: {deliveries} доставок")

                    bars = ax5.barh(range(len(region_names)), region_deliveries, color='#E17055', alpha=0.8)
                    ax5.set_title('Використання транспорту по регіонах', fontsize=11, fontweight='bold')
                    ax5.set_xlabel('Кількість доставок')
                    ax5.set_yticks(range(len(region_names)))
                    ax5.set_yticklabels(region_names)

                    for bar, value in zip(bars, region_deliveries):
                        if value > 0:
                            ax5.text(bar.get_width() + max(region_deliveries) * 0.01,
                                     bar.get_y() + bar.get_height() / 2,
                                     f'{value:,}', ha='left', va='center', fontsize=8)

            # 6. Транспорт по типах посилок
            if parcel_data and 'data' in parcel_data and parcel_data['data']:
                print("📦 Створення графіка по типах посилок...")
                ax6 = plt.subplot(3, 4, 6)

                # Групуємо по типах посилок
                parcel_transport = {}
                for key, value in parcel_data['data'].items():
                    parts = key.split('_')
                    parcel_type = parts[2] if len(parts) > 2 else 'Unknown'

                    if parcel_type not in parcel_transport:
                        parcel_transport[parcel_type] = {'deliveries': 0, 'weight': 0, 'count': 0}

                    parcel_transport[parcel_type]['deliveries'] += value.get('total_deliveries', 0)
                    parcel_transport[parcel_type]['weight'] += value.get('avg_parcel_weight', 0)
                    parcel_transport[parcel_type]['count'] += 1

                print(f"📦 Типів посилок: {len(parcel_transport)}")

                if parcel_transport:
                    parcel_types = list(parcel_transport.keys())
                    parcel_deliveries = [parcel_transport[pt]['deliveries'] for pt in parcel_types]

                    if any(parcel_deliveries):
                        colors = plt.cm.Set3(np.linspace(0, 1, len(parcel_types)))
                        wedges, texts, autotexts = ax6.pie(parcel_deliveries, labels=parcel_types,
                                                           autopct='%1.1f%%', colors=colors, startangle=90)
                        ax6.set_title('Розподіл транспорту по типах посилок', fontsize=11, fontweight='bold')

            # 7. Середній час обробки по типах транспорту
            if usage_data and 'data' in usage_data and usage_data['data']:
                print("⏱️ Створення графіка часу обробки...")
                ax7 = plt.subplot(3, 4, 7)

                # Групуємо по типах транспорту
                transport_processing_time = {}
                for key, value in usage_data['data'].items():
                    parts = key.split('_')
                    transport_type = parts[2] if len(parts) > 2 else 'Unknown'

                    if transport_type not in transport_processing_time:
                        transport_processing_time[transport_type] = {'time': 0, 'count': 0}

                    transport_processing_time[transport_type]['time'] += value.get('avg_processing_time', 0)
                    transport_processing_time[transport_type]['count'] += 1

                if transport_processing_time:
                    # Обчислюємо середній час
                    for transport_type in transport_processing_time:
                        if transport_processing_time[transport_type]['count'] > 0:
                            transport_processing_time[transport_type]['avg_time'] = (
                                    transport_processing_time[transport_type]['time'] /
                                    transport_processing_time[transport_type]['count']
                            )

                    transport_types = list(transport_processing_time.keys())[:8]
                    processing_times = [transport_processing_time[tt].get('avg_time', 0) for tt in transport_types]

                    print(f"⏱️ Типів транспорту для часу обробки: {len(transport_types)}")

                    if any(processing_times):
                        bars = ax7.bar(range(len(transport_types)), processing_times, color='#FD79A8', alpha=0.8)
                        ax7.set_title('Час обробки по типах транспорту', fontsize=11, fontweight='bold')
                        ax7.set_ylabel('Час обробки (год)')
                        ax7.set_xlabel('Тип транспорту')
                        ax7.set_xticks(range(len(transport_types)))
                        ax7.set_xticklabels([tt[:8] for tt in transport_types], rotation=45)

                        for bar, time in zip(bars, processing_times):
                            if time > 0:
                                ax7.text(bar.get_x() + bar.get_width() / 2,
                                         bar.get_height() + max(processing_times) * 0.01,
                                         f'{time:.1f}', ha='center', va='bottom', fontsize=8)

            # 8. Найбільш використовувані типи транспорту
            if most_used_data and 'data' in most_used_data and most_used_data['data']:
                print("🏆 Створення графіка найбільш використовуваних...")
                ax8 = plt.subplot(3, 4, 8)

                # Беремо перший період з найбільшою кількістю даних
                first_period = list(most_used_data['data'].keys())[0] if most_used_data['data'] else None
                if first_period and most_used_data['data'][first_period]:
                    top_transport = list(most_used_data['data'][first_period].items())[:6]

                    if top_transport:
                        transport_names = []
                        utilization_scores = []

                        for transport_key, transport_data in top_transport:
                            transport_name = transport_key.split('_')[2] if '_' in transport_key else transport_key[:10]
                            score = transport_data.get('period_utilization_score', 0)

                            transport_names.append(transport_name)
                            utilization_scores.append(score)
                            print(f"  🏆 {transport_name}: рейтинг = {score}")

                        if any(utilization_scores):
                            bars = ax8.barh(range(len(transport_names)), utilization_scores, color='#FDCB6E', alpha=0.8)
                            ax8.set_title(f'Топ транспорт ({first_period})', fontsize=11, fontweight='bold')
                            ax8.set_xlabel('Рейтинг використання')
                            ax8.set_yticks(range(len(transport_names)))
                            ax8.set_yticklabels(transport_names)

                            for bar, score in zip(bars, utilization_scores):
                                if score > 0:
                                    ax8.text(bar.get_width() + max(utilization_scores) * 0.01,
                                             bar.get_y() + bar.get_height() / 2,
                                             f'{score:.1f}', ha='left', va='center', fontsize=8)

            # 9. Динаміка змін використання транспорту
            if changes_data and 'data' in changes_data and changes_data['data']:
                print("📊 Створення графіка динаміки змін...")
                ax9 = plt.subplot(3, 4, 9)

                changes = list(changes_data['data'].values())
                if changes:
                    change_labels = [change.get('current_period', 'Unknown') for change in changes]
                    delivery_changes = [change.get('deliveries_change', 0) for change in changes]

                    print(f"📊 Періодів змін: {len(changes)}")

                    if any(delivery_changes):
                        colors = ['green' if x >= 0 else 'red' for x in delivery_changes]
                        bars = ax9.bar(range(len(change_labels)), delivery_changes, color=colors, alpha=0.7)
                        ax9.set_title('Зміни кількості доставок', fontsize=11, fontweight='bold')
                        ax9.set_ylabel('Зміна доставок')
                        ax9.set_xlabel('Період')
                        ax9.set_xticks(range(len(change_labels)))
                        ax9.set_xticklabels(change_labels, rotation=45)
                        ax9.axhline(y=0, color='black', linestyle='-', alpha=0.3)

                        for bar, change in zip(bars, delivery_changes):
                            if change != 0:
                                ax9.text(bar.get_x() + bar.get_width() / 2,
                                         bar.get_height() + (max(delivery_changes) - min(delivery_changes)) * 0.01,
                                         f'{change:+}', ha='center', va='bottom', fontsize=8)

            # 10. Розподіл відділень по типах транспорту
            if usage_data and 'data' in usage_data and usage_data['data']:
                print("🏢 Створення графіка відділень по транспорту...")
                ax10 = plt.subplot(3, 4, 10)

                # Групуємо кількість відділень по типах транспорту
                transport_departments = {}
                for key, value in usage_data['data'].items():
                    parts = key.split('_')
                    transport_type = parts[2] if len(parts) > 2 else 'Unknown'

                    if transport_type not in transport_departments:
                        transport_departments[transport_type] = 0
                    transport_departments[transport_type] += value.get('departments_served', 0)

                if transport_departments:
                    # Топ 8 типів транспорту
                    top_transport_dept = sorted(transport_departments.items(), key=lambda x: x[1], reverse=True)[:8]
                    transport_names = [item[0][:10] for item in top_transport_dept]
                    dept_counts = [item[1] for item in top_transport_dept]

                    print(f"🏢 Типів транспорту для відділень: {len(transport_names)}")

                    if any(dept_counts):
                        bars = ax10.bar(range(len(transport_names)), dept_counts, color='#A29BFE', alpha=0.8)
                        ax10.set_title('Відділення по типах транспорту', fontsize=11, fontweight='bold')
                        ax10.set_ylabel('Кількість відділень')
                        ax10.set_xlabel('Тип транспорту')
                        ax10.set_xticks(range(len(transport_names)))
                        ax10.set_xticklabels(transport_names, rotation=45)

                        for bar, count in zip(bars, dept_counts):
                            if count > 0:
                                ax10.text(bar.get_x() + bar.get_width() / 2,
                                          bar.get_height() + max(dept_counts) * 0.01,
                                          f'{count}', ha='center', va='bottom', fontsize=8)

            # 11. Середня вага посилок по типах транспорту
            if parcel_data and 'data' in parcel_data and parcel_data['data']:
                print("⚖️ Створення графіка ваги посилок...")
                ax11 = plt.subplot(3, 4, 11)

                # Групуємо середню вагу по типах транспорту
                transport_weight = {}
                for key, value in parcel_data['data'].items():
                    parts = key.split('_')
                    transport_type = parts[1] if len(parts) > 1 else 'Unknown'

                    if transport_type not in transport_weight:
                        transport_weight[transport_type] = {'weight': 0, 'count': 0}

                    transport_weight[transport_type]['weight'] += value.get('avg_parcel_weight', 0)
                    transport_weight[transport_type]['count'] += 1

                if transport_weight:
                    # Обчислюємо середню вагу
                    for transport_type in transport_weight:
                        if transport_weight[transport_type]['count'] > 0:
                            transport_weight[transport_type]['avg_weight'] = (
                                    transport_weight[transport_type]['weight'] /
                                    transport_weight[transport_type]['count']
                            )

                    transport_types = list(transport_weight.keys())[:8]
                    avg_weights = [transport_weight[tt].get('avg_weight', 0) for tt in transport_types]

                    print(f"⚖️ Типів транспорту для ваги: {len(transport_types)}")

                    if any(avg_weights):
                        bars = ax11.bar(range(len(transport_types)), avg_weights, color='#00CEC9', alpha=0.8)
                        ax11.set_title('Середня вага посилок по транспорту', fontsize=11, fontweight='bold')
                        ax11.set_ylabel('Середня вага (кг)')
                        ax11.set_xlabel('Тип транспорту')
                        ax11.set_xticks(range(len(transport_types)))
                        ax11.set_xticklabels([tt[:8] for tt in transport_types], rotation=45)

                        for bar, weight in zip(bars, avg_weights):
                            if weight > 0:
                                ax11.text(bar.get_x() + bar.get_width() / 2,
                                          bar.get_height() + max(avg_weights) * 0.01,
                                          f'{weight:.1f}', ha='center', va='bottom', fontsize=8)

            # 12. Гістограма розподілу рейтингів використання
            if usage_data and 'data' in usage_data and usage_data['data']:
                print("📊 Створення гістограми рейтингів...")
                ax12 = plt.subplot(3, 4, 12)

                utilization_scores = []
                for value in usage_data['data'].values():
                    score = value.get('period_utilization_score', 0)
                    if score > 0:
                        utilization_scores.append(score)

                print(f"📊 Рейтингів для гістограми: {len(utilization_scores)}")

                if utilization_scores and len(utilization_scores) > 1:
                    bins_count = min(15, len(utilization_scores))
                    ax12.hist(utilization_scores, bins=bins_count,
                              color='#6C5CE7', alpha=0.7, edgecolor='black')
                    ax12.set_title('Розподіл рейтингів використання', fontsize=11, fontweight='bold')
                    ax12.set_xlabel('Рейтинг використання')
                    ax12.set_ylabel('Кількість записів')

                    mean_score = np.mean(utilization_scores)
                    ax12.axvline(mean_score, color='red', linestyle='--',
                                 label=f'Середнє: {mean_score:.1f}')
                    ax12.legend()

                    print(f"📊 Середній рейтинг: {mean_score:.1f}")

            print("🎨 Налаштування макету...")
            plt.tight_layout(pad=2.0)

            print("💾 Збереження графіка...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chart_path = os.path.join(self.config.CHARTS_PATH, f'transport_utilization_extended_{timestamp}.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✅ Розширений графік транспорту збережено: {os.path.basename(chart_path)}")
            return True

        except Exception as e:
            print(f"❌ Помилка створення графіків транспорту: {e}")
            print(f"🔍 Детальна помилка: {traceback.format_exc()}")
            plt.close('all')
            return False

    def create_all_charts(self):
        """Створює всі графіки"""
        print("🎨 Створення всіх графіків...")

        charts_created = []

        # Кур'єри
        if self.create_courier_performance_charts():
            charts_created.append("Кур'єри")

        # Відділення
        if self.create_department_workload_charts():
            charts_created.append("Відділення")

        # Час обробки
        if self.create_processing_time_charts():
            charts_created.append("Час обробки")

        # Транспорт
        if self.create_transport_utilization_charts():
            charts_created.append("Транспорт")

        print(f"✅ Створено графіки для: {', '.join(charts_created)}")
        return charts_created