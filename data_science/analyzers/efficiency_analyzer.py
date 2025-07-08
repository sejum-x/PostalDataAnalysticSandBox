# data_science/analyzers/efficiency_analyzer.py
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import sys

sys.path.append('..')
from data_science.base_model import BaseMLModel
from utils.helpers import get_latest_csv_file


class EfficiencyAnalyzer(BaseMLModel):
    """Аналіз ефективності на основі реальних даних"""

    def __init__(self):
        super().__init__("efficiency_analyzer")
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)

    def analyze_department_performance(self):
        """Аналіз продуктивності відділень"""
        print("🏢 Аналіз продуктивності відділень...")

        # Завантаження даних
        delivery_file = get_latest_csv_file(self.config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
        if not delivery_file:
            raise FileNotFoundError("Файл delivery_periodic_raw_data не знайдено")

        data = pd.read_csv(delivery_file)

        # Аналіз по відділенням
        dept_analysis = data.groupby(
            ['department_id', 'department_number', 'department_city', 'department_region']).agg({
            'deliveries_count': ['sum', 'mean', 'std'],
            'processing_time_hours': ['mean', 'std'],
            'deliveries_share_percentage': 'mean',
            'start_month': 'count'  # кількість періодів роботи
        }).round(2)

        # Спрощення назв колонок
        dept_analysis.columns = [
            'total_deliveries', 'avg_deliveries_per_period', 'std_deliveries',
            'avg_processing_time', 'std_processing_time',
            'avg_market_share', 'active_periods'
        ]

        # Розрахунок метрик ефективності
        dept_analysis['deliveries_per_hour'] = (
                dept_analysis['avg_deliveries_per_period'] /
                (dept_analysis['avg_processing_time'] + 0.1)
        )

        dept_analysis['consistency_score'] = (
                1 / (dept_analysis['std_deliveries'] + 1)
        )

        dept_analysis['efficiency_score'] = (
                dept_analysis['deliveries_per_hour'] * 0.4 +
                dept_analysis['consistency_score'] * 0.3 +
                dept_analysis['avg_market_share'] * 0.3
        )

        # Виявлення аномалій
        features_for_anomaly = ['avg_deliveries_per_period', 'avg_processing_time', 'avg_market_share']
        X_anomaly = dept_analysis[features_for_anomaly].fillna(0)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_anomaly)

        anomalies = self.anomaly_detector.fit_predict(X_scaled)
        dept_analysis['is_anomaly'] = anomalies == -1

        # Класифікація відділень
        dept_analysis['performance_category'] = pd.cut(
            dept_analysis['efficiency_score'],
            bins=[-np.inf, dept_analysis['efficiency_score'].quantile(0.25),
                  dept_analysis['efficiency_score'].quantile(0.75), np.inf],
            labels=['Needs_Improvement', 'Average', 'High_Performance']
        )

        return dept_analysis.reset_index()

    def analyze_transport_efficiency(self):
        """Аналіз ефективності транспорту"""
        print("🚛 Аналіз ефективності транспорту...")

        delivery_file = get_latest_csv_file(self.config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
        data = pd.read_csv(delivery_file)

        # Аналіз по типах транспорту
        transport_analysis = data.groupby(['transport_body_type_id', 'transport_type_name']).agg({
            'deliveries_count': ['sum', 'mean'],
            'processing_time_hours': 'mean',
            'parcel_max_weight': 'mean',
            'department_id': 'nunique'  # кількість відділень, що використовують
        }).round(2)

        transport_analysis.columns = [
            'total_deliveries', 'avg_deliveries_per_period',
            'avg_processing_time', 'avg_max_weight', 'departments_using'
        ]

        # Метрики ефективності транспорту
        transport_analysis['weight_efficiency'] = (
                transport_analysis['avg_max_weight'] /
                (transport_analysis['avg_processing_time'] + 0.1)
        )

        transport_analysis['utilization_rate'] = (
                transport_analysis['departments_using'] /
                transport_analysis['departments_using'].max()
        )

        transport_analysis['transport_efficiency_score'] = (
                transport_analysis['weight_efficiency'] * 0.4 +
                transport_analysis['utilization_rate'] * 0.6
        )

        return transport_analysis.reset_index()

    def analyze_seasonal_patterns(self):
        """Аналіз сезонних патернів"""
        print("📅 Аналіз сезонних патернів...")

        delivery_file = get_latest_csv_file(self.config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
        data = pd.read_csv(delivery_file)

        # Аналіз по місяцях
        monthly_analysis = data.groupby('start_month').agg({
            'deliveries_count': ['sum', 'mean', 'std'],
            'processing_time_hours': 'mean',
            'department_id': 'nunique'
        }).round(2)

        monthly_analysis.columns = [
            'total_deliveries', 'avg_deliveries', 'std_deliveries',
            'avg_processing_time', 'active_departments'
        ]

        # Визначення сезонів
        seasons = {
            'Winter': [12, 1, 2],
            'Spring': [3, 4, 5],
            'Summer': [6, 7, 8],
            'Autumn': [9, 10, 11]
        }

        seasonal_analysis = {}
        for season, months in seasons.items():
            season_data = data[data['start_month'].isin(months)]
            if len(season_data) > 0:
                seasonal_analysis[season] = {
                    'total_deliveries': season_data['deliveries_count'].sum(),
                    'avg_processing_time': season_data['processing_time_hours'].mean(),
                    'active_departments': season_data['department_id'].nunique(),
                    'months_included': months
                }

        return {
            'monthly_patterns': monthly_analysis.to_dict('index'),
            'seasonal_patterns': seasonal_analysis
        }

    def generate_improvement_recommendations(self):
        """Генерація рекомендацій для покращення"""
        print("💡 Генерація рекомендацій для покращення...")

        dept_analysis = self.analyze_department_performance()
        transport_analysis = self.analyze_transport_efficiency()
        seasonal_analysis = self.analyze_seasonal_patterns()

        recommendations = []

        # Рекомендації для відділень
        low_performance_depts = dept_analysis[
            dept_analysis['performance_category'] == 'Needs_Improvement'
            ]

        for _, dept in low_performance_depts.iterrows():
            recommendations.append({
                'type': 'department_improvement',
                'target': f"Відділення {dept['department_number']} ({dept['department_city']})",
                'priority': 'high',
                'issues': [
                    f"Низька ефективність: {dept['efficiency_score']:.2f}",
                    f"Час обробки: {dept['avg_processing_time']:.1f} год",
                    f"Середня кількість доставок: {dept['avg_deliveries_per_period']:.1f}"
                ],
                'suggestions': [
                    'Оптимізувати процеси обробки посилок',
                    'Перевірити завантаженість персоналу',
                    'Розглянути додаткове обладнання'
                ]
            })

        # Рекомендації по транспорту
        transport_sorted = transport_analysis.sort_values('transport_efficiency_score')
        low_efficiency_transport = transport_sorted.head(3)

        for _, transport in low_efficiency_transport.iterrows():
            recommendations.append({
                'type': 'transport_optimization',
                'target': f"Транспорт: {transport['transport_type_name']}",
                'priority': 'medium',
                'issues': [
                    f"Низька ефективність: {transport['transport_efficiency_score']:.2f}",
                    f"Використовується в {transport['departments_using']} відділеннях"
                ],
                'suggestions': [
                    'Розглянути альтернативні типи транспорту',
                    'Оптимізувати маршрути',
                    'Збільшити використання в інших відділеннях'
                ]
            })

        # Сезонні рекомендації
        seasonal_patterns = seasonal_analysis['seasonal_patterns']
        if seasonal_patterns:
            peak_season = max(seasonal_patterns.items(),
                              key=lambda x: x[1]['total_deliveries'])

            recommendations.append({
                'type': 'seasonal_planning',
                'target': f"Сезонне планування",
                'priority': 'medium',
                'issues': [
                    f"Пік доставок в сезоні: {peak_season[0]}",
                    f"Максимальна кількість: {peak_season[1]['total_deliveries']}"
                ],
                'suggestions': [
                    f'Підготувати додаткові ресурси на {peak_season[0].lower()}',
                    'Планувати персонал заздалегідь',
                    'Оптимізувати склади перед піковим сезоном'
                ]
            })

        return recommendations