# data_science/base_model.py
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
import joblib
import os
from datetime import datetime, timedelta
import json
import sys

sys.path.append('..')
from config.database_config import DatabaseConfig


class BaseMLModel:
    """Базовий клас для всіх ML моделей"""

    def __init__(self, model_name):
        self.model_name = model_name
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.feature_names = []
        self.target_name = ""
        self.config = DatabaseConfig()
        self.model_path = os.path.join(self.config.PROCESSED_DATA_PATH, 'models')
        os.makedirs(self.model_path, exist_ok=True)

    def prepare_data(self, data, target_column, feature_columns=None):
        """Підготовка даних для навчання"""
        print(f"📊 Підготовка даних для моделі {self.model_name}...")

        if feature_columns is None:
            feature_columns = [col for col in data.columns if col != target_column]

        # Видаляємо рядки з пропущеними значеннями в цільовій змінній
        data_clean = data.dropna(subset=[target_column])

        X = data_clean[feature_columns].copy()
        y = data_clean[target_column].copy()

        # Обробка категоріальних змінних
        for column in X.columns:
            if X[column].dtype == 'object':
                if column not in self.label_encoders:
                    self.label_encoders[column] = LabelEncoder()
                    X[column] = self.label_encoders[column].fit_transform(X[column].astype(str))
                else:
                    X[column] = self.label_encoders[column].transform(X[column].astype(str))

        # Заповнення пропущених значень
        X = X.fillna(X.mean())

        self.feature_names = feature_columns
        self.target_name = target_column

        print(f"✅ Підготовлено {len(X)} записів з {len(feature_columns)} ознаками")
        return X, y

    def train_model(self, X, y):
        """Навчання моделі"""
        print(f"🎯 Навчання моделі {self.model_name}...")

        # Розділення на тренувальну та тестову вибірки
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Масштабування даних
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Навчання моделі
        self.model.fit(X_train_scaled, y_train)

        # Прогнозування на тестовій вибірці
        y_pred = self.model.predict(X_test_scaled)

        # Розрахунок метрик
        metrics = {
            'train_r2': self.model.score(X_train_scaled, y_train),
            'test_r2': r2_score(y_test, y_pred),
            'mae': mean_absolute_error(y_test, y_pred),
            'mse': mean_squared_error(y_test, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_test, y_pred))
        }

        print(f"✅ Модель навчена! R² = {metrics['test_r2']:.3f}")
        return metrics

    def predict(self, X):
        """Прогнозування"""
        if self.model is None:
            raise ValueError("Модель не навчена! Спочатку викличте train_model()")

        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)

    def save_model(self):
        """Збереження моделі"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        model_filename = f"{self.model_name}_{timestamp}.joblib"
        model_filepath = os.path.join(self.model_path, model_filename)

        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'label_encoders': self.label_encoders,
            'feature_names': self.feature_names,
            'target_name': self.target_name
        }

        joblib.dump(model_data, model_filepath)
        print(f"💾 Модель збережена: {model_filename}")
        return model_filepath

    def load_model(self, model_filepath):
        """Завантаження моделі"""
        model_data = joblib.load(model_filepath)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.label_encoders = model_data['label_encoders']
        self.feature_names = model_data['feature_names']
        self.target_name = model_data['target_name']
        print(f"📥 Модель завантажена: {os.path.basename(model_filepath)}")

    def save_predictions(self, predictions, filename_suffix=""):
        """Збереження прогнозів у файл"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.model_name}_predictions_{filename_suffix}_{timestamp}.json"
        filepath = os.path.join(self.config.PROCESSED_DATA_PATH, filename)

        prediction_data = {
            'model_name': self.model_name,
            'timestamp': timestamp,
            'predictions': predictions
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(prediction_data, f, ensure_ascii=False, indent=2)

        print(f"💾 Прогнози збережено: {filename}")
        return filepath