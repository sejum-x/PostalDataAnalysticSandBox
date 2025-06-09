from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_restx import Api, Resource, fields, Namespace
from werkzeug.exceptions import NotFound
import os
import sys
from datetime import datetime
import traceback
import glob
import zipfile
import tempfile

# Додаємо шляхи для імпорту
sys.path.append('.')

from data_extraction.data_extractor import DataWarehouseExtractor
from analysis.courier_analysis import CourierAnalyzer
from analysis.department_analysis import DepartmentAnalyzer
from analysis.processing_time_analysis import ProcessingTimeAnalyzer
from analysis.transport_analysis import TransportAnalyzer
from reports.report_generator import DWReportGenerator
from visualizations.charts import DWChartGenerator
from config.database_config import DatabaseConfig
from utils.helpers import get_latest_csv_file

# Ініціалізація Flask та Swagger
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['RESTX_MASK_SWAGGER'] = False

# Swagger конфігурація
api = Api(
    app,
    version='1.0',
    title='PostDW Analytics API',
    description='API для аналізу Data Warehouse поштової служби',
    doc='/swagger/',
    prefix='/api/v1'
)

# Ініціалізуємо компоненти
config = DatabaseConfig()
extractor = DataWarehouseExtractor()
courier_analyzer = CourierAnalyzer()
department_analyzer = DepartmentAnalyzer()
processing_analyzer = ProcessingTimeAnalyzer()
transport_analyzer = TransportAnalyzer()
report_generator = DWReportGenerator()
chart_generator = DWChartGenerator()

# Namespaces для групування endpoints
health_ns = Namespace('health', description='Перевірка здоров\'я системи')
data_ns = Namespace('data', description='Операції з даними')
analysis_ns = Namespace('analysis', description='Аналітичні операції')
reports_ns = Namespace('reports', description='Генерація звітів')
files_ns = Namespace('files', description='Робота з файлами')

api.add_namespace(health_ns, path='/health')
api.add_namespace(data_ns, path='/data')
api.add_namespace(analysis_ns, path='/analysis')
api.add_namespace(reports_ns, path='/reports')
api.add_namespace(files_ns, path='/files')

# Моделі для Swagger документації
health_model = api.model('Health', {
    'status': fields.String(description='Статус системи'),
    'timestamp': fields.String(description='Час перевірки'),
    'service': fields.String(description='Назва сервісу')
})

simple_response_model = api.model('SimpleResponse', {
    'success': fields.Boolean(description='Статус операції'),
    'message': fields.String(description='Повідомлення про результат'),
    'used_file': fields.String(description='Використаний файл'),
    'records_processed': fields.Integer(description='Кількість оброблених записів'),
    'execution_time': fields.String(description='Час виконання'),
    'timestamp': fields.String(description='Час завершення')
})


# =============================================================================
# HEALTH ENDPOINTS
# =============================================================================

@health_ns.route('/')
class HealthCheck(Resource):
    @health_ns.marshal_with(health_model)
    @health_ns.doc('health_check')
    def get(self):
        """Перевірка здоров'я API"""
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'service': 'PostDW Analytics API'
        }


@health_ns.route('/connection')
class ConnectionTest(Resource):
    @health_ns.doc('test_connection')
    def get(self):
        """Тестування підключення до DW"""
        try:
            success, message = extractor.test_connection()
            return {
                'success': success,
                'message': message,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500


@health_ns.route('/status')
class SystemStatus(Resource):
    @health_ns.doc('system_status')
    def get(self):
        """Детальний статус системи та файлів"""
        try:
            # Перевіряємо доступні файли
            files = {
                'courier_delivery': get_latest_csv_file(config.RAW_DATA_PATH, 'courier_delivery_raw_data_*.csv'),
                'delivery_periodic': get_latest_csv_file(config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
            }

            file_status = {}
            for file_type, file_path in files.items():
                if file_path and os.path.exists(file_path):
                    file_size = os.path.getsize(file_path) / 1024
                    mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    file_status[file_type] = {
                        'exists': True,
                        'filename': os.path.basename(file_path),
                        'size_kb': round(file_size, 1),
                        'modified': mod_time.isoformat()
                    }
                else:
                    file_status[file_type] = {'exists': False}

            # Перевіряємо директорії
            directories = {
                'raw_data': config.RAW_DATA_PATH,
                'processed_data': config.PROCESSED_DATA_PATH,
                'reports': config.REPORTS_PATH,
                'charts': config.CHARTS_PATH
            }

            dir_status = {}
            for dir_name, dir_path in directories.items():
                dir_status[dir_name] = {
                    'exists': os.path.exists(dir_path),
                    'path': dir_path,
                    'files_count': len(os.listdir(dir_path)) if os.path.exists(dir_path) else 0
                }

            return {
                'status': 'operational',
                'files': file_status,
                'directories': dir_status,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500


# =============================================================================
# DATA ENDPOINTS
# =============================================================================

@data_ns.route('/extract')
class DataExtraction(Resource):
    @data_ns.doc('extract_data')
    def get(self):
        """Вивантаження даних з DW"""
        try:
            start_time = datetime.now()
            results = extractor.extract_all_raw_data()
            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            if isinstance(results, dict) and 'error' in results:
                return {
                    'success': False,
                    'message': results['error'],
                    'timestamp': datetime.now().isoformat()
                }, 400

            successful = sum(1 for result in results.values() if result.get('success', False))
            total = len(results)

            # Підраховуємо загальну кількість записів
            total_records = 0
            for result in results.values():
                if isinstance(result, dict) and 'records' in result:
                    total_records += result.get('records', 0)

            return {
                'success': successful > 0,
                'message': f'Вивантажено {successful}/{total} файлів успішно',
                'records_processed': total_records,
                'execution_time': execution_time,
                'files_created': [f"{k}.csv" for k, v in results.items() if v.get('success', False)],
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при вивантаженні: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


# =============================================================================
# ANALYSIS ENDPOINTS
# =============================================================================

@analysis_ns.route('/courier')
class CourierAnalysis(Resource):
    @analysis_ns.doc('analyze_courier')
    @analysis_ns.marshal_with(simple_response_model)
    def get(self):
        """Аналіз продуктивності кур'єрів"""
        try:
            start_time = datetime.now()

            filepath = get_latest_csv_file(config.RAW_DATA_PATH, 'courier_delivery_raw_data_*.csv')
            if not filepath:
                return {
                    'success': False,
                    'message': 'Файл courier_delivery_raw_data не знайдено',
                    'timestamp': datetime.now().isoformat()
                }, 404

            # Виконуємо аналіз (результат не повертаємо)
            results = courier_analyzer.analyze_courier_performance(filepath)

            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            if 'error' in results:
                return {
                    'success': False,
                    'message': results['error'],
                    'timestamp': datetime.now().isoformat()
                }, 400

            # Підраховуємо кількість оброблених записів
            records_count = 0
            if 'summary' in results:
                summary = results['summary']
                if 'total_couriers' in summary:
                    records_count = summary['total_couriers']

            return {
                'success': True,
                'message': 'Аналіз кур\'єрів виконано успішно',
                'used_file': os.path.basename(filepath),
                'records_processed': records_count,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при аналізі кур\'єрів: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


@analysis_ns.route('/department')
class DepartmentAnalysis(Resource):
    @analysis_ns.doc('analyze_department')
    @analysis_ns.marshal_with(simple_response_model)
    def get(self):
        """Аналіз завантажень відділень"""
        try:
            start_time = datetime.now()

            filepath = get_latest_csv_file(config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
            if not filepath:
                return {
                    'success': False,
                    'message': 'Файл delivery_periodic_raw_data не знайдено',
                    'timestamp': datetime.now().isoformat()
                }, 404

            results = department_analyzer.analyze_department_workload_by_periods(filepath)

            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            if 'error' in results:
                return {
                    'success': False,
                    'message': results['error'],
                    'timestamp': datetime.now().isoformat()
                }, 400

            # Підраховуємо кількість оброблених записів
            records_count = 0
            if 'summary' in results:
                summary = results['summary']
                if 'total_departments' in summary:
                    records_count = summary['total_departments']

            return {
                'success': True,
                'message': 'Аналіз відділень виконано успішно',
                'used_file': os.path.basename(filepath),
                'records_processed': records_count,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при аналізі відділень: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


@analysis_ns.route('/processing-time')
class ProcessingTimeAnalysis(Resource):
    @analysis_ns.doc('analyze_processing_time')
    @analysis_ns.marshal_with(simple_response_model)
    def get(self):
        """Аналіз часу обробки посилок"""
        try:
            start_time = datetime.now()

            filepath = get_latest_csv_file(config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
            if not filepath:
                return {
                    'success': False,
                    'message': 'Файл delivery_periodic_raw_data не знайдено',
                    'timestamp': datetime.now().isoformat()
                }, 404

            results = processing_analyzer.analyze_processing_times_by_periods(filepath)

            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            if 'error' in results:
                return {
                    'success': False,
                    'message': results['error'],
                    'timestamp': datetime.now().isoformat()
                }, 400

            # Підраховуємо кількість оброблених записів
            records_count = 0
            if 'summary' in results:
                summary = results['summary']
                if 'total_records' in summary:
                    records_count = summary['total_records']

            return {
                'success': True,
                'message': 'Аналіз часу обробки виконано успішно',
                'used_file': os.path.basename(filepath),
                'records_processed': records_count,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при аналізі часу обробки: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


@analysis_ns.route('/transport')
class TransportAnalysis(Resource):
    @analysis_ns.doc('analyze_transport')
    @analysis_ns.marshal_with(simple_response_model)
    def get(self):
        """Аналіз використання транспорту"""
        try:
            start_time = datetime.now()

            filepath = get_latest_csv_file(config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
            if not filepath:
                return {
                    'success': False,
                    'message': 'Файл delivery_periodic_raw_data не знайдено',
                    'timestamp': datetime.now().isoformat()
                }, 404

            results = transport_analyzer.analyze_transport_utilization_by_periods(filepath)

            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            if 'error' in results:
                return {
                    'success': False,
                    'message': results['error'],
                    'timestamp': datetime.now().isoformat()
                }, 400

            # Підраховуємо кількість оброблених записів
            records_count = 0
            if 'summary' in results:
                summary = results['summary']
                if 'total_vehicles' in summary:
                    records_count = summary['total_vehicles']

            return {
                'success': True,
                'message': 'Аналіз транспорту виконано успішно',
                'used_file': os.path.basename(filepath),
                'records_processed': records_count,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при аналізі транспорту: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


@analysis_ns.route('/all')
class AllAnalysis(Resource):
    @analysis_ns.doc('analyze_all')
    def get(self):
        """Запуск всіх аналізів одночасно"""
        try:
            start_time = datetime.now()
            results = {}
            total_records = 0

            # Аналіз кур'єрів
            courier_file = get_latest_csv_file(config.RAW_DATA_PATH, 'courier_delivery_raw_data_*.csv')
            if courier_file:
                try:
                    courier_results = courier_analyzer.analyze_courier_performance(courier_file)
                    if 'summary' in courier_results and 'total_couriers' in courier_results['summary']:
                        total_records += courier_results['summary']['total_couriers']
                    results['courier_analysis'] = {
                        'success': True,
                        'message': 'Аналіз кур\'єрів виконано',
                        'used_file': os.path.basename(courier_file)
                    }
                except Exception as e:
                    results['courier_analysis'] = {
                        'success': False,
                        'message': f'Помилка аналізу кур\'єрів: {str(e)}'
                    }
            else:
                results['courier_analysis'] = {
                    'success': False,
                    'message': 'Файл courier_delivery_raw_data не знайдено'
                }

            # Аналіз відділень, обробки та транспорту
            delivery_file = get_latest_csv_file(config.RAW_DATA_PATH, 'delivery_periodic_raw_data_*.csv')
            if delivery_file:
                # Аналіз відділень
                try:
                    dept_results = department_analyzer.analyze_department_workload_by_periods(delivery_file)
                    if 'summary' in dept_results and 'total_departments' in dept_results['summary']:
                        total_records += dept_results['summary']['total_departments']
                    results['department_analysis'] = {
                        'success': True,
                        'message': 'Аналіз відділень виконано',
                        'used_file': os.path.basename(delivery_file)
                    }
                except Exception as e:
                    results['department_analysis'] = {
                        'success': False,
                        'message': f'Помилка аналізу відділень: {str(e)}'
                    }

                # Аналіз часу обробки
                try:
                    proc_results = processing_analyzer.analyze_processing_times_by_periods(delivery_file)
                    results['processing_analysis'] = {
                        'success': True,
                        'message': 'Аналіз часу обробки виконано',
                        'used_file': os.path.basename(delivery_file)
                    }
                except Exception as e:
                    results['processing_analysis'] = {
                        'success': False,
                        'message': f'Помилка аналізу часу обробки: {str(e)}'
                    }

                # Аналіз транспорту
                try:
                    transport_results = transport_analyzer.analyze_transport_utilization_by_periods(delivery_file)
                    if 'summary' in transport_results and 'total_vehicles' in transport_results['summary']:
                        total_records += transport_results['summary']['total_vehicles']
                    results['transport_analysis'] = {
                        'success': True,
                        'message': 'Аналіз транспорту виконано',
                        'used_file': os.path.basename(delivery_file)
                    }
                except Exception as e:
                    results['transport_analysis'] = {
                        'success': False,
                        'message': f'Помилка аналізу транспорту: {str(e)}'
                    }
            else:
                error_msg = 'Файл delivery_periodic_raw_data не знайдено'
                results['department_analysis'] = {'success': False, 'message': error_msg}
                results['processing_analysis'] = {'success': False, 'message': error_msg}
                results['transport_analysis'] = {'success': False, 'message': error_msg}

            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            # Підрахунок успішних аналізів
            successful = sum(1 for result in results.values() if result.get('success', False))
            total = len(results)

            return {
                'success': successful > 0,
                'message': f'Виконано {successful}/{total} аналізів успішно',
                'results': results,
                'records_processed': total_records,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при виконанні аналізів: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


# =============================================================================
# REPORTS ENDPOINTS
# =============================================================================

@reports_ns.route('/generate')
class ReportGeneration(Resource):
    @reports_ns.doc('generate_reports')
    def get(self):
        """Генерація всіх звітів"""
        try:
            start_time = datetime.now()
            reports = report_generator.generate_all_reports()
            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            return {
                'success': True,
                'message': f'Згенеровано {len(reports)} звітів',
                'reports': [os.path.basename(report) for report in reports],
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при генерації звітів: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


@reports_ns.route('/charts')
class ChartGeneration(Resource):
    @reports_ns.doc('generate_charts')
    def get(self):
        """Створення всіх графіків"""
        try:
            start_time = datetime.now()
            charts = chart_generator.create_all_charts()
            end_time = datetime.now()
            execution_time = str(end_time - start_time)

            return {
                'success': True,
                'message': f'Створено {len(charts)} графіків',
                'charts': charts,
                'execution_time': execution_time,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Помилка при створенні графіків: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, 500


# =============================================================================
# FILES ENDPOINTS
# =============================================================================

@files_ns.route('/list')
class FilesList(Resource):
    @files_ns.doc('list_files')
    @files_ns.param('type', 'Тип файлів (raw, processed, reports, charts)',
                    enum=['raw', 'processed', 'reports', 'charts', 'all'])
    def get(self):
        """Список доступних файлів"""
        try:
            file_type = request.args.get('type', 'all')

            files_info = {}

            directories = {
                'raw': config.RAW_DATA_PATH,
                'processed': config.PROCESSED_DATA_PATH,
                'reports': config.REPORTS_PATH,
                'charts': config.CHARTS_PATH
            }

            if file_type == 'all':
                search_dirs = directories
            else:
                search_dirs = {file_type: directories.get(file_type)}

            for dir_type, dir_path in search_dirs.items():
                if not dir_path or not os.path.exists(dir_path):
                    files_info[dir_type] = []
                    continue

                files = []
                for file_path in glob.glob(os.path.join(dir_path, '*')):
                    if os.path.isfile(file_path):
                        file_size = os.path.getsize(file_path) / 1024
                        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))

                        files.append({
                            'filename': os.path.basename(file_path),
                            'size_kb': round(file_size, 1),
                            'modified': mod_time.isoformat(),
                            'path': file_path
                        })

                files_info[dir_type] = sorted(files, key=lambda x: x['modified'], reverse=True)

            return {
                'success': True,
                'files': files_info,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500


@files_ns.route('/download/<path:filename>')
class FileDownload(Resource):
    @files_ns.doc('download_file')
    def get(self, filename):
        """Завантаження конкретного файлу"""
        try:
            # Шукаємо файл у всіх директоріях
            search_paths = [
                config.RAW_DATA_PATH,
                config.PROCESSED_DATA_PATH,
                config.REPORTS_PATH,
                config.CHARTS_PATH
            ]

            file_path = None
            for search_path in search_paths:
                potential_path = os.path.join(search_path, filename)
                if os.path.exists(potential_path):
                    file_path = potential_path
                    break

            if not file_path:
                return {
                    'success': False,
                    'error': f'Файл {filename} не знайдено',
                    'timestamp': datetime.now().isoformat()
                }, 404

            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename
            )

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500


@files_ns.route('/download-all/<string:file_type>')
class BulkDownload(Resource):
    @files_ns.doc('download_all_files')
    @files_ns.param('file_type', 'Тип файлів для завантаження', enum=['raw', 'processed', 'reports', 'charts'])
    def get(self, file_type):
        """Завантаження всіх файлів певного типу в ZIP архіві"""
        try:
            directories = {
                'raw': config.RAW_DATA_PATH,
                'processed': config.PROCESSED_DATA_PATH,
                'reports': config.REPORTS_PATH,
                'charts': config.CHARTS_PATH
            }

            if file_type not in directories:
                return {
                    'success': False,
                    'error': f'Невідомий тип файлів: {file_type}',
                    'timestamp': datetime.now().isoformat()
                }, 400

            source_dir = directories[file_type]
            if not os.path.exists(source_dir):
                return {
                    'success': False,
                    'error': f'Директорія {source_dir} не існує',
                    'timestamp': datetime.now().isoformat()
                }, 404

            # Створюємо тимчасовий ZIP файл
            temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')

            with zipfile.ZipFile(temp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, source_dir)
                        zipf.write(file_path, arcname)

            zip_filename = f'postal_analytics_{file_type}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'

            return send_file(
                temp_zip.name,
                as_attachment=True,
                download_name=zip_filename,
                mimetype='application/zip'
            )

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500


@files_ns.route('/latest/<string:data_type>')
class LatestFile(Resource):
    @files_ns.doc('get_latest_file')
    @files_ns.param('data_type', 'Тип даних', enum=['courier', 'delivery'])
    def get(self, data_type):
        """Отримання найновішого файлу певного типу"""
        try:
            patterns = {
                'courier': 'courier_delivery_raw_data_*.csv',
                'delivery': 'delivery_periodic_raw_data_*.csv'
            }

            if data_type not in patterns:
                return {
                    'success': False,
                    'error': f'Невідомий тип даних: {data_type}',
                    'timestamp': datetime.now().isoformat()
                }, 400

            file_path = get_latest_csv_file(config.RAW_DATA_PATH, patterns[data_type])

            if not file_path:
                return {
                    'success': False,
                    'error': f'Файл типу {data_type} не знайдено',
                    'timestamp': datetime.now().isoformat()
                }, 404

            return send_file(
                file_path,
                as_attachment=True,
                download_name=os.path.basename(file_path)
            )

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }, 500


if __name__ == '__main__':
    print("🚀 Запуск PostDW Analytics API...")
    print("📖 Swagger документація: http://localhost:5000/swagger/")
    print("🏥 Health check: http://localhost:5000/api/v1/health/")
    print("\n📋 Доступні endpoints:")
    print("   GET /api/v1/data/extract - Вивантаження даних")
    print("   GET /api/v1/analysis/courier - Аналіз кур'єрів")
    print("   GET /api/v1/analysis/department - Аналіз відділень")
    print("   GET /api/v1/analysis/processing-time - Аналіз часу обробки")
    print("   GET /api/v1/analysis/transport - Аналіз транспорту")
    print("   GET /api/v1/analysis/all - Всі аналізи")
    print("   GET /api/v1/reports/generate - Генерація звітів")
    print("   GET /api/v1/reports/charts - Створення графіків")
    app.run(debug=True, host='0.0.0.0', port=5000)