"""
SQL запити для отримання сирих даних з Data Warehouse
Назви полів згідно реальної схеми БД
"""

class DWQueries:
    """Клас з SQL запитами для отримання сирих даних з DW"""

    @staticmethod
    def get_courier_delivery_data():
        """
        Завдання 1: Сирі дані кур'єрської доставки з усіма вимірами
        """
        return """
        SELECT 
            cd.courier_delivery_id,
            cd.courier_id,
            c.full_name as courier_name,
            c.phone as courier_phone,
            cd.location_id,
            l.city_name,
            l.region_name,
            l.country_name,
            cd.parcel_id,
            p.description as parcel_description,
            p.weight as parcel_weight,
            p.size as parcel_size,
            cd.delivery_duration as delivery_time_minutes,
            cd.previous_delta_delivery_duration as improvement_minutes,
            cd.recive_date_id,
            cd.issue_date_id
        FROM CourierDeliveryTransactionalFact cd
        INNER JOIN CourierDim c ON cd.courier_id = c.courier_id
        INNER JOIN LocationDim l ON cd.location_id = l.location_id
        INNER JOIN ParcelDim p ON cd.parcel_id = p.parcel_id
        ORDER BY cd.courier_delivery_id DESC
        """

    @staticmethod
    def get_delivery_periodic_data():
        """
        Завдання 2, 3, 4: Сирі дані з DeliveryPeriodicFact для всіх аналізів
        - Завдання 2: Аналіз завантажень відділень
        - Завдання 3: Аналіз часу обробки посилок
        - Завдання 4: Аналіз використання транспорту
        """
        return """
        SELECT 
            dpf.delivery_id,
            dpf.department_id,
            d.number as department_number,
            d.address as department_address,
            d.department_type,
            dl.city_name as department_city,
            dl.region_name as department_region,
            dl.country_name as department_country,
            dpf.parcel_type_id,
            pt.name as parcel_type_name,
            pt.max_size as parcel_max_size,
            pt.max_weight as parcel_max_weight,
            dpf.transport_body_type_id,
            tb.name as transport_type_name,
            dpf.start_period_id,
            dd_start.date_year as start_year,
            dd_start.date_month as start_month,
            dd_start.date_day as start_day,
            dpf.end_period_id,
            dd_end.date_year as end_year,
            dd_end.date_month as end_month,
            dd_end.date_day as end_day,
            dpf.deliveries_count,
            dpf.average_dwell_parcel_type as processing_time_hours,
            dpf.deliveries_share_percentage
        FROM DeliveryPeriodicFact dpf
        INNER JOIN DepartmentDim d ON dpf.department_id = d.department_id
        INNER JOIN LocationDim dl ON d.department_location_id = dl.location_id
        INNER JOIN ParcelTypeDim pt ON dpf.parcel_type_id = pt.parcel_type_id
        INNER JOIN TransportBodyTypeDim tb ON dpf.transport_body_type_id = tb.transport_body_id
        INNER JOIN DateDim dd_start ON dpf.start_period_id = dd_start.date_id
        INNER JOIN DateDim dd_end ON dpf.end_period_id = dd_end.date_id
        ORDER BY dpf.delivery_id DESC
        """