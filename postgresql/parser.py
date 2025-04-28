import psycopg2
from datetime import datetime
import requests
from tqdm import tqdm
import os

host = os.environ['HOST']
port = os.environ['PORT']
database = os.environ['DBNAME']
user = os.environ['USERNAME']
password = os.environ['PWD']

response = requests.get('https://pulkovoairport.ru/api/?type=departure&when=0')
response.raise_for_status()
flights = response.json()


def insert_if_not_exists(cur, table, unique_field, value, other_fields):
    cur.execute(f"SELECT {unique_field} FROM airport.{table} WHERE {unique_field} = %s", (value,))
    if cur.fetchone() is None:
        fields = ', '.join([unique_field] + list(other_fields.keys()))
        placeholders = ', '.join(['%s'] * (1 + len(other_fields)))
        cur.execute(
            f"INSERT INTO airport.{table} ({fields}) VALUES ({placeholders})",
            (value, *other_fields.values())
        )


def truncate_code(code, length=3):
    return code[:length] if code else 'UNK'


def parse_date(date_str, default_date=None):
    try:
        return datetime.fromisoformat(date_str)
    except (TypeError, ValueError):
        return default_date


def insert_aircraft_type_if_not_exists(cur, aircraft_type_id, model_name="Unknown Model",
                                       manufacturer="Unknown Manufacturer", capacity=0, max_range_km=0,
                                       cruise_speed_kmh=0, length_m=0, wingspan_m=0, height_m=0, mtow_kg=0,
                                       engine_count=0, engine_type="Unknown", now=None):
    cur.execute("SELECT aircraft_type_id FROM airport.aircraft_types WHERE aircraft_type_id = %s", (aircraft_type_id,))
    if cur.fetchone() is None:
        cur.execute(
            """
            INSERT INTO airport.aircraft_types (
                aircraft_type_id, model_name, manufacturer, capacity, max_range_km, cruise_speed_kmh, 
                length_m, wingspan_m, height_m, mtow_kg, engine_count, engine_type, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                aircraft_type_id, model_name, manufacturer, capacity, max_range_km, cruise_speed_kmh,
                length_m, wingspan_m, height_m, mtow_kg, engine_count, engine_type, now, now
            )
        )


def insert_airline_if_not_exists(cur, airline_id, airline_name="Unknown Airline", iata_code="UNK", icao_code="UNK",
                                 country="Unknown", headquarters="Unknown", founded_year=2000, now=None):
    cur.execute("SELECT airline_id FROM airport.airlines WHERE airline_id = %s", (airline_id,))
    if cur.fetchone() is None:
        cur.execute(
            """
            INSERT INTO airport.airlines (
                airline_id, airline_name, iata_code, icao_code, country, headquarters, founded_year,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                airline_id, airline_name, iata_code, icao_code, country, headquarters, founded_year,
                now, now
            )
        )


def insert_aircraft_if_not_exists(cur, aircraft_id, registration_number="UNK", aircraft_type_id=1, airline_id=1,
                                  now=None):
    cur.execute("SELECT aircraft_id FROM airport.aircrafts WHERE registration_number = %s", (registration_number,))
    if cur.fetchone() is None:
        cur.execute(
            """
            INSERT INTO airport.aircrafts (
                aircraft_id, registration_number, aircraft_type_id, airline_id, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                aircraft_id, registration_number, aircraft_type_id, airline_id, 'active', now, now
            )
        )


def insert_airport_if_not_exists(cur, airport_id, airport_code, airport_name="Unknown Airport", city="Unknown",
                                 country="Unknown", now=None):
    cur.execute("SELECT airport_id FROM airport.airports WHERE airport_id = %s", (airport_id,))
    if cur.fetchone() is None:
        icao_code = airport_code
        cur.execute(
            """
            INSERT INTO airport.airports (
                airport_id, iata_code, icao_code, airport_name, city, country, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                airport_id, airport_code, icao_code, airport_name, city, country, now, now
            )
        )


def main():
    try:
        conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        cur = conn.cursor()

        now = datetime.now()

        insert_airline_if_not_exists(
            cur,
            1,  # airline_id = 1
            airline_name="Sample Airline",
            iata_code="SA",
            icao_code="SMP",
            country="Unknown",
            headquarters="Unknown HQ",
            founded_year=2000,
            now=now
        )

        insert_aircraft_type_if_not_exists(
            cur,
            1,
            model_name="Boeing 737",
            manufacturer="Boeing",
            capacity=215,
            max_range_km=5400,
            cruise_speed_kmh=850,
            length_m=39.5,
            wingspan_m=34.3,
            height_m=12.5,
            mtow_kg=78500,
            engine_count=2,
            engine_type="Turbofan",
            now=now
        )

        insert_airport_if_not_exists(cur, 1, "SVO", "Sheremetyevo Airport", "Moscow", "Russia", now)
        insert_airport_if_not_exists(cur, 2, "LED", "Pulkovo Airport", "Saint Petersburg", "Russia", now)

        insert_aircraft_if_not_exists(cur, 1, registration_number="N4 321", aircraft_type_id=1, airline_id=1, now=now)

        for f in tqdm(flights):
            flight_number = f.get('OD_FLIGHT_NUMBER', 'UNKNOWN')
            airline_code = f.get('OD_RAL_CODE', 'UNK')
            plane_code = f.get('OD_RAC_CODE', 'UNK')
            dep_airport_iata = f.get('OD_RAP_CODE_NEXT', 'UNK')
            arr_airport_iata = f.get('OD_RAP_CODE_DESTINATION', 'UNK')
            scheduled_departure = f.get('OD_STD', None)
            scheduled_arrival = f.get('OD_RAP_CODE_DESTINATION_STA', None)

            airline_code = truncate_code(airline_code, 3)  # Обрезка до 3 символов
            dep_airport_iata = truncate_code(dep_airport_iata, 3)
            arr_airport_iata = truncate_code(arr_airport_iata, 3)
            plane_code = truncate_code(plane_code, 3)

            default_date = now.replace(year=2025)
            scheduled_departure_dt = parse_date(scheduled_departure, default_date)
            scheduled_arrival_dt = parse_date(scheduled_arrival, default_date)

            insert_if_not_exists(
                cur,
                table='airlines',
                unique_field='iata_code',
                value=airline_code,
                other_fields={
                    'airline_name': f.get('OD_RAL_NAME_EN', 'Unknown Airline'),
                    'icao_code': airline_code,
                    'country': 'Unknown',
                    'headquarters': 'Unknown',
                    'founded_year': 2000,
                    'created_at': now,
                    'updated_at': now
                }
            )

            for airport_code in [dep_airport_iata, arr_airport_iata]:
                insert_if_not_exists(
                    cur,
                    table='airports',
                    unique_field='iata_code',
                    value=airport_code,
                    other_fields={
                        'airport_name': 'Unknown Airport',
                        'icao_code': airport_code,
                        'city': 'Unknown',
                        'country': 'Unknown',
                        'created_at': now,
                        'updated_at': now
                    }
                )

            cur.execute(
                """
                INSERT INTO airport.flights (
                    updated_at, status, scheduled_departure, scheduled_arrival,
                    flight_number, departure_airport_id, arrival_airport_id, created_at,
                    airline_id, aircraft_id, actual_departure, actual_arrival
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    now, 'scheduled', scheduled_departure_dt, scheduled_arrival_dt,
                    flight_number, 1, 2, now, 1, 1, None, None
                )
            )

        conn.commit()
        print("Данные успешно загружены!")

    except Exception as e:
        print("Ошибка:", e)


if __name__ == '__main__':
    main()
