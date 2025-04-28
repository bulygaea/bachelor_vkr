-- Создание схемы для логического разделения объектов
CREATE SCHEMA airport;

-- Таблица авиакомпаний
CREATE TABLE airport.airlines (
    airline_id SERIAL PRIMARY KEY,
    airline_name VARCHAR(100) NOT NULL,
    iata_code CHAR(2) UNIQUE NOT NULL,
    icao_code CHAR(3) UNIQUE NOT NULL,
    country VARCHAR(50) NOT NULL,
    headquarters VARCHAR(100),
    founded_year INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица типов самолетов
CREATE TABLE airport.aircraft_types (
    aircraft_type_id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    manufacturer VARCHAR(100) NOT NULL,
    capacity INTEGER NOT NULL,
    max_range_km INTEGER NOT NULL,
    cruise_speed_kmh INTEGER NOT NULL,
    length_m NUMERIC(6,2) NOT NULL,
    wingspan_m NUMERIC(6,2) NOT NULL,
    height_m NUMERIC(6,2) NOT NULL,
    mtow_kg INTEGER NOT NULL, -- maximum takeoff weight
    engine_count INTEGER NOT NULL,
    engine_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица самолетов
CREATE TABLE airport.aircrafts (
    aircraft_id SERIAL PRIMARY KEY,
    registration_number VARCHAR(20) UNIQUE NOT NULL,
    aircraft_type_id INTEGER NOT NULL REFERENCES airport.aircraft_types(aircraft_type_id),
    airline_id INTEGER NOT NULL REFERENCES airport.airlines(airline_id),
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица аэропортов
CREATE TABLE airport.airports (
    airport_id SERIAL PRIMARY KEY,
    airport_name VARCHAR(100) NOT NULL,
    iata_code CHAR(3) UNIQUE NOT NULL,
    icao_code CHAR(4) UNIQUE NOT NULL,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица рейсов
CREATE TABLE airport.flights (
    flight_id SERIAL PRIMARY KEY,
    flight_number VARCHAR(10) NOT NULL,
    airline_id INTEGER NOT NULL REFERENCES airport.airlines(airline_id),
    aircraft_id INTEGER NOT NULL REFERENCES airport.aircrafts(aircraft_id),
    departure_airport_id INTEGER NOT NULL REFERENCES airport.airports(airport_id),
    arrival_airport_id INTEGER NOT NULL REFERENCES airport.airports(airport_id),
    scheduled_departure TIMESTAMP WITH TIME ZONE NOT NULL,
    scheduled_arrival TIMESTAMP WITH TIME ZONE NOT NULL,
    actual_departure TIMESTAMP WITH TIME ZONE,
    actual_arrival TIMESTAMP WITH TIME ZONE,
    status VARCHAR(127) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_different_airports CHECK (departure_airport_id != arrival_airport_id),
    CONSTRAINT check_departure_before_arrival CHECK (scheduled_departure <= scheduled_arrival)
);

-- Таблица расписаний регулярных рейсов
CREATE TABLE airport.flight_schedules (
    schedule_id SERIAL PRIMARY KEY,
    airline_id INTEGER NOT NULL REFERENCES airport.airlines(airline_id),
    flight_number VARCHAR(10) NOT NULL,
    departure_airport_id INTEGER NOT NULL REFERENCES airport.airports(airport_id),
    arrival_airport_id INTEGER NOT NULL REFERENCES airport.airports(airport_id),
    scheduled_departure_time TIME NOT NULL,
    scheduled_arrival_time TIME NOT NULL,
    days_of_operation VARCHAR(7) NOT NULL, -- e.g., "1234567" для каждого дня недели
    effective_from DATE NOT NULL,
    effective_until DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_different_schedule_airports CHECK (departure_airport_id != arrival_airport_id),
    CONSTRAINT check_valid_date_range CHECK (effective_from <= effective_until)
);

-- Таблица истории статусов рейсов
CREATE TABLE airport.flight_status_history (
    status_history_id SERIAL PRIMARY KEY,
    flight_id INTEGER NOT NULL REFERENCES airport.flights(flight_id),
    status VARCHAR(127) NOT NULL,
    status_time TIMESTAMP WITH TIME ZONE NOT NULL,
    remarks TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов

-- Индексы для рейсов
CREATE INDEX idx_flights_airline ON airport.flights(airline_id);
CREATE INDEX idx_flights_aircraft ON airport.flights(aircraft_id);
CREATE INDEX idx_flights_departure_airport ON airport.flights(departure_airport_id);
CREATE INDEX idx_flights_arrival_airport ON airport.flights(arrival_airport_id);
CREATE INDEX idx_flights_departure_date ON airport.flights(scheduled_departure);
CREATE INDEX idx_flights_status ON airport.flights(status);

-- Индексы для аэропортов
CREATE INDEX idx_airports_city ON airport.airports(city);
CREATE INDEX idx_airports_country ON airport.airports(country);

-- Индексы для истории статусов рейсов
CREATE INDEX idx_flight_status_history_flight ON airport.flight_status_history(flight_id);
CREATE INDEX idx_flight_status_history_time ON airport.flight_status_history(status_time);

-- Триггерные функции

-- Функция для обновления времени изменения записи
CREATE OR REPLACE FUNCTION airport.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггеры для автоматического обновления updated_at
CREATE TRIGGER update_airlines_timestamp
BEFORE UPDATE ON airport.airlines
FOR EACH ROW EXECUTE FUNCTION airport.update_timestamp();

CREATE TRIGGER update_aircraft_types_timestamp
BEFORE UPDATE ON airport.aircraft_types
FOR EACH ROW EXECUTE FUNCTION airport.update_timestamp();

CREATE TRIGGER update_aircrafts_timestamp
BEFORE UPDATE ON airport.aircrafts
FOR EACH ROW EXECUTE FUNCTION airport.update_timestamp();

CREATE TRIGGER update_airports_timestamp
BEFORE UPDATE ON airport.airports
FOR EACH ROW EXECUTE FUNCTION airport.update_timestamp();

CREATE TRIGGER update_flights_timestamp
BEFORE UPDATE ON airport.flights
FOR EACH ROW EXECUTE FUNCTION airport.update_timestamp();

-- Функция для добавления записи в историю статусов рейса
CREATE OR REPLACE FUNCTION airport.log_flight_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status <> NEW.status THEN
        INSERT INTO airport.flight_status_history (flight_id, status, status_time, reason)
        VALUES (NEW.id, NEW.status, CURRENT_TIMESTAMP, 'Автоматическое обновление статуса');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автоматического логирования изменений статуса рейса
CREATE TRIGGER log_flight_status
AFTER UPDATE OF status ON airport.flights
FOR EACH ROW EXECUTE FUNCTION airport.log_flight_status_change();