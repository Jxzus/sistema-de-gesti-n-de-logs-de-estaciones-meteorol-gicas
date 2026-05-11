-- =============================================================================
-- Sistema de Gestión de Logs de Estaciones Meteorológicas
-- Script de inicialización de base de datos PostgreSQL
-- =============================================================================

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- -----------------------------------------------------------------------------
-- Tabla principal de logs meteorológicos
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_logs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id      VARCHAR(50)     NOT NULL,
    station_name    VARCHAR(100)    NOT NULL,
    latitude        DECIMAL(9, 6)   NOT NULL,
    longitude       DECIMAL(9, 6)   NOT NULL,
    timestamp       TIMESTAMPTZ     NOT NULL,
    received_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Mediciones meteorológicas
    temperature     DECIMAL(5, 2),   -- Celsius: -90 a 60
    humidity        DECIMAL(5, 2),   -- Porcentaje: 0 a 100
    pressure        DECIMAL(7, 2),   -- hPa: 870 a 1084
    wind_speed      DECIMAL(6, 2),   -- km/h: 0 a 400
    wind_direction  SMALLINT,        -- Grados: 0 a 360
    precipitation   DECIMAL(6, 2),   -- mm: 0 a 500
    uv_index        DECIMAL(4, 1),   -- Índice UV: 0 a 20
    visibility      DECIMAL(6, 2),   -- km: 0 a 100

    -- Metadatos del procesamiento
    raw_message     JSONB           NOT NULL,
    is_valid        BOOLEAN         NOT NULL DEFAULT TRUE,
    validation_errors JSONB,
    processing_time_ms INTEGER,

    -- Alertas
    has_alert       BOOLEAN         NOT NULL DEFAULT FALSE,
    alert_type      VARCHAR(50),
    alert_severity  VARCHAR(20),     -- LOW, MEDIUM, HIGH, CRITICAL

    CONSTRAINT chk_temperature   CHECK (temperature   BETWEEN -90  AND 60),
    CONSTRAINT chk_humidity      CHECK (humidity      BETWEEN 0    AND 100),
    CONSTRAINT chk_pressure      CHECK (pressure      BETWEEN 870  AND 1084),
    CONSTRAINT chk_wind_speed    CHECK (wind_speed    BETWEEN 0    AND 400),
    CONSTRAINT chk_wind_direction CHECK (wind_direction BETWEEN 0  AND 360),
    CONSTRAINT chk_precipitation CHECK (precipitation BETWEEN 0    AND 500),
    CONSTRAINT chk_uv_index      CHECK (uv_index      BETWEEN 0    AND 20),
    CONSTRAINT chk_visibility    CHECK (visibility    BETWEEN 0    AND 100)
);

-- -----------------------------------------------------------------------------
-- Tabla de estaciones meteorológicas registradas
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_stations (
    station_id      VARCHAR(50)     PRIMARY KEY,
    station_name    VARCHAR(100)    NOT NULL,
    location        VARCHAR(200),
    latitude        DECIMAL(9, 6)   NOT NULL,
    longitude       DECIMAL(9, 6)   NOT NULL,
    altitude_m      DECIMAL(7, 2),
    country         VARCHAR(100),
    region          VARCHAR(100),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    registered_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ,
    metadata        JSONB
);

-- -----------------------------------------------------------------------------
-- Tabla de alertas generadas
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_alerts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    log_id          UUID            REFERENCES weather_logs(id),
    station_id      VARCHAR(50)     NOT NULL,
    alert_type      VARCHAR(50)     NOT NULL,
    severity        VARCHAR(20)     NOT NULL,
    value           DECIMAL(10, 4),
    threshold       DECIMAL(10, 4),
    message         TEXT            NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    acknowledged    BOOLEAN         NOT NULL DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by VARCHAR(100)
);

-- -----------------------------------------------------------------------------
-- Tabla de métricas del sistema (para Prometheus / monitoreo interno)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_metrics (
    id              BIGSERIAL       PRIMARY KEY,
    metric_name     VARCHAR(100)    NOT NULL,
    metric_value    DECIMAL(15, 4)  NOT NULL,
    labels          JSONB,
    recorded_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Tabla de errores de procesamiento
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processing_errors (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    error_type      VARCHAR(100)    NOT NULL,
    error_message   TEXT            NOT NULL,
    raw_message     JSONB,
    station_id      VARCHAR(50),
    occurred_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    resolved        BOOLEAN         NOT NULL DEFAULT FALSE,
    stack_trace     TEXT
);

-- -----------------------------------------------------------------------------
-- Índices para optimizar consultas frecuentes
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_weather_logs_station_id
    ON weather_logs(station_id);

CREATE INDEX IF NOT EXISTS idx_weather_logs_timestamp
    ON weather_logs(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_weather_logs_station_timestamp
    ON weather_logs(station_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_weather_logs_received_at
    ON weather_logs(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_weather_logs_has_alert
    ON weather_logs(has_alert) WHERE has_alert = TRUE;

CREATE INDEX IF NOT EXISTS idx_weather_logs_is_valid
    ON weather_logs(is_valid) WHERE is_valid = FALSE;

CREATE INDEX IF NOT EXISTS idx_weather_alerts_station_id
    ON weather_alerts(station_id);

CREATE INDEX IF NOT EXISTS idx_weather_alerts_created_at
    ON weather_alerts(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_weather_alerts_acknowledged
    ON weather_alerts(acknowledged) WHERE acknowledged = FALSE;

CREATE INDEX IF NOT EXISTS idx_system_metrics_name_time
    ON system_metrics(metric_name, recorded_at DESC);

-- -----------------------------------------------------------------------------
-- Vista para estadísticas por estación
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_station_stats AS
SELECT
    station_id,
    COUNT(*)                            AS total_logs,
    COUNT(*) FILTER (WHERE is_valid)    AS valid_logs,
    COUNT(*) FILTER (WHERE has_alert)   AS logs_with_alerts,
    MIN(timestamp)                      AS first_reading,
    MAX(timestamp)                      AS last_reading,
    AVG(temperature)                    AS avg_temperature,
    MIN(temperature)                    AS min_temperature,
    MAX(temperature)                    AS max_temperature,
    AVG(humidity)                       AS avg_humidity,
    AVG(pressure)                       AS avg_pressure,
    AVG(wind_speed)                     AS avg_wind_speed,
    MAX(wind_speed)                     AS max_wind_speed,
    SUM(precipitation)                  AS total_precipitation,
    AVG(processing_time_ms)             AS avg_processing_time_ms
FROM weather_logs
GROUP BY station_id;

-- -----------------------------------------------------------------------------
-- Vista para alertas no reconocidas
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT
    wa.*,
    ws.station_name,
    ws.location,
    ws.country
FROM weather_alerts wa
LEFT JOIN weather_stations ws ON wa.station_id = ws.station_id
WHERE wa.acknowledged = FALSE
ORDER BY
    CASE wa.severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        WHEN 'LOW'      THEN 4
        ELSE 5
    END,
    wa.created_at DESC;

-- -----------------------------------------------------------------------------
-- Función para limpiar métricas antiguas (mantenimiento)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cleanup_old_metrics(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM system_metrics
    WHERE recorded_at < NOW() - MAKE_INTERVAL(days => days_to_keep);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- Datos iniciales de estaciones de ejemplo
-- -----------------------------------------------------------------------------
INSERT INTO weather_stations (station_id, station_name, location, latitude, longitude, altitude_m, country, region)
VALUES
    ('WS-COL-001', 'Estación Bogotá Centro',    'Bogotá, DC',       4.6097,  -74.0817, 2625, 'Colombia', 'Cundinamarca'),
    ('WS-COL-002', 'Estación Medellín Poblado', 'Medellín',         6.2087,  -75.5742, 1495, 'Colombia', 'Antioquia'),
    ('WS-COL-003', 'Estación Cartagena Puerto', 'Cartagena',        10.3910, -75.4794,    0, 'Colombia', 'Bolívar'),
    ('WS-COL-004', 'Estación Cali Norte',       'Cali',             3.4516,  -76.5320, 1018, 'Colombia', 'Valle del Cauca'),
    ('WS-COL-005', 'Estación Leticia Amazónica','Leticia',         -4.2153,  -69.9406,   82, 'Colombia', 'Amazonas')
ON CONFLICT (station_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Grants de permisos (usuario de la aplicación)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'weather_app') THEN
        CREATE ROLE weather_app LOGIN PASSWORD 'weather_secure_pass_2024';
    END IF;
END
$$;

GRANT SELECT, INSERT, UPDATE ON weather_logs, weather_stations, weather_alerts, processing_errors TO weather_app;
GRANT INSERT ON system_metrics TO weather_app;
GRANT SELECT ON v_station_stats, v_active_alerts TO weather_app;
GRANT USAGE, SELECT ON SEQUENCE system_metrics_id_seq TO weather_app;
GRANT EXECUTE ON FUNCTION cleanup_old_metrics TO weather_app;

-- Confirmación
SELECT 'Base de datos inicializada correctamente' AS status,
       NOW() AS initialized_at;
