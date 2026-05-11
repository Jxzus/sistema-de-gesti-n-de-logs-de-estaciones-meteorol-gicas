"""
=============================================================================
Sistema de Gestión de Logs de Estaciones Meteorológicas
Consumidor: procesa mensajes de RabbitMQ y persiste en PostgreSQL
=============================================================================
"""

import json
import logging
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

import pika
import pika.exceptions
import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2 import OperationalError as PGError

# =============================================================================
# Configuración de logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("weather.consumer")

# =============================================================================
# Configuración desde variables de entorno
# =============================================================================
RABBITMQ_HOST    = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT    = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER    = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS    = os.getenv("RABBITMQ_PASS", "admin_password")
RABBITMQ_VHOST   = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME    = os.getenv("EXCHANGE_NAME", "weather.exchange")
QUEUE_NAME       = os.getenv("QUEUE_NAME", "weather.readings.queue")
ROUTING_KEY      = os.getenv("ROUTING_KEY", "weather.readings")
PREFETCH_COUNT   = int(os.getenv("PREFETCH_COUNT", "1"))
MAX_RETRIES      = int(os.getenv("MAX_RETRIES", "10"))
RETRY_DELAY      = float(os.getenv("RETRY_DELAY", "5.0"))

PG_HOST          = os.getenv("PG_HOST", "postgres")
PG_PORT          = int(os.getenv("PG_PORT", "5432"))
PG_DB            = os.getenv("PG_DB", "weather_db")
PG_USER          = os.getenv("PG_USER", "weather_user")
PG_PASS          = os.getenv("PG_PASS", "weather_password")
PG_MIN_CONN      = int(os.getenv("PG_MIN_CONN", "1"))
PG_MAX_CONN      = int(os.getenv("PG_MAX_CONN", "5"))


# =============================================================================
# Umbrales para generación de alertas
# =============================================================================
ALERT_THRESHOLDS = {
    "temperature": {
        "HIGH":     {"min": -50, "max": 40,  "severity": "HIGH"},
        "CRITICAL": {"min": -70, "max": 55,  "severity": "CRITICAL"},
    },
    "wind_speed": {
        "HIGH":     {"max": 80,  "severity": "HIGH"},
        "CRITICAL": {"max": 150, "severity": "CRITICAL"},
    },
    "humidity": {
        "LOW":      {"min": 10,  "severity": "LOW"},
    },
    "pressure": {
        "LOW":      {"min": 950, "max": 1050, "severity": "LOW"},
    },
}

# Rangos de validación estricta
VALID_RANGES = {
    "temperature":    (-90.0, 60.0),
    "humidity":       (0.0, 100.0),
    "pressure":       (870.0, 1084.0),
    "wind_speed":     (0.0, 400.0),
    "wind_direction": (0, 360),
    "precipitation":  (0.0, 500.0),
    "uv_index":       (0.0, 20.0),
    "visibility":     (0.0, 100.0),
}

REQUIRED_FIELDS = ["station_id", "station_name", "latitude", "longitude", "timestamp"]


# =============================================================================
# Validador de mensajes
# =============================================================================
class WeatherValidator:
    """Valida los datos meteorológicos recibidos."""

    @staticmethod
    def validate(data: dict) -> tuple[bool, list[str]]:
        """
        Valida un mensaje meteorológico.

        Args:
            data: Diccionario con los datos del mensaje.

        Returns:
            Tupla (es_válido, lista_de_errores).
        """
        errors: list[str] = []

        # Campos requeridos
        for field in REQUIRED_FIELDS:
            if field not in data or data[field] is None:
                errors.append(f"Campo requerido ausente: '{field}'")

        # Validar rangos
        for field, (min_val, max_val) in VALID_RANGES.items():
            value = data.get(field)
            if value is not None:
                try:
                    val = float(value)
                    if not (min_val <= val <= max_val):
                        errors.append(
                            f"Campo '{field}' fuera de rango: {val} "
                            f"(esperado {min_val} a {max_val})"
                        )
                except (TypeError, ValueError):
                    errors.append(f"Campo '{field}' tiene valor no numérico: {value}")

        # Validar timestamp
        if "timestamp" in data and data["timestamp"]:
            try:
                datetime.fromisoformat(str(data["timestamp"]).replace("Z", "+00:00"))
            except ValueError:
                errors.append(f"Timestamp inválido: {data.get('timestamp')}")

        is_valid = len(errors) == 0
        return is_valid, errors

    @staticmethod
    def check_alerts(data: dict) -> list[dict]:
        """
        Verifica si los valores superan umbrales de alerta.

        Args:
            data: Diccionario con los datos del mensaje.

        Returns:
            Lista de alertas generadas.
        """
        alerts = []

        # Temperatura extrema
        temp = data.get("temperature")
        if temp is not None:
            if float(temp) >= 40:
                alerts.append({
                    "type": "HIGH_TEMPERATURE",
                    "severity": "CRITICAL" if float(temp) >= 50 else "HIGH",
                    "value": float(temp),
                    "threshold": 40,
                    "message": f"Temperatura muy alta: {temp}°C"
                })
            elif float(temp) <= -20:
                alerts.append({
                    "type": "LOW_TEMPERATURE",
                    "severity": "HIGH",
                    "value": float(temp),
                    "threshold": -20,
                    "message": f"Temperatura muy baja: {temp}°C"
                })

        # Viento extremo
        wind = data.get("wind_speed")
        if wind is not None and float(wind) >= 80:
            alerts.append({
                "type": "HIGH_WIND",
                "severity": "CRITICAL" if float(wind) >= 120 else "HIGH",
                "value": float(wind),
                "threshold": 80,
                "message": f"Viento extremo: {wind} km/h"
            })

        # Lluvia intensa
        rain = data.get("precipitation")
        if rain is not None and float(rain) >= 50:
            alerts.append({
                "type": "HEAVY_RAIN",
                "severity": "HIGH",
                "value": float(rain),
                "threshold": 50,
                "message": f"Lluvia intensa: {rain} mm"
            })

        return alerts


# =============================================================================
# Gestor de base de datos PostgreSQL
# =============================================================================
class DatabaseManager:
    """
    Gestiona conexiones y operaciones con PostgreSQL.

    Usa un pool de conexiones con reconexión automática.
    """

    def __init__(self):
        self.pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._connect()

    def _connect(self):
        """Crea el pool de conexiones."""
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Conectando a PostgreSQL {PG_HOST}:{PG_PORT}/{PG_DB}...")
                self.pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn  = PG_MIN_CONN,
                    maxconn  = PG_MAX_CONN,
                    host     = PG_HOST,
                    port     = PG_PORT,
                    dbname   = PG_DB,
                    user     = PG_USER,
                    password = PG_PASS,
                    connect_timeout = 10,
                    options  = "-c application_name=weather_consumer",
                )
                logger.info("✓ Pool de conexiones PostgreSQL creado")
                return
            except PGError as e:
                wait = min(RETRY_DELAY * (2 ** attempt), 60)
                logger.warning(f"PG no disponible (intento {attempt + 1}): {e}. Esperando {wait}s...")
                time.sleep(wait)

        logger.critical("No se pudo conectar a PostgreSQL. Saliendo.")
        sys.exit(1)

    def get_connection(self):
        """Obtiene una conexión del pool con reconexión automática."""
        try:
            conn = self.pool.getconn()
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.error(f"Error obteniendo conexión del pool: {e}")
            self._connect()  # Recrear pool
            return self.pool.getconn()

    def return_connection(self, conn, error: bool = False):
        """Devuelve una conexión al pool."""
        try:
            self.pool.putconn(conn, close=error)
        except Exception as e:
            logger.warning(f"Error devolviendo conexión al pool: {e}")

    def save_log(self, data: dict, is_valid: bool, validation_errors: list,
                 alerts: list, processing_time_ms: int) -> Optional[str]:
        """
        Persiste un log meteorológico en la base de datos.

        Args:
            data: Datos del mensaje.
            is_valid: Si el mensaje pasó validación.
            validation_errors: Lista de errores de validación.
            alerts: Lista de alertas generadas.
            processing_time_ms: Tiempo de procesamiento en ms.

        Returns:
            UUID del registro insertado, o None en caso de error.
        """
        conn = self.get_connection()
        error_occurred = False
        try:
            with conn.cursor() as cur:
                # Determinar alerta principal
                primary_alert = alerts[0] if alerts else None

                cur.execute("""
                    INSERT INTO weather_logs (
                        station_id, station_name, latitude, longitude,
                        timestamp, received_at,
                        temperature, humidity, pressure,
                        wind_speed, wind_direction, precipitation,
                        uv_index, visibility,
                        raw_message, is_valid, validation_errors,
                        processing_time_ms,
                        has_alert, alert_type, alert_severity
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, NOW(),
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s, %s
                    )
                    RETURNING id
                """, (
                    data.get("station_id"),
                    data.get("station_name"),
                    data.get("latitude"),
                    data.get("longitude"),
                    data.get("timestamp"),
                    data.get("temperature"),
                    data.get("humidity"),
                    data.get("pressure"),
                    data.get("wind_speed"),
                    data.get("wind_direction"),
                    data.get("precipitation"),
                    data.get("uv_index"),
                    data.get("visibility"),
                    json.dumps(data),
                    is_valid,
                    json.dumps(validation_errors) if validation_errors else None,
                    processing_time_ms,
                    bool(alerts),
                    primary_alert["type"] if primary_alert else None,
                    primary_alert["severity"] if primary_alert else None,
                ))

                log_id = cur.fetchone()[0]

                # Persistir alertas
                for alert in alerts:
                    cur.execute("""
                        INSERT INTO weather_alerts (
                            log_id, station_id, alert_type, severity,
                            value, threshold, message
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        log_id,
                        data.get("station_id"),
                        alert["type"],
                        alert["severity"],
                        alert.get("value"),
                        alert.get("threshold"),
                        alert["message"],
                    ))

                # Actualizar last_seen en weather_stations si existe
                cur.execute("""
                    UPDATE weather_stations
                    SET last_seen_at = NOW()
                    WHERE station_id = %s
                """, (data.get("station_id"),))

                conn.commit()
                return str(log_id)

        except Exception as e:
            error_occurred = True
            conn.rollback()
            logger.error(f"Error persistiendo log en BD: {e}")
            self._save_error(data, str(e), traceback.format_exc())
            return None
        finally:
            self.return_connection(conn, error=error_occurred)

    def _save_error(self, data: Any, error_msg: str, stack_trace: str):
        """Persiste un error de procesamiento."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO processing_errors (
                        error_type, error_message, raw_message, station_id, stack_trace
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    "DATABASE_ERROR",
                    error_msg[:1000],
                    json.dumps(data) if isinstance(data, dict) else str(data),
                    data.get("station_id") if isinstance(data, dict) else None,
                    stack_trace[:5000],
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Error guardando error de procesamiento: {e}")
        finally:
            if conn:
                self.return_connection(conn)


# =============================================================================
# Clase principal del Consumidor
# =============================================================================
class WeatherConsumer:
    """
    Consumidor de mensajes meteorológicos desde RabbitMQ.

    Características:
    - ACK manual para garantizar entrega exactamente-una-vez
    - prefetch_count=1 para procesamiento ordenado
    - Reconexión automática
    - Validación de datos y gestión de errores
    """

    def __init__(self):
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.db = DatabaseManager()
        self.validator = WeatherValidator()
        self.running = True
        self.messages_processed = 0
        self.messages_failed = 0
        self.messages_rejected = 0
        self._setup_signals()

    def _setup_signals(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info(f"Señal {signum} recibida. Iniciando apagado limpio...")
        self.running = False
        if self.channel:
            try:
                self.channel.stop_consuming()
            except Exception:
                pass

    def _get_connection_params(self) -> pika.ConnectionParameters:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        return pika.ConnectionParameters(
            host         = RABBITMQ_HOST,
            port         = RABBITMQ_PORT,
            virtual_host = RABBITMQ_VHOST,
            credentials  = credentials,
            heartbeat    = 60,
            blocked_connection_timeout = 300,
        )

    def connect(self) -> bool:
        """Conecta a RabbitMQ y configura cola y exchange."""
        try:
            logger.info(f"Conectando a RabbitMQ en {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
            self.connection = pika.BlockingConnection(self._get_connection_params())
            self.channel = self.connection.channel()

            # Configurar QoS: procesar de a 1 mensaje
            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)

            # Declarar exchange durable
            self.channel.exchange_declare(
                exchange      = EXCHANGE_NAME,
                exchange_type = "topic",
                durable       = True,
            )

            # Declarar cola durable con DLX (Dead Letter Exchange)
            args = {
                "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx",
                "x-message-ttl": 86400000,  # 24 horas en ms
            }
            self.channel.queue_declare(
                queue     = QUEUE_NAME,
                durable   = True,
                arguments = args,
            )

            # Declarar cola de mensajes muertos (DLQ)
            self.channel.exchange_declare(
                exchange      = f"{EXCHANGE_NAME}.dlx",
                exchange_type = "fanout",
                durable       = True,
            )
            self.channel.queue_declare(
                queue   = f"{QUEUE_NAME}.dead",
                durable = True,
            )
            self.channel.queue_bind(
                queue    = f"{QUEUE_NAME}.dead",
                exchange = f"{EXCHANGE_NAME}.dlx",
            )

            # Bind de la cola principal al exchange
            self.channel.queue_bind(
                queue       = QUEUE_NAME,
                exchange    = EXCHANGE_NAME,
                routing_key = ROUTING_KEY,
            )

            logger.info(f"✓ Cola '{QUEUE_NAME}' configurada con prefetch={PREFETCH_COUNT}")
            return True

        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error de conexión AMQP: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado al conectar: {e}")
            return False

    def connect_with_retry(self):
        """Reintenta conexión con backoff exponencial."""
        retries = 0
        while self.running and retries < MAX_RETRIES:
            if self.connect():
                return
            retries += 1
            delay = min(RETRY_DELAY * (2 ** (retries - 1)), 60)
            logger.warning(f"Reintento {retries}/{MAX_RETRIES} en {delay:.1f}s...")
            time.sleep(delay)

        if not self.running:
            return
        logger.critical("No se pudo conectar a RabbitMQ tras múltiples intentos. Saliendo.")
        sys.exit(1)

    def process_message(self, body: bytes, delivery_tag: int) -> bool:
        """
        Procesa un mensaje individual.

        Args:
            body: Cuerpo del mensaje en bytes.
            delivery_tag: Tag de entrega para ACK/NACK.

        Returns:
            True si el procesamiento fue exitoso.
        """
        start_time = time.time()

        try:
            # Deserializar JSON
            data = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Error deserializando mensaje: {e}")
            return False

        station_id = data.get("station_id", "UNKNOWN")
        msg_id     = data.get("message_id", "NO-ID")[:8]

        # Validar datos
        is_valid, errors = self.validator.validate(data)
        if not is_valid:
            logger.warning(f"[INVALID] station={station_id} msg={msg_id} errors={errors}")

        # Detectar alertas
        alerts = self.validator.check_alerts(data)
        for alert in alerts:
            logger.warning(
                f"[ALERT] station={station_id} type={alert['type']} "
                f"severity={alert['severity']} msg={alert['message']}"
            )

        # Calcular tiempo de procesamiento
        processing_time_ms = int((time.time() - start_time) * 1000)

        # Persistir en PostgreSQL
        log_id = self.db.save_log(data, is_valid, errors, alerts, processing_time_ms)

        if log_id:
            logger.info(
                f"[✓ SAVED] station={station_id} msg={msg_id} "
                f"log_id={str(log_id)[:8]}... valid={is_valid} "
                f"alerts={len(alerts)} time={processing_time_ms}ms"
            )
            return True
        else:
            logger.error(f"[✗ FAILED] No se pudo persistir log station={station_id}")
            return False

    def on_message(self, channel, method, properties, body):
        """
        Callback invocado por pika al recibir un mensaje.

        Implementa ACK manual: solo confirma si el procesamiento fue exitoso.
        En caso de error, rechaza el mensaje (NACK) para que RabbitMQ
        lo reencole o lo envíe a la DLQ.
        """
        delivery_tag = method.delivery_tag

        try:
            success = self.process_message(body, delivery_tag)

            if success:
                channel.basic_ack(delivery_tag=delivery_tag)
                self.messages_processed += 1
            else:
                # Rechazar sin reencolar (va a DLQ)
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
                self.messages_failed += 1

        except Exception as e:
            logger.error(f"Error inesperado procesando mensaje: {e}\n{traceback.format_exc()}")
            try:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            except Exception:
                pass
            self.messages_rejected += 1

        # Log de métricas cada 100 mensajes
        total = self.messages_processed + self.messages_failed
        if total > 0 and total % 100 == 0:
            logger.info(
                f"[METRICS] Procesados={self.messages_processed} "
                f"Fallidos={self.messages_failed} "
                f"Rechazados={self.messages_rejected}"
            )

    def run(self):
        """Loop principal del consumidor."""
        logger.info("=" * 60)
        logger.info("  Weather Station Consumer - Iniciando")
        logger.info(f"  Queue: {QUEUE_NAME}")
        logger.info(f"  Prefetch: {PREFETCH_COUNT}")
        logger.info("=" * 60)

        self.connect_with_retry()

        self.channel.basic_consume(
            queue        = QUEUE_NAME,
            on_message_callback = self.on_message,
            auto_ack     = False,
        )

        logger.info(f"Esperando mensajes en '{QUEUE_NAME}'... (CTRL+C para detener)")

        while self.running:
            try:
                self.connection.process_data_events(time_limit=1)
            except pika.exceptions.AMQPConnectionError as e:
                if not self.running:
                    break
                logger.error(f"Conexión perdida: {e}. Reconectando...")
                time.sleep(RETRY_DELAY)
                self.connect_with_retry()
                self.channel.basic_consume(
                    queue               = QUEUE_NAME,
                    on_message_callback = self.on_message,
                    auto_ack            = False,
                )
            except Exception as e:
                if not self.running:
                    break
                logger.error(f"Error en loop de consumo: {e}")
                time.sleep(2)

        self._cleanup()

    def _cleanup(self):
        """Cierra conexiones limpiamente."""
        logger.info("Cerrando conexiones del consumer...")
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error al cerrar conexiones: {e}")
        finally:
            logger.info(
                f"Consumer detenido. Procesados={self.messages_processed} "
                f"Fallidos={self.messages_failed} Rechazados={self.messages_rejected}"
            )


# =============================================================================
# Punto de entrada
# =============================================================================
if __name__ == "__main__":
    consumer = WeatherConsumer()
    consumer.run()
