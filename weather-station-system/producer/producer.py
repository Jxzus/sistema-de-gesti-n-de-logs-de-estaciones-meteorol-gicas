"""
=============================================================================
Sistema de Gestión de Logs de Estaciones Meteorológicas
Productor de datos: simula lecturas de estaciones y publica en RabbitMQ
=============================================================================
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pika
import pika.exceptions

# =============================================================================
# Configuración de logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("weather.producer")

# =============================================================================
# Configuración desde variables de entorno
# =============================================================================
RABBITMQ_HOST     = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT     = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER     = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS     = os.getenv("RABBITMQ_PASS", "admin_password")
RABBITMQ_VHOST    = os.getenv("RABBITMQ_VHOST", "/")
EXCHANGE_NAME     = os.getenv("EXCHANGE_NAME", "weather.exchange")
ROUTING_KEY       = os.getenv("ROUTING_KEY", "weather.readings")
PUBLISH_INTERVAL  = float(os.getenv("PUBLISH_INTERVAL", "2.0"))   # segundos entre mensajes
STATIONS_PER_BATCH = int(os.getenv("STATIONS_PER_BATCH", "1"))    # estaciones por ciclo
MAX_RETRIES       = int(os.getenv("MAX_RETRIES", "10"))
RETRY_DELAY       = float(os.getenv("RETRY_DELAY", "5.0"))


# =============================================================================
# Modelos de datos
# =============================================================================
STATIONS = [
    {"id": "WS-COL-001", "name": "Estación Bogotá Centro",    "lat":  4.6097, "lon": -74.0817},
    {"id": "WS-COL-002", "name": "Estación Medellín Poblado", "lat":  6.2087, "lon": -75.5742},
    {"id": "WS-COL-003", "name": "Estación Cartagena Puerto", "lat": 10.3910, "lon": -75.4794},
    {"id": "WS-COL-004", "name": "Estación Cali Norte",       "lat":  3.4516, "lon": -76.5320},
    {"id": "WS-COL-005", "name": "Estación Leticia Amazónica","lat": -4.2153, "lon": -69.9406},
]

# Rangos de valores meteorológicos por zona climática
CLIMATE_PROFILES = {
    "WS-COL-001": {"temp": (7, 19),   "hum": (60, 90),  "pres": (1010, 1025)},   # Bogotá - Andina
    "WS-COL-002": {"temp": (15, 28),  "hum": (55, 85),  "pres": (1005, 1020)},   # Medellín - Andina
    "WS-COL-003": {"temp": (28, 36),  "hum": (65, 95),  "pres": (1008, 1015)},   # Cartagena - Costera
    "WS-COL-004": {"temp": (18, 30),  "hum": (60, 85),  "pres": (1005, 1018)},   # Cali - Valle
    "WS-COL-005": {"temp": (24, 34),  "hum": (80, 98),  "pres": (1008, 1016)},   # Leticia - Amazónica
}


@dataclass
class WeatherReading:
    """Representa una lectura de datos meteorológicos de una estación."""
    message_id:    str
    station_id:    str
    station_name:  str
    latitude:      float
    longitude:     float
    timestamp:     str
    temperature:   float          # Celsius
    humidity:      float          # %
    pressure:      float          # hPa
    wind_speed:    float          # km/h
    wind_direction: int           # grados
    precipitation: float          # mm
    uv_index:      float
    visibility:    float          # km
    schema_version: str = "1.0"
    producer_id:   str = field(default_factory=lambda: os.getenv("HOSTNAME", "producer-1"))


def generate_reading(station: dict, inject_anomaly: bool = False) -> WeatherReading:
    """
    Genera una lectura meteorológica simulada para una estación.

    Args:
        station: Diccionario con datos de la estación.
        inject_anomaly: Si True, inyecta valores fuera de rango para probar validaciones.

    Returns:
        WeatherReading con valores generados aleatoriamente.
    """
    sid = station["id"]
    profile = CLIMATE_PROFILES.get(sid, {"temp": (-10, 45), "hum": (10, 100), "pres": (900, 1050)})

    temp_min, temp_max = profile["temp"]
    hum_min,  hum_max  = profile["hum"]
    pres_min, pres_max = profile["pres"]

    # Pequeñas fluctuaciones gaussianas para simular datos reales
    temperature   = round(random.gauss((temp_min + temp_max) / 2, (temp_max - temp_min) / 6), 2)
    humidity      = round(random.uniform(hum_min, hum_max), 2)
    pressure      = round(random.gauss((pres_min + pres_max) / 2, 3), 2)
    wind_speed    = round(abs(random.gauss(15, 10)), 2)
    wind_direction = random.randint(0, 359)
    precipitation = round(random.choices([0, random.uniform(0.1, 15)], weights=[0.7, 0.3])[0], 2)
    uv_index      = round(random.uniform(0, 11), 1)
    visibility    = round(random.uniform(5, 30), 2)

    if inject_anomaly:
        # Inyectar anomalía aleatoria para pruebas de validación
        anomaly_field = random.choice(["temperature", "humidity", "wind_speed"])
        logger.warning(f"[ANOMALY] Inyectando anomalía en campo: {anomaly_field}")
        if anomaly_field == "temperature":
            temperature = random.choice([-95.0, 65.0])
        elif anomaly_field == "humidity":
            humidity = random.choice([-5.0, 105.0])
        elif anomaly_field == "wind_speed":
            wind_speed = 450.0

    return WeatherReading(
        message_id     = str(uuid.uuid4()),
        station_id     = station["id"],
        station_name   = station["name"],
        latitude       = station["lat"],
        longitude      = station["lon"],
        timestamp      = datetime.now(timezone.utc).isoformat(),
        temperature    = temperature,
        humidity       = humidity,
        pressure       = pressure,
        wind_speed     = wind_speed,
        wind_direction = wind_direction,
        precipitation  = precipitation,
        uv_index       = uv_index,
        visibility     = visibility,
    )


# =============================================================================
# Clase principal del Productor
# =============================================================================
class WeatherProducer:
    """
    Servicio productor que publica lecturas meteorológicas en RabbitMQ.

    Características:
    - Mensajes persistentes (delivery_mode=2)
    - Reconexión automática con backoff exponencial
    - Exchange topic para enrutamiento flexible
    - Confirmación de publicación (publisher confirms)
    """

    def __init__(self):
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.running = True
        self.messages_published = 0
        self.messages_failed = 0
        self._setup_signals()

    def _setup_signals(self):
        """Configura manejadores de señales para apagado limpio."""
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info(f"Señal {signum} recibida. Iniciando apagado limpio...")
        self.running = False

    def _get_connection_params(self) -> pika.ConnectionParameters:
        """Retorna parámetros de conexión a RabbitMQ."""
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        return pika.ConnectionParameters(
            host         = RABBITMQ_HOST,
            port         = RABBITMQ_PORT,
            virtual_host = RABBITMQ_VHOST,
            credentials  = credentials,
            heartbeat    = 60,
            blocked_connection_timeout = 300,
            connection_attempts = 3,
            retry_delay  = 2,
        )

    def connect(self) -> bool:
        """
        Establece conexión con RabbitMQ y configura el exchange.

        Returns:
            True si la conexión fue exitosa, False en caso contrario.
        """
        try:
            logger.info(f"Conectando a RabbitMQ en {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
            self.connection = pika.BlockingConnection(self._get_connection_params())
            self.channel = self.connection.channel()

            # Habilitar confirmaciones de publicación
            self.channel.confirm_delivery()

            # Declarar exchange durable de tipo topic
            self.channel.exchange_declare(
                exchange      = EXCHANGE_NAME,
                exchange_type = "topic",
                durable       = True,
            )

            logger.info(f"✓ Conectado a RabbitMQ. Exchange: '{EXCHANGE_NAME}'")
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

    def is_connected(self) -> bool:
        """Verifica si la conexión está activa."""
        return (
            self.connection is not None
            and self.connection.is_open
            and self.channel is not None
            and self.channel.is_open
        )

    def publish(self, reading: WeatherReading) -> bool:
        """
        Publica una lectura meteorológica en RabbitMQ.

        Args:
            reading: Objeto WeatherReading a publicar.

        Returns:
            True si la publicación fue confirmada, False en caso contrario.
        """
        if not self.is_connected():
            logger.warning("Conexión perdida. Reconectando...")
            self.connect_with_retry()

        payload = json.dumps(asdict(reading), ensure_ascii=False)

        properties = pika.BasicProperties(
            delivery_mode   = pika.spec.PERSISTENT_DELIVERY_MODE,  # Mensaje persistente
            content_type    = "application/json",
            content_encoding = "utf-8",
            message_id      = reading.message_id,
            timestamp       = int(time.time()),
            headers         = {
                "station_id":    reading.station_id,
                "schema_version": reading.schema_version,
            },
        )

        try:
            self.channel.basic_publish(
                exchange    = EXCHANGE_NAME,
                routing_key = ROUTING_KEY,
                body        = payload.encode("utf-8"),
                properties  = properties,
                mandatory   = False,
            )
            self.messages_published += 1
            logger.info(
                f"[→ PUBLISHED] station={reading.station_id} "
                f"temp={reading.temperature}°C hum={reading.humidity}% "
                f"msg_id={reading.message_id[:8]}... "
                f"[total={self.messages_published}]"
            )
            return True

        except pika.exceptions.UnroutableError:
            logger.error(f"Mensaje sin ruta: {reading.message_id}")
            self.messages_failed += 1
            return False
        except pika.exceptions.AMQPChannelError as e:
            logger.error(f"Error de canal AMQP: {e}")
            self.messages_failed += 1
            self.channel = None
            return False
        except Exception as e:
            logger.error(f"Error publicando mensaje: {e}")
            self.messages_failed += 1
            return False

    def run(self):
        """Loop principal del productor."""
        logger.info("=" * 60)
        logger.info("  Weather Station Producer - Iniciando")
        logger.info(f"  Intervalo de publicación: {PUBLISH_INTERVAL}s")
        logger.info(f"  Estaciones: {len(STATIONS)}")
        logger.info("=" * 60)

        self.connect_with_retry()

        cycle = 0
        anomaly_interval = 20  # Inyectar anomalía cada N ciclos

        while self.running:
            cycle += 1
            inject_anomaly = (cycle % anomaly_interval == 0)

            # Publicar para cada estación (o un subconjunto)
            for station in random.sample(STATIONS, min(STATIONS_PER_BATCH, len(STATIONS))):
                if not self.running:
                    break

                reading = generate_reading(station, inject_anomaly=inject_anomaly)
                self.publish(reading)

            # Log de métricas cada 30 ciclos
            if cycle % 30 == 0:
                logger.info(
                    f"[METRICS] Ciclo={cycle} Publicados={self.messages_published} "
                    f"Fallidos={self.messages_failed}"
                )

            time.sleep(PUBLISH_INTERVAL)

        self._cleanup()

    def _cleanup(self):
        """Cierra conexiones limpiamente."""
        logger.info("Cerrando conexiones...")
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error al cerrar conexiones: {e}")
        finally:
            logger.info(
                f"Producer detenido. Total publicados: {self.messages_published}, "
                f"fallidos: {self.messages_failed}"
            )


# =============================================================================
# Punto de entrada
# =============================================================================
if __name__ == "__main__":
    producer = WeatherProducer()
    producer.run()
