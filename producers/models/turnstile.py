"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro
from dataclasses import asdict, dataclass

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)

@dataclass
class TurnstileEvent():
    station_id: int
    station_name : str
    line : str

class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )


        topic_name = f"org.chicago.cta.turnstile.v1"

        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=5,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        for i in range(0, num_entries) :
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": int(timestamp.timestamp() * 1000)},
                key_schema=Turnstile.key_schema,
                value=asdict(TurnstileEvent(station_id=self.station.station_id,
                                            line=self.station.color.name,
                                            station_name=self.station.name)),
                value_schema=Turnstile.value_schema
            )
