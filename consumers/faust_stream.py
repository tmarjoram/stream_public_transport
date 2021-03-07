"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)

HOSTNAME = 'DESKTOP-MRAIJ7J'

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker=f"kafka://{HOSTNAME}:9092", store="memory://")

#TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("com.udacity.connect-stations", value_type=Station)
#TODO: Define the output Kafka Topic
out_topic = app.topic("com.udacity.stations.transformed", partitions=1, value_type=TransformedStation)
#TODO: Define a Faust Table
table = app.Table(
   "station_aggregation",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
   help="Aggregated transformed station events"
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def station_event(stationevents):
    async for stationevent in stationevents:

        color = "red" if stationevent.red else "blue" if stationevent.blue else "green" if stationevent.green else None
        transformed_station = TransformedStation(station_id=stationevent.station_id,
                                                 station_name=stationevent.station_name,
                                                 order=stationevent.order,
                                                 line=color)

        table[stationevent.station_id] = transformed_station



if __name__ == "__main__":
    app.main()
