"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


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


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
# places it into a new topic with only the necessary information.
app = faust.App("stations-stream",
                broker="kafka://localhost:9092", store="memory://")
# Define the input Kafka Topic
topic = app.topic("jdbc-source-psql.stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic("com.udacity.faust.stations.transformed", partitions=1)
# Define a Faust Table
table_name = "com.udacity.faust.stations.transformed.table"
table = app.Table(
    table_name,
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

# "line" is the color of the station. So if the Station record has the field red set to true,
# then you would set the line of the TransformedStation record to the string "red"
@app.agent(topic)
async def process_stream(input_stream):
    async for droplet in input_stream:
        line = ''  # empty string to maintain str type

        # assign colors as line names
        if droplet.red:
            line = 'red'
        elif droplet.green:
            line = 'green'
        elif droplet.blue:
            line = 'blue'
        else:
            logger.info(f"Wrong line color for {droplet.station_id}")
            logger.info(
                "Line name/color MUST be one of ['red','green','blue']")

        transformed_station = TransformedStation(
            station_id=droplet.station_id,
            station_name=droplet.station_name,
            order=droplet.order,
            line=line
        )
        # add transformed_station to the table as row, keyed on station_id
        table[droplet.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
