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
topic = app.topic("jdbc.source.psql.stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic("com.udacity.faust.stations.transformed.table", partitions=1)
# Define a Faust Table
table_name = "com.udacity.faust.stations.transformed.table"
table = app.Table(
    name=table_name,
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)

# "line" is the color of the station. So if the Station record has the field red set to true,
# then you would set the line of the TransformedStation record to the string "red"
@app.agent(topic)
async def process_stream(input_stream):
    async for droplet in input_stream:
        line = None  # empty string to maintain str type

        # assign colors as line names
        if droplet.red == True:
            line = 'red'
        elif droplet.green == True:
            line = 'green'
        elif droplet.blue == True:
            line = 'blue'
        
        if line is None:
            logger.info(f"Wrong line color for {droplet.station_id}")
            logger.info(
                "Line name/color MUST be one of ['red','green','blue']")
            continue

        table[droplet.station_id] = TransformedStation(
            station_id=droplet.station_id,
            station_name=droplet.station_name,
            order=droplet.order,
            line=line
        )


if __name__ == "__main__":
    app.main()
