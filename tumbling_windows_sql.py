import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings,TableConfig



def main():
    # Create streaming environment

    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode().build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.11-1.13.0.jar')

    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))

    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE future (
            
            TradingDay VARCHAR,
            InstrumentID VARCHAR,
            ExchangeID VARCHAR,
            LastPrice DOUBLE,
            Volume INT,
            Turnover DOUBLE,
            UpperLimitPrice DOUBLE,
            LowerLimitPrice DOUBLE,
            UpdateTime VARCHAR,
            UpdateMillisec INT,
            ActionDay VARCHAR,
            UpdateTimeStamp BIGINT,
            EventTime AS TO_TIMESTAMP_LTZ(UpdateTimeStamp - 8 * 60 * 60 * 1000, 3),
            WATERMARK FOR EventTime AS EventTime - INTERVAL '60' SECOND,
            ProcessTime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'czce',
            'properties.bootstrap.servers' = '127.0.0.1:9092',
            'properties.group.id' = 'czce',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('future')

    print('\nSource Schema')
    tbl.print_schema()

    #####################################################################
    # Define Tumbling Window Aggregate Calculation of Price per InstrumentID
    #
    # - for every 60 second non-overlapping window
    #
    #####################################################################
    sql = """
        SELECT
            InstrumentID,ExchangeID,
            TUMBLE_START(EventTime, INTERVAL '60' SECONDS) AS window_start,
            TUMBLE_END(EventTime, INTERVAL '60' SECONDS) AS window_end,
            first_value(LastPrice) as opening_price,
            max(LastPrice) as highest_price,
            min(LastPrice) as lowest_price,
            last_value(LastPrice) as closing_price
        FROM future
        GROUP BY
        InstrumentID,ExchangeID,
        TUMBLE(EventTime, INTERVAL '60' SECONDS)
    """
    windowed_rev = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    windowed_rev.print_schema()

    ###############################################################
    # Create JSON Sink Table
    ###############################################################
    sink_ddl = """
        CREATE TABLE future_result (
            InstrumentID VARCHAR,
            ExchangeID VARCHAR,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            opening_price DOUBLE,
            highest_price DOUBLE,
            lowest_price DOUBLE,
            closing_price DOUBLE
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///Users/icourt/PycharmProjects/zhuoshi/windowed-czce.json',
            'format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    # 'sink.rolling-policy.rollover-interval' = '5s'

    # write time windowed aggregations to sink table
    windowed_rev.execute_insert('future_result').wait()

    tbl_env.execute('sql-tumbling-windows')


if __name__ == '__main__':
    main()
