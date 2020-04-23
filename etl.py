import configparser
import sql_queries as sql


def load_staging_tables(cursor, connection):
    """
    Load Staging Tables.
    Function which executes the queries to load data into staging tables.
    Args:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
    """
    for query in sql.COPY_QUERIES:
        sql.execute(query, cursor)
        connection.commit()


def insert_tables(cursor, connection):
    """
    Insert Tables.
    Function which executes the queries to insert data into final tables.
    Args:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
    """
    for query in sql.INSERT_QUERIES:
        sql.execute(query, cursor)
        connection.commit()


def main():
    """
    Main function.
    Main function to execute when module is called through command line.
    Creates a connection to the cluster, loads staging tables and inserts
    staging tables data into final tables.
    """
    # Get configuration parameters
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Get connections
    connection_string = "host={} dbname={} user={} password={} port={}".format(
        *config["CLUSTER"].values())
    cursor, connection = sql.connect(connection_string)
    if not cursor:  # End execution if error
        return
    # Load staging tables
    load_staging_tables(cursor, connection)
    # Insert data into final tables
    insert_tables(cursor, connection)
    # Disconnect
    sql.disconnect(cursor, connection)


if __name__ == "__main__":
    main()
