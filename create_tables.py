"""
Create Tables script.
Module which creates the project's tables. The use of this module assumes an
available connection to the Redshift cluster through correct conifguration
parameters for the databse connection, through the dwh.cfg file.
"""
import configparser
import sql_queries as sql


def drop_tables(cursor):
    """
    Drop project tables.
    Function which drops projects tables.
    Args:
        - cursor: cursor object to connected sparkifydb.
    """
    for table in sql.TABLES:
        sql.drop(table, cursor)


def create_tables(cursor):
    """
    Create project tables.
    Function which creates projects tables.
    Args:
        - cursor: cursor object to connected sparkifydb.
    """
    for table in sql.CREATE_QUERIES:
        sql.execute(table, cursor)


def main():
    """
    Main function.
    Main function to execute when module is called through command line.
    Creates a connection to the cluster, drops the tables (if exist)
    creates them again.
    """
    # Get configuration parameters
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Get connections
    connection_string = "host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values())
    cursor, connection = sql.connect(connection_string, autocommit=True)
    if not cursor:  # End execution if error
        return
    # Drop tables if exist
    drop_tables(cursor)
    # Create dimension tables
    create_tables(cursor)
    # Disconnect
    sql.disconnect(cursor, connection)


if __name__ == "__main__":
    main()
