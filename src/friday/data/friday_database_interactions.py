
import pandas as pd

from ncaa_basketball_db import NcaaBballDb


class FridayDatabaseInteractions:

    def __init__(self, database_connector=None):
        self.database_connector = database_connector
        self.table_names = None

    def set_table_names(self, table_names):
        """
        method to set the available database table names as class attribute after some checks
        """

        if isinstance(table_names, pd.core.series.Series):
            self.table_names = table_names
        else:
            raise RuntimeWarning('set_table_names expects a pandas series for table_names')

    def find_tables(self, table_types="'public'"):
        """
        method to store and print all tables in the database
        :param table_types: str, type of table to find 'public', 'private', etc
        """
        # do the SQL query
        sql_query = '''
                    SELECT table_schema, table_name
                        FROM %s.information_schema.tables
                      WHERE table_schema LIKE %s
                      ORDER BY table_schema,table_name;
                    ''' % (getattr(self.database_connector, 'database_name'), table_types)
        try:
            tables_sql = pd.read_sql_query(sql_query, getattr(self.database_connector, 'con'))
            if tables_sql is not None:
                self.set_table_names(tables_sql['table_name'])
        except RuntimeError:
            raise RuntimeError('find_tables encountered RuntimeError')

    def run_interaction(self):
        """
        method to run all known interactions against the database
        :return:
        """
        self.find_tables()
        print(self.table_names)


if __name__ == "__main__":
    connector = NcaaBballDb(peek_tables=True, make_scoreboard=True)

    connector.make_database_engine()
    connector.check_database_engine()
    connector.connect_database()

    instance = FridayDatabaseInteractions(database_connector=connector)
    instance.run_interaction()

