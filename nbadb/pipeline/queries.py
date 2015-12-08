"""
Holds some generic method to do basic retrieve/insert/create queries.
"""


class Queries(object):
    @staticmethod
    def retrieve_query(conn, query, params):
        """
        Takes a given query and a tuple of parameters and then executes it and returns the cursor.fetchall() object,
        if applicable.
        :param psycopg2.connection conn:
        :param str query:
        :param tuple params:
        :return cur.fetchall() data:
        """
        with conn.cursor() as cur:
            cur.execute(query, params)
            data = cur.fetchall()
        return data

    @staticmethod
    def insert_query(conn, query, params):
        """
        Takes a given query and a tuple of parameters and then executes it and commits it to the database.
        :param psycopg2.connection conn:
        :param str query:
        :param tuple params:
        """
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()

    @staticmethod
    def build_query(conn, query):
        """
        Takes a given query and a tuple of parameters and then executes it and commits it to the database. For
        building the table setup.
        :param psycopg2.connection conn:
        :param str query:
        """
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
