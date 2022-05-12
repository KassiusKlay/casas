import pandas as pd
import streamlit as st
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import unidecode


def remove_accent(string):
    return unidecode.unidecode(string)


def remove_accent_from_series(series):
    return series.apply(remove_accent)


@st.experimental_singleton
def init_engine():
    return create_engine(
        f'postgresql://'
        f'{st.secrets["postgres"]["user"]}:'
        f'{st.secrets["postgres"]["password"]}@'
        f'{st.secrets["postgres"]["host"]}:'
        f'{st.secrets["postgres"]["port"]}/'
        f'{st.secrets["postgres"]["dbname"]}',
        )


@st.experimental_singleton
def init_connection():
    connection = psycopg2.connect(**st.secrets["postgres"])
    connection.autocommit = True
    return connection


def copy_from_stringio(connection, df, table):
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    cursor = connection.cursor()
    cursor.copy_expert(
            """COPY %s FROM STDIN WITH (FORMAT CSV)""" % table, buffer)
    cursor.close()


def run_query(connection, query, fetch=None):
    with connection.cursor() as cur:
        cur.execute(query)
        connection.commit()
        if fetch:
            return cur.fetchall()


def read_sql_inmem_uncompressed(query, connection):
    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
       query=query, head="HEADER"
    )
    cur = connection.cursor()
    store = StringIO()
    cur.copy_expert(copy_sql, store)
    store.seek(0)
    df = pd.read_csv(store)
    return df


def find_difference_between_two_dataframes(new_df, old_df):
    return pd.concat([pd.concat([new_df, old_df]), old_df]).drop_duplicates(
            keep=False)
