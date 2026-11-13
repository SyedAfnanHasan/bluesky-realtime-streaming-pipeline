
"""Display Bluesky event and trending-topic metrics from PostgreSQL.

This Streamlit dashboard retrieves the latest event metrics and the most
recent trending-topic snapshot from the bluesky_streaming database.
"""

import pandas as pd
import psycopg
import streamlit as st


# ------------------------------------------
# PAGE CONFIGURATION
# ------------------------------------------

st.set_page_config(
    page_title="Bluesky Streaming Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("Bluesky Streaming Analytics")


# ------------------------------------------
# POSTGRESQL CONNECTION
# ------------------------------------------

try:

    with psycopg.connect(
        host="localhost",
        port=5432,
        dbname="bluesky_streaming",
        user="syedafnan"
    ) as connection:

        # --------------------------------------
        # EVENT METRICS
        # --------------------------------------

        event_query = """
            SELECT *
            FROM event_metrics
            ORDER BY recorded_at DESC
            LIMIT 10;
        """

        with psycopg.Connection.cursor(connection) as cursor:

            cursor.execute(event_query)

            event_rows = cursor.fetchall()

            event_columns = [
                description.name
                for description in cursor.description
            ]

        event_df = pd.DataFrame(
            event_rows,
            columns=event_columns
        )

        st.subheader("Latest Event Metrics")

        st.dataframe(event_df, width="stretch")

        # --------------------------------------
        # TREND METRICS
        # --------------------------------------

        trend_query = """
            SELECT
                rank,
                topic_name,
                post_count,
                category,
                status,
                recorded_at
            FROM trend_metrics
            WHERE recorded_at = (
                SELECT MAX(recorded_at)
                FROM trend_metrics
            )
            ORDER BY rank;
        """

        with psycopg.Connection.cursor(connection) as cursor:

            cursor.execute(trend_query)

            trend_rows = cursor.fetchall()

            trend_columns = [
                description.name
                for description in cursor.description
            ]

        trend_df = pd.DataFrame(
            trend_rows,
            columns=trend_columns
        )

    # --------------------------------------
    # DISPLAY RESULTS
    # --------------------------------------

    st.success("Successfully connected to PostgreSQL!")

    st.subheader("Trending Topics")

    st.dataframe(trend_df, width="stretch")


except Exception as error:

    st.error(f"Error: {error}")
