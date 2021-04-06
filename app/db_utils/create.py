"""
DB Recovery
"""
import pandas as pd
from flask import current_app as app
from sqlalchemy import Table, MetaData
from sqlalchemy.sql import text
from sqlalchemy_views import CreateView
from app import db
from app.db_utils.etl import load_cp_df
from app.db_utils.etl import (
    preprocess_national, preprocess_regional, preprocess_provincial,
    preprocess_admins, preprocess_admins_summary
)
from settings.urls import (
    URL_NATIONAL, URL_REGIONAL, URL_PROVINCIAL, URL_ADMINS, URL_SUMMARY,
    URL_ADMINS_SUMMARY
)
from settings.vars import VAX_DATE_KEY
national_trends_view_script = './MySQL/create_national_trends_view.sql'


def create_national_table():
    """
    Create National data table
    :return:
    """
    df = load_cp_df(URL_NATIONAL)
    df = preprocess_national(df)
    try:
        app.logger.info("Creating National table")
        df.to_sql('NATIONAL', db.engine, index=False, if_exists='replace')
    except Exception as e:
        app.logger.error(f"While creating National table: {e}")


def create_national_trends_view():
    """
    Create National Trends view
    :return:
    """
    try:
        with open(national_trends_view_script, 'r') as script_in:
            stmt = script_in.read()
        view = Table('v_NATIONAL_TRENDS', MetaData())
        definition = text(stmt)
        create_view = CreateView(view, definition, or_replace=True)
        app.logger.info("Creating National Trends view")
        with db.engine.connect() as con:
            con.execute(create_view)
    except Exception as e:
        app.logger.error(f'While creating national-trends view: {e}')


def create_regional_table():
    """
    Create Regional table
    :return:
    """
    df = load_cp_df(URL_REGIONAL)
    df = preprocess_regional(df)
    try:
        app.logger.info("Creating Regional table")
        df.to_sql('REGIONAL', db.engine, index=False, if_exists='replace')
    except Exception as e:
        app.logger.error(f"While creating Regional table: {e}")


def create_provincial_collections():
    """
    Create Provincial table
    :return:
    """
    df = load_cp_df(URL_PROVINCIAL)
    df = preprocess_provincial(df)
    try:
        app.logger.info("Creating Provincial table")
        df.to_sql('PROVINCIAL', db.engine, index=False, if_exists='replace')
    except Exception as e:
        app.logger.error(f"While creating Provincial table: {e}")


def create_admins_table():
    """
    Create Admins table
    :return:
    """
    df = pd.read_csv(URL_ADMINS, parse_dates=[VAX_DATE_KEY])
    df = preprocess_admins(df)
    try:
        app.logger.info("Creating Admins table")
        df.to_sql('ADMINS', db.engine, index=False, if_exists='replace')
    except Exception as e:
        app.logger.error(f"While creating Admins table: {e}")


def create_summary_table():
    """
    Create Summary table
    :return:
    """
    df = pd.read_csv(URL_SUMMARY)
    try:
        app.logger.info("Creating Summary table")
        df.to_sql('SUMMARY', db.engine, index=False, if_exists='replace')
    except Exception as e:
        app.logger.error(f"While creating Summaryu table: {e}")


def create_admins_summary_table():
    """
    Create Admins Summary table
    :return:
    """
    df = pd.read_csv(URL_ADMINS_SUMMARY, parse_dates=[VAX_DATE_KEY])
    df = preprocess_admins_summary(df)
    try:
        app.logger.info("Creating Admins Summary table")
        df.to_sql(
            'ADMINS_SUMMARY',
            db.engine,
            index=False,
            if_exists='replace'
        )
    except Exception as e:
        app.logger.error(f"While creating Admins summary table: {e}")
