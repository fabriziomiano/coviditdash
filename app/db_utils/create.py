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
    URL_NATIONAL, URL_REGIONAL, URL_PROVINCIAL, URL_VAX_ADMINS_DATA,
    URL_VAX_SUMMARY_DATA, URL_VAX_ADMINS_SUMMARY_DATA
)
from settings.vars import VAX_DATE_KEY

national_trends_view_script = './MySQL/national_trends_view.sql'
regional_trends_view_script = './MySQL/regional_trends_view.sql'
provincial_trends_view_script = './MySQL/provincial_trends_view.sql'
regional_breakdown_view_script = './MySQL/regional_breakdown_view.sql'


class DBObjectsCreator:
    """DB Object creator class"""

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def create_regional_trends_view():
        """
        Create Regional Trends view
        :return:
        """
        try:
            with open(regional_trends_view_script, 'r') as script_in:
                stmt = script_in.read()
            view = Table('v_REGIONAL_TRENDS', MetaData())
            definition = text(stmt)
            create_view = CreateView(view, definition, or_replace=True)
            app.logger.info("Creating Regional Trends view")
            with db.engine.connect() as con:
                con.execute(create_view)
        except Exception as e:
            app.logger.error(f'While creating regional-trends view: {e}')    \


    @staticmethod
    def create_regional_breakdown_view():
        """
        Create Regional breakdown view
        :return:
        """
        try:
            with open(regional_breakdown_view_script, 'r') as script_in:
                stmt = script_in.read()
            view = Table('v_REGIONAL_BREAKDOWN', MetaData())
            definition = text(stmt)
            create_view = CreateView(view, definition, or_replace=True)
            app.logger.info("Creating Regional Breakdown view")
            with db.engine.connect() as con:
                con.execute(create_view)
        except Exception as e:
            app.logger.error(f'While creating regional-breakdown view: {e}')

    @staticmethod
    def create_provincial_table():
        """
        Create Provincial table
        :return:
        """
        df = load_cp_df(URL_PROVINCIAL)
        df = preprocess_provincial(df)
        try:
            app.logger.info("Creating Provincial table")
            df.to_sql(
                'PROVINCIAL',
                db.engine,
                index=False,
                if_exists='replace')
        except Exception as e:
            app.logger.error(f"While creating Provincial table: {e}")

    @staticmethod
    def create_provincial_trends_view():
        """
        Create Provincial Trends view
        :return:
        """
        try:
            with open(provincial_trends_view_script, 'r') as script_in:
                stmt = script_in.read()
            view = Table('v_PROVINCIAL_TRENDS', MetaData())
            definition = text(stmt)
            create_view = CreateView(view, definition, or_replace=True)
            app.logger.info("Creating Provincial Trends view")
            with db.engine.connect() as con:
                con.execute(create_view)
        except Exception as e:
            app.logger.error(f'While creating provincial-trends view: {e}')

    @staticmethod
    def create_vax_admins_table():
        """
        Create Vax admins table
        :return:
        """
        df = pd.read_csv(URL_VAX_ADMINS_DATA, parse_dates=[VAX_DATE_KEY])
        df = preprocess_admins(df)
        try:
            app.logger.info("Creating Admins table")
            df.to_sql(
                'VAX_ADMINS',
                db.engine,
                index=False,
                if_exists='replace')
        except Exception as e:
            app.logger.error(f"While creating Vax admins table: {e}")

    @staticmethod
    def create_vax_summary_table():
        """
        Create Vax summary table
        :return:
        """
        df = pd.read_csv(URL_VAX_SUMMARY_DATA)
        try:
            app.logger.info("Creating Summary table")
            df.to_sql(
                'VAX_SUMMARY',
                db.engine,
                index=False,
                if_exists='replace')
        except Exception as e:
            app.logger.error(f"While creating Vax summaryu table: {e}")

    @staticmethod
    def create_vax_admins_summary_table():
        """
        Create Vax admins summary table
        :return:
        """
        df = pd.read_csv(
            URL_VAX_ADMINS_SUMMARY_DATA, parse_dates=[VAX_DATE_KEY])
        df = preprocess_admins_summary(df)
        try:
            app.logger.info("Creating Admins Summary table")
            df.to_sql(
                'VAX_ADMINS_SUMMARY',
                db.engine,
                index=False,
                if_exists='replace')
        except Exception as e:
            app.logger.error(f"While creating Vax admins summary table: {e}")
