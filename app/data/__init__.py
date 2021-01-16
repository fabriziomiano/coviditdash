"""
Data Module
"""
import datetime as dt

import pandas as pd
import requests
from flask import current_app as app
from flask_babel import gettext

from app.db import (
    NAT_DATA_COLL, NAT_TRENDS_COLL, NAT_SERIES_COLL, REG_DATA_COLL,
    REG_TRENDS_COLL, REG_SERIES_COLL, REG_BREAKDOWN_COLL, PROV_DATA_COLL,
    PROV_TRENDS_COLL, PROV_SERIES_COLL, PROV_BREAKDOWN_COLL, VAX_COLL,
    VAX_SUMMARY_COLL
)
from app.utils import rubbish_notes, translate_series_lang
from config import (
    REGION_KEY, PROVINCE_KEY, DATE_KEY, NOTE_KEY, DAILY_POSITIVITY_INDEX,
    UPDATE_FMT, VARS, ITALY_MAP, VERSION, REGIONS, PROVINCES, TOTAL_CASES_KEY,
    NEW_POSITIVE_KEY, KEY_PERIODS, URL_VAX_LATEST_UPDATE,
    VAX_LATEST_UPDATE_KEY, VAX_DATE_FMT, VAX_UPDATE_FMT, VAX_AREA_KEY,
    VAX_AGE_KEY, HEALTHCARE_PERS, NONHEALTHCARE_PERS, HFE_GUESTS, OD_TO_PC_MAP
)

DATA_SERIES = [VARS[key]["title"] for key in VARS]
DASHBOARD_DATA = {
    "vars_config": VARS,
    "data_series": DATA_SERIES,
    "italy_map": ITALY_MAP,
    "VERSION": VERSION,
    "regions": REGIONS,
    "provinces": PROVINCES,
    "key_periods": KEY_PERIODS
}

CUM_QUANTITIES = [qty for qty in VARS if VARS[qty]["type"] == "cum"]
NON_CUM_QUANTITIES = [qty for qty in VARS if VARS[qty]["type"] == "current"]
DAILY_QUANTITIES = [qty for qty in VARS if VARS[qty]["type"] == "daily"]
TREND_CARDS = [
    qty for qty in VARS
    if not qty.endswith("_ma") and VARS[qty]["type"] != "vax"
]
PROV_TREND_CARDS = [TOTAL_CASES_KEY, NEW_POSITIVE_KEY]
VAX_PEOPLE_CATEGORIES = [HEALTHCARE_PERS, NONHEALTHCARE_PERS, HFE_GUESTS]


def get_query_menu(area=None):
    """
    Return the query menu
    :param area: str
    :return: dict
    """
    return {
        "national": {
            "query": {},
            "collection": NAT_DATA_COLL
        },
        "regional": {
            "query": {REGION_KEY: area},
            "collection": REG_DATA_COLL
        },
        "provincial": {
            "query": {PROVINCE_KEY: area},
            "collection": PROV_DATA_COLL
        }
    }


def get_notes(notes_type="national", area=None):
    """
    Return the notes in the data otherwise empty string when
    the received note is 0 or matches the RUBBISH_NOTE_REGEX
    :param notes_type: str
    :param area: str
    :return: str
    """
    query_menu = get_query_menu(area)
    query = query_menu[notes_type]["query"]
    collection = query_menu[notes_type]["collection"]
    notes = ""
    try:
        doc = next(collection.find(query).sort([(DATE_KEY, -1)]).limit(1))
        notes = doc[NOTE_KEY] if doc[NOTE_KEY] != 0 else None
    except StopIteration:
        app.logger.error("While getting notes: no data")
    return notes if notes is not None and not rubbish_notes(notes) else ""


def get_national_trends():
    """Return national trends from DB"""
    return sorted(
        list(NAT_TRENDS_COLL.find({})),
        key=lambda x: list(VARS.keys()).index(x['id'])
    )


def get_regional_trends(region):
    """
    Return a list of regional trends for a given region
    :param region: str
    :return: list
    """
    trends = []
    doc = REG_TRENDS_COLL.find_one({REGION_KEY: region})
    if doc:
        trends = doc["trends"]
    return trends


def get_provincial_trends(province):
    """
    Return a list of provincial trends for a given province
    :param province: str
    :return: list
    """
    doc = PROV_TRENDS_COLL.find_one({PROVINCE_KEY: province})
    return doc["trends"]


def get_regional_breakdown():
    """Return regional breakdown from DB"""
    doc = REG_BREAKDOWN_COLL.find_one({}, {"_id": False})
    if doc:
        breakdown = {
            key: sorted(doc[key], key=lambda x: x['count'], reverse=True)
            for key in doc
        }
    else:
        breakdown = {"err": "No data"}
    return breakdown


def get_provincial_breakdown(region):
    """Return provincial breakdown from DB"""
    b = {}
    doc = PROV_BREAKDOWN_COLL.find_one({REGION_KEY: region}, {"_id": False})
    if doc:
        b = doc["breakdowns"]
        for key in b.keys():
            b[key] = sorted(b[key], key=lambda x: x['count'], reverse=True)
    return b


def get_national_series():
    """Return national series from DB"""
    series = NAT_SERIES_COLL.find_one({}, {"_id": False})
    if series:
        data = translate_series_lang(series)
    else:
        data = {"err": "No data"}
    return data


def get_regional_series(region):
    """Return regional series from DB"""
    data = {}
    series = REG_SERIES_COLL.find_one({REGION_KEY: region}, {"_id": False})
    if series:
        data = translate_series_lang(series)
    return data


def get_provincial_series(province):
    """Return provincial series from DB"""
    series = PROV_SERIES_COLL.find_one(
        {PROVINCE_KEY: province}, {"_id": False})
    return translate_series_lang(series)


def get_positivity_idx(area_type="national", area=None):
    """
    Return the positivity index for either the national or the regional
    views
    :param area_type: str: "national" or "regional"
    :param area: str
    :return: str
    """
    query_menu = get_query_menu(area)
    query = query_menu[area_type]["query"]
    collection = query_menu[area_type]["collection"]
    try:
        doc = next(collection.find(query).sort([(DATE_KEY, -1)]).limit(1))
        idx = f"{round(doc[DAILY_POSITIVITY_INDEX])}%"
    except StopIteration:
        app.logger.error("While getting positivity idx: no data")
        idx = "n/a"
    return idx


def get_national_data():
    """Return a data frame of the national data from DB"""
    cursor = NAT_DATA_COLL.find({})
    df = pd.DataFrame(list(cursor))
    if df.empty:
        app.logger.error("While getting national data: no data")
    return df


def get_region_data(region):
    """Return a data frame for a given region from the regional collection"""
    cursor = REG_DATA_COLL.find({REGION_KEY: region})
    df = pd.DataFrame(list(cursor))
    if df.empty:
        app.logger.error(f"While getting {region} data: no data")
    return df


def get_province_data(province):
    """
    Return a data frame for a given province from the provincial collection
    """
    cursor = PROV_DATA_COLL.find({PROVINCE_KEY: province})
    df = pd.DataFrame(list(cursor))
    if df.empty:
        app.logger.error(f"While getting {province} data: no data")
    return df


def get_latest_update(data_type="national"):
    """
    Return the value of the key PCM_DATE_KEY of the last dict in data
    :return: str
    """
    query_menu = get_query_menu()
    collection = query_menu[data_type]["collection"]
    try:
        doc = next(collection.find({}).sort([(DATE_KEY, -1)]).limit(1))
        latest_update = doc[DATE_KEY].strftime(UPDATE_FMT)
    except StopIteration:
        app.logger.error("While getting latest update: no data")
        latest_update = "n/a"
    return latest_update


def get_latest_vax_update():
    """Return the lastest update dt"""
    try:
        response = requests.get(URL_VAX_LATEST_UPDATE).json()
        datestr = response[VAX_LATEST_UPDATE_KEY]
        date_dt = dt.datetime.strptime(datestr, VAX_DATE_FMT)
        latest_update = date_dt.strftime(VAX_UPDATE_FMT)
    except Exception as e:
        app.logger.error(f"Error while getting latest vax update dt: {e}")
        latest_update = "n/a"
    return latest_update


def get_perc_pop_vax(tot_admins, population):
    """Return the ratio tot administrations / population rounded to 2 figs"""
    return round(((int(tot_admins) / population) * 100), 2)


def enrich_frontend_data(area=None, **data):
    """
    Return a data dict to be rendered which is an augmented copy of
    DASHBOARD_DATA defined in config.py
    :param area: optional, str
    :param data: **kwargs
    :return: dict
    """
    try:
        data["area"] = area
    except KeyError:
        pass
    data.update(DASHBOARD_DATA)
    return data


def get_total_administrations(area=None):
    """Return the total administration performed"""
    tot_adms = "n/a"
    try:
        pipe = [
            {'$match': {VAX_AREA_KEY: area}},
            {'$group': {'_id': f'${VAX_AREA_KEY}', 'tot': {'$sum': '$totale'}}}
        ]
        cursor = VAX_SUMMARY_COLL.aggregate(pipeline=pipe)
        tot_adms = next(cursor)['tot']
    except Exception as e:
        app.logger.error(f"While getting total admins: {e}")
    return tot_adms


def get_age_chart_data(area=None):
    """Return age series data"""
    chart_data = {}
    match = {'$match': {VAX_AREA_KEY: area}}
    group = {'$group': {'_id': f'${VAX_AGE_KEY}', 'tot': {'$sum': '$totale'}}}
    sort = {'$sort': {'_id': 1}}
    try:
        if area is not None:
            pipe = [match, group, sort]
        else:
            pipe = [group, sort]
        cursor = VAX_COLL.aggregate(pipeline=pipe)
        data = list(cursor)
        df = pd.DataFrame(data)
        categories = df['_id'].values.tolist()
        admins_per_age = df['tot'].values.tolist()
        chart_data = {
            "categories": categories,
            "admins_per_age": [{
                'name': gettext("Vaccinated"),
                'data': admins_per_age
            }]
        }
    except Exception as e:
        app.logger.error(f"While getting age chart data: {e}")
    return chart_data


def get_category_chart_data(area=None):
    """Return category series data"""
    chart_data = []
    try:
        pipe = [
            {
                '$match': {
                    VAX_AREA_KEY: area
                }
            },
            {
                '$group': {
                    '_id': f'${VAX_AREA_KEY}',
                    HEALTHCARE_PERS: {'$sum': f'${HEALTHCARE_PERS}'},
                    NONHEALTHCARE_PERS: {'$sum': f'${NONHEALTHCARE_PERS}'},
                    HFE_GUESTS: {'$sum': f'${HFE_GUESTS}'},
                }
            }
        ]
        cursor = VAX_SUMMARY_COLL.aggregate(pipeline=pipe)
        doc = next(cursor)
        app.logger.debug(doc)
        chart_data = [
            {'name': gettext(VARS[cat]["title"]), 'y': doc[cat]}
            for cat in VAX_PEOPLE_CATEGORIES
        ]
    except Exception as e:
        app.logger.error(f"While getting category-chart data: {e}")
    app.logger.debug(chart_data)
    return chart_data


def get_region_chart_data():
    """Return administrations data per region"""
    chart_data = {}
    try:
        pipe = [
            {
                '$match': {
                    VAX_AREA_KEY: {
                        '$not': {
                            '$eq': 'ITA'
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': f'${VAX_AREA_KEY}',
                    'tot': {'$sum': '$totale'}
                }
            },
            {'$sort': {'tot': -1}}
        ]
        cursor = VAX_SUMMARY_COLL.aggregate(pipeline=pipe)
        data = list(cursor)
        df = pd.DataFrame(data)
        app.logger.debug(data)
        chart_data = {
            "categories": df['_id'].apply(
                lambda x: OD_TO_PC_MAP[x]).values.tolist(),
            "admins_per_region": [
                {
                    'name': gettext("Vaccinati"),
                    'data': df['tot'].values.tolist()
                }
            ]
        }
    except Exception as e:
        app.logger.error(f"While getting region chart data: {e}")
    return chart_data
