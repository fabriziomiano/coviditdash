from app import mongo
from config import (
    NATIONAL_DATA_COLLECTION, NATIONAL_TRENDS_COLLECTION,
    NATIONAL_SERIES_COLLECTION, REGIONAL_DATA_COLLECTION,
    REGIONAL_TRENDS_COLLECTION, REGIONAL_SERIES_COLLECTION,
    REGIONAL_BREAKDOWN_COLLECTION, PROVINCIAL_DATA_COLLECTION,
    PROVINCIAL_TRENDS_COLLECTION, PROVINCIAL_SERIES_COLLECTION,
    PROVINCIAL_BREAKDOWN_COLLECTION, BAR_CHART_COLLECTION
)

NATIONAL_DATA = mongo.db[NATIONAL_DATA_COLLECTION]
NATIONAL_TRENDS = mongo.db[NATIONAL_TRENDS_COLLECTION]
NATIONAL_SERIES = mongo.db[NATIONAL_SERIES_COLLECTION]
REGIONAL_DATA = mongo.db[REGIONAL_DATA_COLLECTION]
REGIONAL_TRENDS = mongo.db[REGIONAL_TRENDS_COLLECTION]
REGIONAL_SERIES = mongo.db[REGIONAL_SERIES_COLLECTION]
REGIONAL_BREAKDOWN = mongo.db[REGIONAL_BREAKDOWN_COLLECTION]
PROVINCIAL_DATA = mongo.db[PROVINCIAL_DATA_COLLECTION]
PROVINCIAL_TRENDS = mongo.db[PROVINCIAL_TRENDS_COLLECTION]
PROVINCIAL_SERIES = mongo.db[PROVINCIAL_SERIES_COLLECTION]
PROVINCIAL_BREAKDOWN = mongo.db[PROVINCIAL_BREAKDOWN_COLLECTION]
BARCHART_COLLECTION = mongo.db[BAR_CHART_COLLECTION]
