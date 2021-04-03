"""
Dates settings
"""
from flask_babel import gettext
import datetime as dt
from collections import OrderedDict

LOCKDOWN_DAY = dt.datetime(2020, 3, 22)
PHASE2_DAY = dt.datetime(2020, 5, 4)
PHASE3_DAY = dt.datetime(2020, 6, 15)
CRITICAL_AREAS_DAY = dt.datetime(2020, 11, 6)
VACCINE_DAY = dt.datetime(2020, 12, 27)
KEY_PERIODS = OrderedDict()

# Key Periods
KEY_PERIODS["lockdown"] = {
    "title": gettext("Lockdown"),
    "text": gettext('Days in Lockdown'),
    "color": "red",
    "from": LOCKDOWN_DAY,
    "to": PHASE2_DAY,
    "n_days": (PHASE2_DAY - LOCKDOWN_DAY).days
}
KEY_PERIODS["phase2"] = {
    "title": gettext("Phase 2"),
    "text": gettext('Days in "Phase 2"'),
    "color": "orange",
    "from": PHASE2_DAY,
    "to": PHASE3_DAY,
    "n_days": (PHASE3_DAY - PHASE2_DAY).days,
}
KEY_PERIODS["phase3"] = {
    "title": gettext("Phase 3"),
    "text": gettext('Days in "Phase 3"'),
    "color": "green",
    "from": PHASE3_DAY,
    "to": CRITICAL_AREAS_DAY,
    "n_days": (CRITICAL_AREAS_DAY - PHASE3_DAY).days,
}
KEY_PERIODS["critical_areas"] = {
    "title": gettext("Critical Areas"),
    "text": gettext('Days since "Critical areas"'),
    "color": "red",
    "from": CRITICAL_AREAS_DAY,
    "to": dt.datetime.today(),
    "n_days": (dt.datetime.today() - CRITICAL_AREAS_DAY).days
}
KEY_PERIODS["vaccine_day"] = {
    "title": gettext("Vaccine day"),
    "text": gettext('Days since "Vaccine day"'),
    "color": "blue",
    "from": VACCINE_DAY,
    "to": dt.datetime.today(),
    "n_days": (dt.datetime.today() - VACCINE_DAY).days
}
