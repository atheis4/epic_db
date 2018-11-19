from glob import glob
import os
import pandas as pd

from epic_db.database import config
from epic_db.models import *
from epic_db.requests import RequestHandler


eng = ('mysql://{user}:{password}@modeling-mortality-db.ihme.washington.edu:'
       '3306/epic?charset=utf8&use_unicode=1'.format(
           user=os.environ.get('EPIC_DB_USER'),
           password=os.environ.get('EPIC_DB_PASS')))

config.create_engine(eng)
session = config.Session()

# Operations on SequelaSetVersion 16
v16 = session.query(SequelaSetVersion).get(16)

# Create request here
request = {}

# Send request through the handler
handler = RequestHandler(session)
handler.process_request(request)

# commit changes
session.commit()
