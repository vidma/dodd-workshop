from kensu.utils.rule_engine import *
from kensu.utils.kensu_provider import KensuProvider

import urllib3
urllib3.disable_warnings()


import kensu.numpy as np
import kensu.pandas as pd

def run(months):
    for month in months:
        pipeline(month)


def pipeline(month):
    print("Starting Month %s" % month)
    k = KensuProvider().initKensu()
    customers_info = pd.read_csv('../data/%s/customers-data.csv' % month)
    contact_info = pd.read_csv('../data/%s/contact-data.csv' % month)

    business_info = pd.read_csv('../data/%s/business-data.csv' % month)
    customer360 = customers_info.merge(contact_info,on='id')
    month_data = pd.merge(customer360,business_info)
    month_data = data_prep(month_data)
    check_nrows_consistency()
    month_data.to_csv('../data/data.csv',index=False)

def data_prep(data):
    data['education']=np.where(data['education'] =='basic.9y', 'Basic', data['education'])
    data['education']=np.where(data['education'] =='basic.6y', 'Basic', data['education'])
    data['education']=np.where(data['education'] =='basic.4y', 'Basic', data['education'])

    cat = [i for i in ['job','marital','education','default','housing','loan','contact','month','day_of_week','poutcome'] if i in data.columns]

    data_dummy = pd.get_dummies(data,columns=cat)

    features=[i for i in ['euribor3m', 'job_blue-collar', 'job_housemaid', 'marital_unknown',
      'month_apr', 'month_aug', 'month_jul', 'month_jun', 'month_mar',
      'month_may', 'month_nov', 'month_oct', "poutcome_success"] if i in data_dummy.columns]

    data_final = data_dummy[features]
    return data_final