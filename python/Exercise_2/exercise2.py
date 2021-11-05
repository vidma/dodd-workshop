import urllib3
urllib3.disable_warnings()
import pandas as pd
data = pd.read_csv('../Exercise_1/data/datalake/customer_notebook_2.csv')
data.rename(columns={'Credit_History':'Credit_History_Null','Loan_Amount_Term':'Term'})
replaced_data = data.replace(360,">180")
final_set = replaced_data.drop('Self_Employed',axis=1)
final_set.to_csv('customer_180_.csv')