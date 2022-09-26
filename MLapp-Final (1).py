# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 02:20:31 2020
@author: Mrinal Desai
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 02:20:31 2020
@author: Mrinal Desai
"""


import numpy as np
import pickle
import pandas as pd
#from flasgger import Swagger
import streamlit as st 

import SessionState
import time
import os
import signal

from PIL import Image

#app=Flask(__name__)
#Swagger(app)

pickle_in = open("/home/mrinal/airflow/output_pickle/finalized_model.pkl","rb")
classifier=pickle.load(pickle_in)

#@app.route('/')
def welcome():
    return "Welcome All"

#@app.route('/predict',methods=["Get"])
def predict_discount_authentication(list1):
    
    
    COLUMN_NAMES = ['STORE_LOCATION', 'PRODUCT_CATEGORY', 'MRP', 'CP', 'SP']
    df1 = pd.DataFrame(columns=COLUMN_NAMES) # Note that there are now row data inserted.
    COLUMN_NAMES_dum = ['MRP', 'CP', 'SP', 'STORE_LOCATION_Denver',
       'STORE_LOCATION_Houston', 'STORE_LOCATION_Miami',
       'STORE_LOCATION_New York', 'STORE_LOCATION_Washington',
       'PRODUCT_CATEGORY_Cosmetics', 'PRODUCT_CATEGORY_Education',
       'PRODUCT_CATEGORY_Electronics', 'PRODUCT_CATEGORY_Fashion',
       'PRODUCT_CATEGORY_Furniture', 'PRODUCT_CATEGORY_Groceries',
       'PRODUCT_CATEGORY_Kitchen']
    
    dum_df = pd.DataFrame(columns=COLUMN_NAMES_dum)



    df2 = pd.DataFrame([list1], columns=['STORE_LOCATION', 'PRODUCT_CATEGORY', 'MRP', 'CP', 'SP'])
    pd.concat([df2, df1])
    dum_df1 = pd.get_dummies(df2)
    df1 = pd.DataFrame(columns=dum_df.columns)
    import numpy as np
    a = np.intersect1d(dum_df.columns, dum_df1.columns)
    frames=[df1,dum_df1]
    df_merge=pd.concat(frames,join='outer', ignore_index=True)
    df_final=df_merge.fillna(0)
    array=classifier.predict(df_final)
    result=array[0]
   
    prediction=classifier.predict(df_final)
    print(prediction)
    return result
    



def main():
    st.title("Discount Predictor")
    html_temp = """
    <div style="background-color:tomato;padding:10px">
    <h2 style="color:white;text-align:center;">Streamlit Discount Predictor ML App </h2>
    </div>
    """
    st.markdown(html_temp,unsafe_allow_html=True)
    store = st.selectbox(
    'STORE_LOCATION',
    ('New York', 'Houston', 'Miami', 'Denver', 'Washington'))
    product = st.selectbox(
    'PRODUCT_CATEGORY',
    ('Electronics', 'Furniture', 'Kitchen', 'Fashion', 'Cosmetics',
       'Groceries', 'Education'))
   
    mrp = st.number_input("MRP")
    cp = st.number_input("CP")
    sp = st.number_input("SP")
    #entropy = st.text_input("entropy","Type Here")
    #'STORE_LOCATION', 'PRODUCT_CATEGORY', 'MRP', 'CP', 'SP'
    list1 = [store,product,mrp,cp,sp]
    result=""
    if st.button("Predict"):
        result=predict_discount_authentication(list1)
        print(type(result))
        if result>0:
         st.success('The Discount is {}'.format(result))
        else:
         st.success('The Discount is zero')
    
        
    if st.button("About"):
        st.text("Discount predictor")
        st.text("Built with Streamlit")
        from multiprocessing import Process


    




    

if __name__=='__main__':
    main()
