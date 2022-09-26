def data_cleaner():

    import pandas as pd
    import re
    import os

    df = pd.read_csv("/home/mrinal/airflow/input_raw/raw_store_transactions.csv")

    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()

    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id

    def remove_dollar(amount):
        return float(amount.replace('$', ''))

    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))
        
    
    os. getcwd()
    df.to_csv('/home/mrinal/airflow/output_cleaned_csv/clean_store_transactions.csv', index=False)
    

    
def data_downloader():
    import urllib.request

    def Download_Progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        progress = int((downloaded/total_size)*100)
        print ("Download Progress",str(progress),"%")
    url = "https://archive.org/download/raw_store_transactions/raw_store_transactions.csv"
    urllib.request.urlretrieve(url, '/home/mrinal/airflow/input_raw/raw_store_transactions.csv', reporthook=Download_Progress)
    print ("Finished")
    
def data_machine_learning():
    import pandas as pd 
    from sklearn.model_selection import KFold
    from sklearn.neighbors import KNeighborsRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    from sklearn.model_selection import GridSearchCV
    from sklearn.model_selection import train_test_split
    from sklearn.tree import DecisionTreeRegressor
    from sklearn.metrics import r2_score
    from sklearn.linear_model import LogisticRegression, LinearRegression
    import pickle
    scaler = StandardScaler()
    df = pd.read_csv("/home/mrinal/airflow/output_cleaned_csv/clean_store_transactions.csv")
    df=df.drop(['STORE_ID','PRODUCT_ID','Date'],axis=1)
    dum_df = pd.get_dummies(df)
    X = dum_df.drop('DISCOUNT',axis=1)
    y = dum_df['DISCOUNT']
    X_train, X_test, y_train, y_test = train_test_split(X,y,
                                            test_size=0.3,
                                            random_state=100
                                            )
    X_scl_trn = scaler.fit_transform(X_train)
    X_scl_tst = scaler.transform(X_test)
    lrmodel = LinearRegression()
    lrmodel.fit(X_train,y_train)
    y_pred = lrmodel.predict(X_test)

    print(r2_score(y_test,y_pred))
    filename = '/home/mrinal/airflow/output_pickle/finalized_model.pkl'
    pickle.dump(lrmodel, open(filename, 'wb'))
    
def data_email_ml():
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email import encoders
    import os

    def sendEmail(smtpHost, smtpPort, mailUname, mailPwd, fromEmail, mailSubject, mailContentHtml, recepientsMailList, attachmentFpaths):
        # create message object
        msg = MIMEMultipart()
        msg['From'] = fromEmail
        msg['To'] = ','.join(recepientsMailList)
        msg['Subject'] = mailSubject
        # msg.attach(MIMEText(mailContentText, 'plain'))
        msg.attach(MIMEText(mailContentHtml, 'html'))

        # create file attachments
        for aPath in attachmentFpaths:
            # check if file exists
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(aPath, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="{0}"'.format(os.path.basename(aPath)))
            msg.attach(part)

        # Send message object as email using smptplib
        s = smtplib.SMTP(smtpHost, smtpPort)
        s.starttls()
        s.login(mailUname, mailPwd)
        msgText = msg.as_string()
        sendErrs = s.sendmail(fromEmail, recepientsMailList, msgText)
        s.quit()

        # check if errors occured and handle them accordingly
        if not len(sendErrs.keys()) == 0:
            raise Exception("Errors occurred while sending email", sendErrs)


    # mail server parameters
    smtpHost = "smtp.gmail.com"
    smtpPort = 587
    mailUname = 'mrinal.dbda@gmail.com'
    mailPwd = 'pwymronhistjajyz'
    fromEmail = 'mrinal.dbda@gmail.com'

    # mail body, recepients, attachment files
    mailSubject = "Data Pipeline Executed-ML Model Saved"
    mailContentHtml = "Pipeline Executed Successfully. <br/> This is a automated <b>test</b> mail .Ml model has been saved <b>as attachment</b>"
    recepientsMailList = ["mrinal.dbda@gmail.com"]
    attachmentFpaths = []
    sendEmail(smtpHost, smtpPort, mailUname, mailPwd, fromEmail,
              mailSubject, mailContentHtml, recepientsMailList, attachmentFpaths)

    print("execution complete...")
    
    
def data_email_pr():
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email import encoders
    import os

    def sendEmail(smtpHost, smtpPort, mailUname, mailPwd, fromEmail, mailSubject, mailContentHtml, recepientsMailList, attachmentFpaths):
        # create message object
        msg = MIMEMultipart()
        msg['From'] = fromEmail
        msg['To'] = ','.join(recepientsMailList)
        msg['Subject'] = mailSubject
        # msg.attach(MIMEText(mailContentText, 'plain'))
        msg.attach(MIMEText(mailContentHtml, 'html'))

        # create file attachments
        for aPath in attachmentFpaths:
            # check if file exists
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(aPath, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="{0}"'.format(os.path.basename(aPath)))
            msg.attach(part)

        # Send message object as email using smptplib
        s = smtplib.SMTP(smtpHost, smtpPort)
        s.starttls()
        s.login(mailUname, mailPwd)
        msgText = msg.as_string()
        sendErrs = s.sendmail(fromEmail, recepientsMailList, msgText)
        s.quit()

        # check if errors occured and handle them accordingly
        if not len(sendErrs.keys()) == 0:
            raise Exception("Errors occurred while sending email", sendErrs)


    # mail server parameters
    smtpHost = "smtp.gmail.com"
    smtpPort = 587
    mailUname = 'mrinal.dbda@gmail.com'
    mailPwd = 'pwymronhistjajyz'
    fromEmail = 'mrinal.dbda@gmail.com'

    # mail body, recepients, attachment files
    mailSubject = "Data Pipeline Executed-Parquet File has been saved"
    mailContentHtml = "Pipeline Executed Successfully. <br/> This is a automated <b>test</b> mail . Parquet file successfully saved <b>as attachment</b>"
    recepientsMailList = ["mrinal.dbda@gmail.com"]
    attachmentFpaths = []
    sendEmail(smtpHost, smtpPort, mailUname, mailPwd, fromEmail,
              mailSubject, mailContentHtml, recepientsMailList, attachmentFpaths)

    print("execution complete...")
    
def data_converttoparquet():   
    import pandas as pd 
    df = pd.read_csv('/home/mrinal/airflow/output_cleaned_csv/clean_store_transactions.csv') 
    df.to_parquet('/home/mrinal/airflow/output_parquet/output.parquet') 

