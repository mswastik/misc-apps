import pandas as pd
import panel as pn
import polars as pl
import urllib
import io
from pycaret.time_series import *
from datetime import datetime
import altair as alt
import dropbox
from dropbox import files
import requests
import json
import base64

alt.data_transformers.disable_max_rows()
pn.extension('vega')
pn.extension('tabulator')
alt.themes.enable('powerbi')
alt.renderers.enable("svg")
buffer = io.BytesIO()
but_s = """
:host {
        margin-left:5px;
        margin-top:18px;
        height: 35px;
        --font-size:15px;
        --panel-primary-color: #03DAC5;
}
:host(.solid) .bk-btn.bk-btn-primary:hover{
  opacity:70%;
  color:black;
}
:host(.solid) .bk-btn.bk-btn-primary {
  border-radius : 3px;
  transition: color 500ms;
  box-shadow: 0 4px 8px 0 rgba(0, 0, 0, 0.2);
}
"""
nav_s = """
:host {
  margin-left:20px;
  margin:0px !important;
  height: 45px;
  --font-size:18px;
}
:host(.solid) .bk-btn.bk-btn-default {
  background-color:black;
  color:        white;
  border-radius : 0px;
  transition: color 300ms ease-in, background-color 300ms ease-in, font-size 300ms ease-in;
}

:host(.solid) .bk-btn.bk-btn-default:hover{
  background-color:white;
  color:black;
  font-size:1.5em;
}
"""

APP_KEY = 'bfc7gt2wmbd081b'
APP_SECRET = 'lobt5soielr6u1y'
ACCESS_CODE_GENERATED = '_3-tzlmvZh4AAAAAAAABvqIDtvyQ2Jv5L0S0Bt8tJb4'
BASIC_AUTH = base64.b64encode(f'{APP_KEY}:{APP_SECRET}'.encode())
headers = {
    'Authorization': f"Basic {BASIC_AUTH}",
    'Content-Type': 'application/x-www-form-urlencoded',
}
data = f'code={ACCESS_CODE_GENERATED}&grant_type=authorization_code'

dd={
  "access_token": "sl.BtuGHJugT_O17vIm435pJMpaHz7AXIwJ5dwMzLiCCDPbXDJQejK2-O32_ddzBopPqgpPPq_9XsUSwkJjn2_IzjLnBDK-86sAhN-namCmITXCFY5-AwNrIEa30P4xS6tXWtvr9L8YLgSjF8Nli2xZsI8",
  "token_type": "bearer",
  "expires_in": 14400,
  "refresh_token": "o7ZbsNFsH4YAAAAAAAAAARShWlKOohLYoKGi0375MI9gcKVOafHLkRDUUzaaURt5",
  "scope": "account_info.read account_info.write contacts.read contacts.write file_requests.read file_requests.write files.content.read files.content.write files.metadata.read files.metadata.write",
  "uid": "28284714",
  "account_id": "dbid:AABK-WoaRoNWNqqX2a-5h8kCMnmSzdDolCw"
}
url=f'https://api.dropbox.com/oauth2/token?grant_type=refresh_token&refresh_token={dd["refresh_token"]}&client_id=bfc7gt2wmbd081b&client_secret=lobt5soielr6u1y'
response = requests.post(url)
tok=json.loads(response.content)['access_token']

dbx = dropbox.Dropbox(tok)
md, res = dbx.files_download('/user.json')
data=json.loads(res.content)
teml="""https://github.com/mswastik/forecastreview/raw/main/template.xlsx"""
tt=f'window.location.href = "{teml}";'
today=datetime.today()

class A:
    def __init__(self) -> None:
        self.df=pd.DataFrame()
        self.udf=pl.DataFrame()
        self.idf=pl.DataFrame()
        self.item = ''
        self.ch=''
        self.ch1=''
        self.user='test'
        self.un=''
        self.pw=''
        self.lun=''
        self.lpw=''
a=A()

r1=pn.Row()
r2=pn.Row()
r3=pn.Row()
r4=pn.Row()
r5=pn.Row()
c31=pn.Column()
c32=pn.Column()
r3.append(c31)
r3.append(c32)

genb=pn.widgets.Button(name='Generate Forecast',button_type='primary',stylesheets=[but_s])
def fileu(e):
    a.udf=pl.from_pandas(pd.read_excel(fup.value))
    r1.append(genb)
    a.udf=a.udf.melt('product')
    a.udf=a.udf.with_columns(pl.col('variable').str.to_date('%Y-%m-%d %H:%M:%S'))
    psel.options=a.udf['product'].unique().to_list()
    a.idf=a.udf.filter(pl.col('product')==psel.value)
    a.idf=a.idf.with_columns((pl.col('variable').dt.month()).alias('month'))
    a.idf=a.idf.with_columns((pl.col('variable').dt.year()).alias('year'))
    a.item=psel.value

def tdwn(e):
    f = urllib.request.urlopen(teml)
    myfile = pd.read_excel(f.read())
    myfile.to_excel(buffer)
  
def drawc():
    a.idf=a.idf.with_columns((pl.col('variable').dt.month()).alias('month'))
    a.idf=a.idf.with_columns((pl.col('variable').dt.year()).alias('year'))
    a.ch1=alt.Chart(a.idf.to_pandas()).mark_line(point=True).encode(x='variable',y='value',tooltip=['value','variable']).properties(width=550)
    a.ch=alt.Chart(a.idf.to_pandas()).mark_bar().encode(x='month:N',y='value',color='year:N', xOffset="year:N",tooltip=['value','month','year']).properties(width=650)

def genfc(e):
    a.idf=a.udf.filter(pl.col('product')==a.item)
    a.idf=a.idf.with_columns((pl.col('variable').dt.month()).alias('month'))
    sdf=a.idf.to_dummies(columns=['month']).to_pandas()
    sdf=sdf.set_index('variable')
    setup(sdf.drop(columns=['product']),fh=6,target='value')
    best=compare_models(include=['croston','arima','ridge_cds_dt','lightgbm_cds_dt'])
    fin=finalize_model(best)
    fd=pl.DataFrame({'variable':pl.select(pl.date_range(start=today.date(),end=today.date()+pl.duration(days=366),interval='1mo'))})
    fd=fd.with_columns((pl.col('variable').dt.month()).alias('month'))
    fd=fd.to_dummies(columns=['month'])
    r5.append(pn.widgets.Tabulator(pull()))
    fdf=predict_model(fin,fh=12,X=fd.to_pandas().set_index('variable'))
    fdf=fdf.reset_index()
    fdf['product']=a.item
    fdf['index']=fdf['index'].astype('datetime64[ns]')
    a.idf=a.idf.with_columns(pl.col('variable').cast(pl.Datetime).dt.cast_time_unit('ns'))
    fdf=fdf.rename(columns={'index':'variable'})
    fdf=a.idf[['product','variable','value']].join(pl.from_pandas(fdf),on=['product','variable'],how='outer_coalesce')
    fdf=fdf.to_pandas()
    fdf['month']=fdf['variable'].dt.month
    fdf['year']=fdf['variable'].dt.year.astype('str')
    fdf=fdf.fillna(0)
    fdf['acwfc']=fdf['value']+fdf['y_pred']
    fdf['variable']=fdf['variable'].dt.strftime('%Y-%m')
    a.df=fdf
    drawc()
    fdf['variable']=fdf['variable'].astype('datetime64[ns]')
    r4.clear()
    r4.append(pn.widgets.Tabulator(fdf.pivot(index=['product','year'],columns='month',values='acwfc').reset_index(),show_index=False))
    a.ch1=a.ch1+alt.Chart(fdf).mark_line(point=True).encode(x='variable',y='y_pred',color=alt.value('purple'),tooltip=['y_pred','variable'])
    a.ch=a.ch+alt.Chart(fdf[-12:]).mark_line(point=True).encode(x='month:N',y='y_pred',tooltip=['month','y_pred'],color='year:N')
    c32.clear()
    c32.append(pn.pane.Vega(a.ch))
    c31.clear()
    c31.append(pn.pane.Vega(a.ch1))
    dnf.disabled = False

dnt=pn.widgets.Button(name="Download Template",button_type='primary',stylesheets=[but_s])
dnt.js_on_click(code=tt)
dnf=pn.widgets.Button(name="Download Forecast",button_type='primary',stylesheets=[but_s])
dnf.disabled=True
genb.on_click(genfc)

fup=pn.widgets.FileInput(name="Upload Filled Template",margin=(23,9))
fup.param.watch(fileu, 'value')

psel=pn.widgets.Select(name='Select Product',options=[],width=200)
@pn.depends(psel,watch=True)
def itc(e):
    print(e)
    a.item = e

navb=pn.Row(pn.widgets.Button(name='Home',stylesheets=[nav_s]),pn.widgets.Button(name='Login',stylesheets=[nav_s]),styles={'background':'black','width':'100%'}).servable()
r1=pn.Row(dnt,dnf,fup,psel).servable()
pn.Column(r2,r3,r4,r5).servable()
