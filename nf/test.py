from nicegui import ui
import polars as pl
import pandas as pd
import io
from pycaret.time_series import *
import json
from datetime import datetime
from dpb import data,dbx
from dropbox import files

buffer = io.BytesIO()

today=datetime.today().replace(day=1)
ui.colors(primary='#6200EE',secondary='#03DAC5')

navcl='w-24 drop-shadow-none shadow-none text-[#FFF] hover:font-black hover:bg-[#FFF] hover:text-[#000] rounded-none transition-all duration-200 ease-linear'
def nav():
    with ui.row(wrap=False).style(f'width:100dvw;min-height: 50px; margin-top:-15px; margin-left:-16px').classes('bg-[#000]'):
        with ui.link(target='/').style('min-height:50px'):
            ui.button('Home',color=None).style('min-height:50px').classes(navcl).props('flat')
        with ui.link(target='/review').style('min-height:50px'):
            ui.button('Review',color=None).style('min-height:50px').classes(navcl).props('flat')
        with ui.link(target='/insights').style('min-height:50px'):
            ui.button('Insights',color=None).style('min-height:50px').classes(navcl).props('flat')
        with ui.link(target='/login').style('min-height:50px'):
            ui.button('Login',color=None).style('min-height:50px').classes(navcl).props('flat')

nav()

r2=ui.row(wrap=False)
r3,r4,r5=ui.row(),ui.row(),ui.row()

with r2:
    c1,c2,c3,c4=ui.column(),ui.column(),ui.column(),ui.column()
with r3:
    c31,c32=ui.column(),ui.column()
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

def drawc():
    a.idf=a.idf.with_columns((pl.col('variable').dt.month()).alias('month'))
    a.idf=a.idf.with_columns((pl.col('variable').dt.year()).alias('year'))
    c31.clear()
    with c31:
        a.ch1=ui.echart({'xAxis':{'data':a.idf['variable'].to_list()},'yAxis': {},'series': [{'type': 'line', 'data': a.idf['value'].to_list()},{'type': 'line', 'data': []}]}).style('height:250px;width:650px')
        a.ch1.options['tooltip']={}
    c32.clear()
    with c32:
        a.ch=ui.echart({'xAxis':{'data': a.idf['month'].unique().to_list()},'yAxis': {},'legend':{},
                       'series': [{'type':'bar','name':i,'data': a.idf.filter(pl.col('year')==i)['value'].to_list()} for i in a.idf['year'].unique()]
                       }).style('height:250px;width:650px')
        a.ch.options['tooltip']={}

def selc(e):
    a.item=e.value
    a.idf=a.udf.filter(pl.col('product')==e.value)
    drawc()

async def genfc(e):
    async def intf():
        a.idf=a.udf.filter(pl.col('product')==a.item)
        a.idf=a.idf.with_columns((pl.col('variable').dt.month()).alias('month'))
        sdf=a.idf.to_dummies(columns=['month'],drop_first=True).to_pandas()
        setup(sdf.drop(columns=['product']),fh=6,target='value',index='variable')
        best=compare_models(include=['croston','arima','ridge_cds_dt','lightgbm_cds_dt'])
        fin=finalize_model(best)
        fd=pl.DataFrame({'variable':pl.select(pl.date_range(start=today.date(),end=today.date()+pl.duration(days=366),interval='1mo'))})
        fd=fd.with_columns((pl.col('variable').dt.month()).alias('month'))
        fd=fd.to_dummies(columns=['month'],drop_first=True)
        with r5:
            r5.clear()
            ui.table.from_pandas(pull()).style('max-height:250px;width:940px').props('virtual-scroll dense')
        fdf=predict_model(fin,fh=12,X=fd.to_pandas())
        fdf=fdf.reset_index()
        fdf['product']=a.item
        fdf['index']=fdf['index'].astype('datetime64[ns]')
        a.idf=a.idf.with_columns(pl.col('variable').cast(pl.Datetime).dt.cast_time_unit('ns'))
        fdf=fdf.rename(columns={'index':'variable'})
        fdf=a.idf[['product','variable','value']].join(pl.from_pandas(fdf),on=['product','variable'],how='outer_coalesce')
        fdf=fdf.to_pandas()
        f1=fdf[-12:]
        f1['month']=f1['variable'].dt.month
        fdf['variable']=fdf['variable'].dt.strftime('%Y-%m')
        fdf1=fdf.rename(columns={'variable':'date'})
        fdf1=fdf1.melt(['product','date'])
        a.df=fdf
        with r4:
            r4.clear()
            ui.table.from_pandas(fdf1.pivot(index=['product','variable'],columns='date',values='value').reset_index()).style('max-width:1446px').props('virtual-scroll')
        a.ch1.options['series'][1]['data']= fdf['y_pred'].to_list()
        a.ch1.options['xAxis']['data']= fdf['variable'].to_list()
        a.ch1.update()
        drawc()
        a.ch.options['series'].extend([{'type':'line','name':'pred','data': f1.sort_values('month')['y_pred'].to_list()}])
        a.ch.update()
        with c4:
            c4.clear()
            dfb=ui.button('Download Forecast',on_click=dfile,color='primary')
    def dd():
        with c4:
            ui.spinner('dots',size='4em')
    dd()
    await intf()


def fileu(e):
    a.udf=pl.read_excel(e.content)
    a.udf.write_excel(buffer)
    try:
        dbx.files_upload(f=buffer.getvalue(),path=f'/{a.user}/template.xlsx',mode=files.WriteMode.overwrite)
    except:
        dbx.files_create_folder_v2(f'/{a.user}')
        dbx.files_upload(f=buffer.getvalue(),path=f'/{a.user}/template.xlsx',mode=files.WriteMode.overwrite)
    with c3:
        pr=ui.select(label='Product',value=a.udf['product'].unique().to_list()[0],options=a.udf['product'].unique().to_list(),on_change=selc).style('padding-left:25px;width:280px')
        ui.button('Generate Forecast',on_click=genfc,color='primary').style('padding-left:25px;border-left:25px;width:280px')
    a.udf=a.udf.melt('product')
    a.udf=a.udf.with_columns(pl.col('variable').str.to_date('%m-%d-%y'))
    a.idf=a.udf.filter(pl.col('product')==pr.value)
    a.item=pr.value
    a.idf=a.idf.with_columns((pl.col('variable').dt.month()).alias('month'))
    a.idf=a.idf.with_columns((pl.col('variable').dt.year()).alias('year'))
    drawc()

def suf(e):
    data[a.un.value]=a.pw.value
    dbx.files_upload(f=json.dumps(data).encode('utf-8'),path='/user.json',mode=files.WriteMode.overwrite)
    a.user = a.lun.value
    ui.open('/')

def lif(e):
    md, res = dbx.files_download('/user.json')
    data=json.loads(res.content)
    try:
        if data[a.lun.value] == a.lpw.value:
            a.user = a.lun.value
            file = dbx.files_list_folder(f'/{a.user}').entries[0]
            md, con = dbx.files_download(f'/{a.user}/{file.name}')
            df=pl.from_pandas(pd.read_excel(con.content))
            ui.open('/')
            a.udf=df.melt('product')
            a.udf=a.udf.with_columns(pl.col('variable').str.to_date('%m-%d-%y'))
            a.idf=a.udf.filter(pl.col('product')==a.udf['product'].unique()[0])
            a.item=a.udf['product'].unique()[0]
            a.idf=a.idf.with_columns((pl.col('variable').dt.month()).alias('month'))
            a.idf=a.idf.with_columns((pl.col('variable').dt.year()).alias('year'))
            drawc()
            with c3:
                pr=ui.select(label='Product',value=a.udf['product'].unique().to_list()[0],options=a.udf['product'].unique().to_list(),on_change=selc).style('padding-left:25px;width:280px')
                ui.button('Generate Forecast',on_click=genfc,color='primary').style('padding-left:25px;border-left:25px;width:280px')
    except:
        ui.notify('Wrong username or password')

with c1:
    ui.button('Download Template',color='primary',on_click=lambda: ui.download(src="https://github.com/mswastik/fire/raw/8c707085c4e08c7e2275f1dae3d0a5976320e5d6/template.xlsx",filename='template.xlsx'))
with c2:
    ui.upload(label='Upload Filled Template',on_upload=fileu)

def dfile(e):
    a.df.to_excel(buffer,index=False)
    ui.download(src=buffer.getvalue(),filename='forecast.xlsx')



@ui.page('/review')
def review():
    nav()
    ui.label("Test Page")

@ui.page('/login',title='Login')
def login():
    ui.colors(primary='#6200EE',secondary='#03DAC5')
    nav()
    with ui.tabs().classes('w-full') as tabs:
        two = ui.tab('Signup')
        one = ui.tab('Login')
    with ui.tab_panels(tabs, value=two).classes('w-full'):
        with ui.tab_panel(two):
            a.un=ui.input(label="Email ID",validation={'Not an email!': lambda value: ("@" in value) & ("." in value)}).classes('content-center').style('margin:auto;min-height:50px;min-width:300px')
            a.pw=ui.input(label="Password",password=True,password_toggle_button=True).classes('content-center').style('margin:auto;min-height:50px;min-width:300px')
            ui.button('Signup',on_click=suf,color='primary').style('margin:auto')
        with ui.tab_panel(one):
            a.lun=ui.input(label="Email ID",validation={'Not an email!': lambda value: ("@" in value) & ("." in value)}).style('margin:auto;min-height:50px;min-width:300px')
            a.lpw=ui.input(label="Password",password=True,password_toggle_button=True).style('margin:auto;min-height:50px;min-width:300px')
            ui.button('Login',on_click=lif,color='primary').style('margin:auto')
        

ui.run(title="Fire Forecasting",binding_refresh_interval=1,reconnect_timeout=70,uvicorn_reload_includes='test.py')