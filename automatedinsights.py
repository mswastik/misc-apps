#import numpy as np
#import os
import pandas as pd
import polars as pl
import altair as alt
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import panel as pn
import bokeh
import warnings
from panel.theme import Material
#import perspective

pn.extension('tabulator')
pn.extension('vega')
alt.themes.enable('powerbi')
alt.data_transformers.disable_max_rows()
warnings.filterwarnings('ignore')

today=datetime.today().replace(day=1)
period = datetime.strptime('01-12-2023','%d-%m-%Y')
lmon=(date(today.year, today.month, 1)-relativedelta(months=1))

sk= 550
#sk=300  #SSP
class ss():
    l2fc='L2 Stat Final Rev'
    l0fc='`Fcst Stat Final Rev'
a=ss()
a.l2fc='L2 DF Final Rev'
a.l0fc='`Fcst DF Final Rev'

#loch = 'Country'
#prodh = 'Franchise'
#loc = 'UNITED STATES'
#prod = 'CMF'

def genai(loch,loc,prodh,prod):
    df=pl.read_parquet("C:\\Users\\smishra14\\OneDrive - Stryker\\data\\Instruments.parquet")
    #df=pl.read_parquet("C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet")
    #df=df.drop(columns=["IBP Level 6","IBP Level 5","Business Sector",'Business Unit','Franchise','Product Line'])
    #print(df.columns)
    #df1=pl.read_parquet("C:\\Users\\smishra14\\OneDrive - Stryker\\data\\hierarchy.parquet")
    ##df1=df1.unique(['CatalogNumber'],keep='last')   ##Temporary fix
    #df=df.join(df1,on='CatalogNumber',how='left')
    df=df.filter(pl.col(prodh)==prod)
    df=df.filter(pl.col(loch)==loc)
    df=df.with_columns(pl.col('SALES_DATE').cast(pl.Datetime).dt.cast_time_unit('us'))

    #if len(df['Country'].unique())==1:
    #    co=df['Country'].unique()[0]
    #elif len(df['Region'].unique())==1:
    #    co = df['Region'].unique()[0]
    #else:
    #    co=''
    #bu=''
    #if len(df['Business Unit'].unique())==1:
    #    bu = df['Business Unit'].unique()[0]
    #else:
    #    bu=''
    if loc=='UNITED STATES':
        if (df['Franchise'].unique()[0]=='Instruments') | (df['Franchise'].unique()[0]=='Endo'):
            df=df.with_columns(pl.col('`Act Orders Rev')/pl.col('UOM'))
            df=df.with_columns(pl.col('L2 DF Final Rev')/pl.col('UOM'))
            df=df.with_columns(pl.col('L2 Stat Final Rev')/pl.col('UOM'))
            df=df.with_columns(pl.col('L0 DF Final Rev')/pl.col('UOM'))
            df=df.with_columns(pl.col('L1 DF Final Rev')/pl.col('UOM'))
            df=df.with_columns(pl.col('`Fcst DF Final Rev')/pl.col('UOM'))
            df=df.with_columns(pl.col('`Fcst Stat Final Rev')/pl.col('UOM'))
            df=df.with_columns(pl.col('`Fcst Stat Prelim Rev')/pl.col('UOM'))
    #print(df['UOM'].unique())
    df=df.drop(columns=["Item_id","IBP Level 6",'Selling Division', 'Area', 'Stryker Group Region', 'firstofmonth','Source','Country','Business Sector','Business Unit','UOM','Item_id', 'gim_itemid', 'NormalizedCatalogNumber','Act Orders Rev Val','`L0 ASP Final Rev'])
    df=df.group_by(['Region','Franchise','CatalogNumber','Product Line','IBP Level 5','SALES_DATE']).sum()
    df=df.with_columns((pl.when(df['SALES_DATE']<datetime(today.year,today.month,1)).then(df['`Act Orders Rev']).otherwise(df['`Fcst DF Final Rev'])).alias('actwfc'))
    df1=df.filter((df['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (df['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)))
    cc=list(df1.group_by('CatalogNumber').sum().sort(by='actwfc',descending=True)['CatalogNumber'][:sk])
    df1=df1.filter(df1["CatalogNumber"].is_in(cc))

    '''df2=df1.filter(pl.col('SALES_DATE')<datetime(today.year,today.month,1))
    df2=df1.pivot(columns='CatalogNumber',index='SALES_DATE',values='`Act Orders Rev',aggregate_function='sum') #use unstack
    df2=df2.select(pl.exclude('SALES_DATE'))
    df2=df2.corr()
    df2=df2.with_columns(pl.Series(name="CatalogNumber", values=df2.columns))
    df2=df2.melt('CatalogNumber')
    df2=df2.filter((pl.col('value')!=0) & (pl.col('value').is_not_nan()))
    df2=df2.rename({'value':'Correlation','variable':'CatalogNumber1'})
    df2=df2.filter((abs(df2['Correlation'])>=.95) & (df2['CatalogNumber']!=df2['CatalogNumber1']))

    df3=df1.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1))
    df3=df1.pivot(columns='CatalogNumber',index='SALES_DATE',values='L2 DF Final Rev',aggregate_function='sum')
    df3=df3.select(pl.exclude('SALES_DATE'))
    df3=df3.corr()
    df3=df3.with_columns(pl.Series(name="CatalogNumber", values=df3.columns))
    df3=df3.melt('CatalogNumber')
    df3=df3.filter((pl.col('value')!=0) & (pl.col('value').is_not_nan()))
    df3=df3.rename({'value':'FC Correlation','variable':'CatalogNumber1'})  
    df3=df3.filter(df3['CatalogNumber']!=df3['CatalogNumber1'])

    cdf=df2.join(df3,on=['CatalogNumber','CatalogNumber1'],how='left')
    tm=cdf.clone()
    tt=pl.DataFrame()
    dd=[]
    for dat in cdf['CatalogNumber'].unique():
        tm=tm.filter((~pl.col('CatalogNumber').is_in(dd)) & (pl.col('CatalogNumber1')!=dat))
        dd.append(dat)
        tt=pl.concat([tt,tm.filter(pl.col('CatalogNumber')==dat)])
    cdf=tt.clone()
    cdf=cdf.with_columns(abs(pl.col('Correlation')-pl.col('FC Correlation')).alias('Diff')).sort('Diff',descending=True)
    cdf=cdf.filter(pl.col('FC Correlation').is_not_null())

    cdf=cdf.head(9)
    fc=df1.filter((df1['SALES_DATE']>=datetime(today.year, today.month, 1)) & (df1['SALES_DATE']<=datetime(today.year, today.month, 1)+relativedelta(months=12)))
    chl=[]
    tdf=df1.filter(df1['SALES_DATE']<datetime(today.year, today.month, 1))
    tdf=tdf.sort('SALES_DATE')
    for dat in cdf.iter_rows():
        tdf1=tdf.filter(tdf['CatalogNumber']==dat[0]).sort('SALES_DATE')
        tdf2=tdf.filter(tdf['CatalogNumber']==dat[1]).sort('SALES_DATE')
        fc1=fc.filter(fc['CatalogNumber']==dat[0]).sort('SALES_DATE')
        fc2=fc.filter(fc['CatalogNumber']==dat[1]).sort('SALES_DATE')
        c1=alt.Chart(tdf1.to_pandas(),title=f'{dat[0]} vs {dat[1]}').mark_line(color='#E91E63',strokeWidth=1.2,point={"filled": False,"opacity":0}).encode(x=alt.X('SALES_DATE:T',axis=alt.Axis(format="%b-%y"),title=''), y='sum(`Act Orders Rev):Q',
                                                            tooltip=['CatalogNumber:O','sum(`Act Orders Rev):Q','SALES_DATE:T']).properties(width=185,height=135)
        c2=alt.Chart(tdf2.to_pandas()).mark_line(color='#42A5F5',strokeWidth=1.2,point={"filled": False,"opacity":0}).encode(x='SALES_DATE:T', y='sum(`Act Orders Rev):Q',
                                                                tooltip=['CatalogNumber:O','sum(`Act Orders Rev):Q','SALES_DATE:T'])
        c3=alt.Chart(fc1.to_pandas()).mark_line(color='#E91E63',strokeWidth=1.2,point={"filled": False,"opacity":0}).encode(x='SALES_DATE:T', y=alt.Y('`Fcst DF Final Rev','sum',title='Fcst '+ 'DF'),
                                                            tooltip=['CatalogNumber:O','`Fcst DF Final Rev','SALES_DATE:T'])
        c4=alt.Chart(fc2.to_pandas()).mark_line(color='#42A5F5',strokeWidth=1.2,point={"filled": False,"opacity":0}).encode(x='SALES_DATE:T', y=alt.X('`Fcst DF Final Rev','sum',title='Fcst '+ 'DF'),
                                                            tooltip=['CatalogNumber:O','`Fcst DF Final Rev','SALES_DATE:T'])
        chl.append(c1+c2+c3+c4)
    corr=pn.Row(pn.Column("## <span style='color:#FFCC33'>Correlated</span> SKUs",pn.widgets.Tabulator(cdf.to_pandas(),show_index=False,widths={'CatalogNumber':125,'CatalogNumber1':125,'FC Correlation':100}),styles=dict(border='solid')),
                alt.concat(*chl, columns=3))'''

    gdf=df.clone().sort(['IBP Level 5','SALES_DATE'],descending=False)
    gdf=gdf.group_by('IBP Level 5',pl.col('SALES_DATE').dt.year()).agg(pl.sum('actwfc')).sort(['IBP Level 5','SALES_DATE'],descending=False).with_columns(pl.col('actwfc').pct_change().over('IBP Level 5').alias("yoy growth"))[['IBP Level 5','SALES_DATE','actwfc','yoy growth']]
    gdf=gdf.with_columns((pl.col('yoy growth')).alias('yoy growth'))
    gdf=gdf.with_columns(pl.col('yoy growth').diff().over('IBP Level 5').alias("LY YoY"))

    # Brands with steep growth compared to this year
    gdf=gdf.with_columns(abs(pl.col('yoy growth')-pl.col('LY YoY')).alias('diff'))
    gdf=gdf.sort(by='diff',descending=True)
    #print(gdf['SALES_DATE'].unique())
    gdf1=gdf.filter(pl.col('SALES_DATE')==today.year+1).filter(pl.col('diff').is_not_nan()).filter(pl.col('diff').is_finite()).filter(pl.col('actwfc')>100).drop_nulls().sort(['diff'],descending=True)
    #gdf1=gdf.filter(pl.col('SALES_DATE')==today.year+1).drop_nulls().sort(['diff'],descending=True)    
    #print(gdf1['SALES_DATE'].unique())
    tmdf=gdf.filter(pl.col('IBP Level 5').is_in(gdf1['IBP Level 5'][:10])).to_pandas().drop(columns=['LY YoY','diff'])
    tmdf['yoy growth']=tmdf['yoy growth'].map('{:.1%}'.format)
    tmdf['actwfc']=tmdf['actwfc'].round(0).map('{:,.0f}'.format)
    tmdf=tmdf.melt(['IBP Level 5','SALES_DATE'])
    tmdf=tmdf.pivot(index=['IBP Level 5','variable'],columns='SALES_DATE',values='value')
    dfc1=pn.Column("## <span style='color:#FFCC33'>YoY</span> Change vs LY",pn.widgets.Tabulator(tmdf.drop(columns=tmdf.columns[0]),hidden_columns=['variable'],groupby=['variable'],text_align='right'),styles=dict(border='solid'))
    #dfc1=pn.Column("## <span style='color:#FFCC33'>YoY</span> Change vs LY",pn.widgets.Tabulator(tmdf,groupby=['variable'],text_align='right'),styles=dict(border='solid'))

    # Brands with high growth next year
    gdf2=gdf.filter(pl.col('SALES_DATE')==today.year+1).filter(pl.col('yoy growth').is_not_nan()).filter(pl.col('yoy growth').is_finite()).filter(pl.col('actwfc')>100).drop_nulls().sort(['yoy growth'],descending=True)
    tmdf2=gdf.filter(pl.col('IBP Level 5').is_in(gdf2['IBP Level 5'][:10])).to_pandas().drop(columns=['LY YoY','diff'])
    tmdf2['yoy growth']=tmdf2['yoy growth'].map('{:.1%}'.format)
    tmdf2['actwfc']=tmdf2['actwfc'].round(0).map('{:,.0f}'.format)
    tmdf2=tmdf2.melt(['IBP Level 5','SALES_DATE'])
    tmdf2=tmdf2.pivot(index=['IBP Level 5','variable'],columns='SALES_DATE',values='value')
    dfc2=pn.Column("## <span style='color:#FFCC33'>YoY</span> Change vs LY",pn.widgets.Tabulator(tmdf2.drop(columns=tmdf2.columns[0]),hidden_columns=['variable'],groupby=['variable'],text_align='right'),styles=dict(border='solid'))
    #dfc2=pn.Column("## <span style='color:#FFCC33'>YoY</span> Change vs LY",pn.widgets.Tabulator(tmdf2,groupby=['variable'],text_align='right'),styles=dict(border='solid'))

    df5=df1.clone()
    df5=df5.with_columns(abs(df5['`Act Orders Rev']-df5['L2 Stat Final Rev']).alias('L2 Stat Abs Err'))
    df5=df5.with_columns(((1-df5['L2 Stat Abs Err']/df5['`Act Orders Rev'])).alias('L2 Stat Acc'))
    df5=df5.with_columns(abs(df5['`Act Orders Rev']-df5['L2 DF Final Rev']).alias('L2 DF Abs Err'))
    df5=df5.with_columns(((1-df5['L2 DF Abs Err']/df5['`Act Orders Rev'])).alias('L2 DF Acc'))
    df5=df5.with_columns(pl.when(df5['`Act Orders Rev']==0).then(1).otherwise(df5['L2 Stat Acc']))
    df5=df5.with_columns(df5['L2 Stat Acc'].clip(0,df5['L2 Stat Acc'].max()).alias('L2 Stat Acc'))
    df5=df5.with_columns(pl.when(df5['`Act Orders Rev']==0).then(1).otherwise(df5['L2 DF Acc']))
    df5=df5.with_columns(df5['L2 DF Acc'].clip(0,df5['L2 DF Acc'].max()).alias('L2 DF Acc'))
    df5=df5.with_columns((df5['`Act Orders Rev']-df5['L2 DF Final Rev']).alias('Bias'))
    df5=df5.with_columns((df5['L2 DF Acc']-df5['L2 Stat Acc']).alias('FVA'))

    tmdf=df5.filter((df5['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=3)) & (df5['SALES_DATE']<=datetime(today.year,today.month,1)-relativedelta(months=1)))
    tmdf=tmdf.join(tmdf.group_by('CatalogNumber').agg([pl.sum('`Act Orders Rev').alias("3m orders")]),on=['CatalogNumber'],how='left')
    tmdf=tmdf.join(tmdf.group_by('CatalogNumber').agg([pl.sum('L2 DF Abs Err').alias("3m l2DF err")]),on=['CatalogNumber'],how='left')
    tmdf=tmdf.join(tmdf.group_by('CatalogNumber').agg([pl.sum('L2 Stat Abs Err').alias("3m l2Stat err")]),on=['CatalogNumber'],how='left')
    tmdf=tmdf.join(tmdf.group_by('CatalogNumber').agg([pl.sum('L2 Stat Final Rev').alias("3m l2 stat")]),on=['CatalogNumber'],how='left')
    tmdf=tmdf.join(tmdf.group_by('CatalogNumber').agg([pl.sum('L2 DF Final Rev').alias("3m l2 df")]),on=['CatalogNumber'],how='left')

    tmdf=tmdf.with_columns((tmdf['3m orders']/tmdf['`Act Orders Rev'].sum()).alias('3m orders cont'))
    tmdf=tmdf.with_columns((tmdf['3m l2DF err']/tmdf['L2 DF Abs Err'].sum()).alias('3m DF err cont'))
    tmdf=tmdf.with_columns((tmdf['3m l2Stat err']/tmdf['L2 Stat Abs Err'].sum()).alias('Stat err cont'))
    tmdf=tmdf.with_columns((tmdf['3m DF err cont']/tmdf['3m orders cont']).alias('DF err ratio'))
    tmdf=tmdf.with_columns(((tmdf['3m orders']-tmdf['3m l2 stat'])/tmdf['3m orders']).alias('3m StatBias'))
    tmdf=tmdf.with_columns(((tmdf['3m orders']-tmdf['3m l2 df'])/tmdf['3m orders']).alias('3m DFBias'))
    tmdf=tmdf.with_columns(((1-tmdf['3m l2DF err']/tmdf['`Act Orders Rev'])).alias('3m l2df acc'))
    tmdf=tmdf.with_columns(tmdf['3m l2df acc'].clip(0,tmdf['3m l2df acc'].max()).alias('3m l2df acc'))
    tmdf=tmdf.with_columns(((1-tmdf['3m l2Stat err']/tmdf['`Act Orders Rev'])).alias('3m l2stat acc'))
    tmdf=tmdf.with_columns(tmdf['3m l2stat acc'].clip(0,tmdf['3m l2stat acc'].max()).alias('3m l2stat acc'))
    tmdf=tmdf.with_columns((tmdf['3m l2df acc']-tmdf['3m l2stat acc']).alias('3m FVA'))
    tmdf=tmdf[['IBP Level 5','CatalogNumber','SALES_DATE','Region','3m orders cont','3m DF err cont','Stat err cont','L2 DF Acc','L2 Stat Acc','3m StatBias','3m DFBias','3m FVA','3m l2df acc','3m l2stat acc','DF err ratio']]
    df5=df5.join(tmdf,on=['CatalogNumber','SALES_DATE','Region'],how='left')
    df5=df5.with_columns((df5['L2 DF Final Rev']-df5['`Fcst DF Final Rev']).alias('FC change'))
    df5=df5.with_columns(abs(df5['FC change']).alias('FC change'))
    df5=df5.sort('SALES_DATE',descending=False)
    df5=df5.with_columns(pl.col('L2 Stat Acc').diff().over(['CatalogNumber','Region']).alias('Stat Decrease'))
    df5=df5.with_columns(pl.col('L2 DF Acc').diff().over(['CatalogNumber','Region']).alias('DF Decrease'))
    df5=df5.with_columns(pl.col('L2 DF Abs Err').diff().over(['CatalogNumber','Region']).alias('L2 DF Err Inc'))
    df5=df5.with_columns(pl.col('L2 Stat Abs Err').diff().over(['CatalogNumber','Region']).alias('L2 Stat Err Inc'))
    

    df6=df5.melt(['IBP Level 5','CatalogNumber','SALES_DATE','3m orders cont','3m DF err cont','Stat err cont','L2 Stat Acc','L2 DF Acc','FC change','3m DFBias','3m StatBias','3m FVA','Stat Decrease','DF Decrease'])
    df6=df6.rename({'variable':'type'})
    frmt2={'3m DFBias':bokeh.models.NumberFormatter(format="0.0%",text_align='right'),'3m DF err cont':bokeh.models.NumberFormatter(format="0.3%",text_align='right'),'3m FVA':bokeh.models.NumberFormatter(format="0.0%",text_align='right'),'3m l2df acc':bokeh.models.NumberFormatter(format="0.0%",text_align='right'),'3m l2stat acc':bokeh.models.NumberFormatter(format="0.0%",text_align='right'),'L2 DF Acc':bokeh.models.NumberFormatter(format="0.0%",text_align='right'),'3m orders cont':bokeh.models.NumberFormatter(format="0.0%",text_align='right'),'3m DF err cont':bokeh.models.NumberFormatter(format="0.0%",text_align='right')}
    ddf1=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).top_k(80,by='3m DF err cont').to_pandas().sort_values('3m FVA')[['IBP Level 5','CatalogNumber','3m FVA']][:10]
    ddf2=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).top_k(80,by='3m DF err cont').filter(pl.col('3m DFBias')<-.05).to_pandas()[['IBP Level 5','CatalogNumber','3m DFBias','3m DF err cont']][:10]
    ddf3=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).top_k(80,by='3m DF err cont').filter(pl.col('3m DFBias')>.05).to_pandas()[['IBP Level 5','CatalogNumber','3m DFBias','3m DF err cont']][:10]
    cat=list(set(ddf1['CatalogNumber'].to_list()+ddf2['CatalogNumber'].to_list()+ddf3['CatalogNumber'].to_list()))
    fdf=df5.filter(pl.col('CatalogNumber').is_in(cat)).melt(id_vars=['IBP Level 5','CatalogNumber','SALES_DATE'],value_vars=['L2 Stat Final Rev','L2 DF Final Rev','`Fcst Stat Final Rev','`Fcst DF Final Rev','`Act Orders Rev'])

    def content_fn(row):
        return (alt.Chart(fdf.filter(pl.col('CatalogNumber')==row['CatalogNumber']).filter(pl.col('variable').is_in(['`Fcst DF Final Rev','`Fcst Stat Final Rev','`Act Orders Rev'])
                ).to_pandas()).mark_line(strokeWidth=1.8,point={"filled": False,"opacity":0}).encode(y=alt.Y('sum(value):Q',title='Rev Qty'),x=alt.X('SALES_DATE:T',axis=alt.Axis(format="%b-%y"),title=None),
                                            color=alt.Color('variable').legend(orient="bottom"),tooltip=['variable','value','SALES_DATE']).properties(height=155)
                + alt.Chart(pd.DataFrame({'SDate': [datetime(today.year,today.month,1)-relativedelta(months=1)]})).mark_rule().encode(x = 'SDate:T'))
                #+ alt.Chart(fdf.filter(pl.col('CatalogNumber')==row['CatalogNumber']).filter(pl.col('variable')=='`Fcst DF Final Rev').to_pandas()).mark_bar().encode(y='sum(value):Q',x=alt.X('SALES_DATE:T'),tooltip=['variable','value','SALES_DATE']))

    row = pn.Row(pn.Column("## <span style='color:#FFCC33'>Top</span> -ve FVA",pn.widgets.Tabulator(ddf1,show_index=False,row_content=content_fn,embed_content=True,formatters={'3m FVA':bokeh.models.NumberFormatter(format="0.0%",text_align='right')}),styles=dict(border='solid')))
    row1 = pn.Row(pn.Column("## <span style='color:#FFCC33'>Top</span> -ve 3M DF Bias",pn.widgets.Tabulator(ddf2,show_index=False,row_content=content_fn,embed_content=True,formatters=frmt2),styles=dict(border='solid')))
    row2 = pn.Row(pn.Column("## <span style='color:#FFCC33'>Top</span> +ve 3M DF Bias",pn.widgets.Tabulator(ddf3,show_index=False,row_content=content_fn,embed_content=True,formatters=frmt2),styles=dict(border='solid')))

    fdf1=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).filter(pl.col('3m FVA')<0).to_pandas().sort_values('3m DF err cont',ascending=False)[['IBP Level 5','CatalogNumber','3m l2df acc','3m l2stat acc','3m FVA','3m DF err cont']][:10]
    fvt=pn.Column("## <span style='color:#FFCC33'>High</span> Err -ve FVA",pn.widgets.Tabulator(fdf1.sort_values('3m DF err cont',ascending=False),show_index=False,formatters=frmt2),styles=dict(border='solid'))
    
    # Items with Maximum Error Increase
    fdf1=df5.filter(pl.col('CatalogNumber').is_in(fdf1['CatalogNumber'])).melt(id_vars=['IBP Level 5','CatalogNumber','SALES_DATE'],value_vars=['L2 Stat Final Rev','L2 DF Final Rev','`Act Orders Rev']).to_pandas()
    edf=df5.filter(pl.col('SALES_DATE').is_in([datetime(today.year,today.month,1)-relativedelta(months=2),datetime(today.year,today.month,1)-relativedelta(months=1)]))
    #print(edf.filter(pl.col("SALES_DATE")==datetime(today.year,today.month,1)-relativedelta(months=1)))
    #print(edf.drop('CatalogNumber','SALES_DATE','IBP Level 5','Region','Franchise','Product Line')) 
    tes=edf.filter(pl.col("SALES_DATE")==datetime(today.year,today.month,1)-relativedelta(months=1)).drop_nulls('L2 DF Err Inc').sort('L2 DF Err Inc',descending=True)[['CatalogNumber']][:10]
    edf=edf.filter(pl.col('CatalogNumber').is_in(list(tes)[0]))
    edf=edf.to_pandas()
    #print(df5.filter(df5['CatalogNumber']=='200072-TRA')[['CatalogNumber','SALES_DATE','`Act Orders Rev','DF Decrease','L2 DF Abs Err','L2 DF Err Inc','L2 DF Final Rev']])
    #df5.filter(df5['CatalogNumber'].is_in(list(tes)[0]))[['CatalogNumber',"IBP Level 5",'Franchise','Product Line']].write_excel('fdfd.xlsx')
    #print(edf[['CatalogNumber','IBP Level 5','L2 DF Err Inc','L2 DF Abs Err']])
    edf['SALES_DATE']=edf['SALES_DATE'].dt.date
    frmt3={i:bokeh.models.NumberFormatter(format="0,0",text_align='right') for i in edf['SALES_DATE'].unique()}
    et1=pn.Column("## <span style='color:#FFCC33'>Maximum</span> Err Increase",pn.widgets.Tabulator(edf.pivot(index=['IBP Level 5','CatalogNumber'],
                                                columns='SALES_DATE',values='L2 DF Abs Err').sort_values(lmon,ascending=False),formatters=frmt3),styles=dict(border='solid'))

    edf2=df5.filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1)).filter(pl.col('DF err ratio')>1)
    tes=edf2.drop_nulls('L2 DF Err Inc').sort('3m DF err cont',descending=True)[['CatalogNumber']][:20]
    edf2=df5.filter(pl.col('CatalogNumber').is_in(list(tes)[0])).filter(pl.col('SALES_DATE')==datetime(today.year,today.month,1)-relativedelta(months=1))
    edf2=edf2.to_pandas()
    edf2['SALES_DATE']=edf2['SALES_DATE'].dt.date
    et2=pn.widgets.Tabulator(edf2[['IBP Level 5','CatalogNumber','L2 DF Acc', '3m DFBias','3m orders cont','3m DF err cont']].sort_values('3m DF err cont',ascending=False),row_content=content_fn,embed_content=True,show_index=False,formatters=frmt2)
    etp=pn.Column("## <span style='color:#FFCC33'>High</span> Err Contributor",et2,styles=dict(border='solid'))

    df6=df5.to_pandas()
    input_dropdown = alt.binding(input='search', name='CatalogNumber    ')
    df_color = alt.condition(alt.datum['FcstDF']>=alt.datum['L2DF'],alt.value("#FFCC33"),alt.value("#ae1325"))
    st_color = alt.condition(alt.datum['FcstStat'] >= alt.datum['L2Stat'],alt.value("#808080"),alt.value("#ae1325"))
    its = alt.selection_point(fields=['CatalogNumber'],bind=input_dropdown,nearest=True,clear="dblclick")
    base=alt.Chart(df6).encode(alt.X('SALES_DATE:T',axis=alt.Axis(format="%b-%y"))).properties(height=300,width=400,title='Act Orders & Forecasts').transform_joinaggregate(L2DF='sum(L2 DF Final Rev):Q',
                                            FcstDF='sum(`Fcst DF Final Rev):Q',L2Stat='sum(L2 Stat Final Rev):Q',FcstStat='sum(`Fcst Stat Final Rev):Q',sumact='sum(`Act Orders Rev):Q',groupby=["SALES_DATE"])
    c1=base.mark_line().encode(y='sum(`Act Orders Rev):Q',color=alt.value("#118DFF"),tooltip=['sum(`Act Orders Rev):Q','SALES_DATE']).transform_filter(
        f'year(datum.SALES_DATE)*100+month(datum.SALES_DATE)+1<{today.year*100+today.month}')
    c2=base.mark_line().encode(y='sum(`Fcst DF Final Rev):Q',color=alt.value("#12239E"),tooltip=['sum(`Fcst DF Final Rev):Q','sum(L2 DF Final Rev):Q','SALES_DATE'])
    c3=base.mark_line().encode(y='sum(`Fcst Stat Final Rev):Q',color=alt.value("#E66C37"),tooltip=['sum(`Fcst Stat Final Rev):Q','sum(L2 Stat Final Rev):Q','SALES_DATE'])
    #c4=base.mark_bar().encode(y2='L2DF:Q',y="FcstDF:Q",color=df_color,tooltip=['L2DF:Q','FcstDF:Q','SALES_DATE']).transform_filter(
    #    f'year(datum.SALES_DATE)>={today.year}').transform_filter(f'month(datum.SALES_DATE)>={today.month}')
    #c5=base.mark_bar().encode(y2='L2Stat:Q',y="FcstStat:Q",color=st_color,tooltip=['L2Stat:Q','FcstStat:Q','SALES_DATE:T']).transform_filter(
    #    f'year(datum.SALES_DATE)>={today.year}').transform_filter(f'month(datum.SALES_DATE)>={today.month}')
    l1 = base.encode(x='max(SALES_DATE):T',y=alt.value(10),text=alt.value('Act Orders'),color=alt.value("#118DFF")).mark_text(align='left', dx=9,fontWeight='bold')
    l2 = base.encode(x='max(SALES_DATE):T',y=alt.value(30),text=alt.value('DF'),color=alt.value("#12239E")).mark_text(align='left', dx=9,fontWeight='bold')
    l3 = base.encode(x='max(SALES_DATE):T',y=alt.value(50),text=alt.value('Stat'),color=alt.value("#E66C37")).mark_text(align='left', dx=9,fontWeight='bold')

    ibdf = df5.filter(pl.all_horizontal(pl.col('`Act Orders Rev','L2 DF Final Rev','L2 Stat Final Rev','`Fcst DF Final Rev','`Fcst Stat Final Rev','L2 Stat Abs Err','L2 DF Abs Err').fill_nan(0))).filter(pl.all_horizontal(pl.col('`Act Orders Rev','L2 DF Final Rev','L2 Stat Final Rev','`Fcst DF Final Rev','`Fcst Stat Final Rev','L2 Stat Abs Err','L2 DF Abs Err').is_finite())).group_by('IBP Level 5','SALES_DATE').sum()
    #ibdf = df5.filter(pl.all().is_not_nan()).filter(pl.all().is_finite()).group_by('IBP Level 5').sum()
    ibdf=ibdf[['IBP Level 5','SALES_DATE','`Act Orders Rev','L2 DF Final Rev','L2 Stat Final Rev','`Fcst DF Final Rev','`Fcst Stat Final Rev','L2 Stat Abs Err','L2 DF Abs Err']]
    ibdf=ibdf.with_columns(((1-ibdf['L2 Stat Abs Err']/ibdf['`Act Orders Rev'])).alias('L2 Stat Acc'))
    ibdf=ibdf.with_columns(((1-ibdf['L2 DF Abs Err']/ibdf['`Act Orders Rev'])).alias('L2 DF Acc'))
    ib1=ibdf.filter(pl.col('SALES_DATE')==today.date())
    ib1=ib1.with_columns((ib1['`Act Orders Rev']/ib1['`Act Orders Rev'].sum()).alias('orders cont'))
    ib1=ib1.with_columns((ib1['L2 DF Abs Err']/ib1['L2 DF Abs Err'].sum()).alias('DF err cont'))
    ib1=ib1.with_columns((ib1['L2 DF Acc']-ib1['L2 Stat Acc']).alias('FVA'))
    ib1=ib1.sort('DF err cont',descending=True)
    bb1=alt.Chart(ib1.to_pandas()).mark_arc(outerRadius=80,innerRadius=30,opacity=.76,stroke="#fff").encode(theta=alt.Theta('DF err cont:Q',stack=True,),order='DF err cont',color="IBP Level 5:N").properties(width=160,height=80)
    #bb1=(base.mark_arc(outerRadius=80,innerRadius=30,opacity=.76,stroke="#fff")+base.mark_text(radius=125).encode(text='IBP Level 5').transform_filter(alt.datum['DF err cont'] > .02)).properties(width=160,height=80)
    #pn.extension(design='material')
    bb2=alt.Chart(ib1.to_pandas()).mark_bar(opacity=.76,stroke="#fff").encode(y=alt.Y('FVA:Q',axis=alt.Axis(format='%') ),x=alt.X('IBP Level 5',sort='-y')).properties(width=160,height=100)
    bb3=alt.Chart(df5.filter(pl.col('SALES_DATE')==today.date()).to_pandas()).mark_bar(opacity=.76).encode(x=alt.X('FVA:Q',axis=alt.Axis(format='%')).bin(),y='count()').properties(width=190,height=120)
    #cha=pn.FlexBox((c2+c3+c5+c4+c1+l1+l2+l3).configure_axisY(title='FC & Act').configure_axisX(title='Date').add_params(its).transform_filter(its).properties(height=290,width=470)
    #            ,styles={'margin-top':'50px','margin-left':'100px'})
    cha=pn.FlexBox((c2+c3+c1+l1+l2+l3).configure_axisY(title='FC & Act').configure_axisX(title='Date').add_params(its).transform_filter(its).properties(height=290,width=470)
                ,styles={'margin-top':'50px','margin-left':'100px'})

    #corb = pn.FlexBox(pn.pane.HTML(''' <b>Criteria:</b><br>
    #                            1. Pearson product-moment correlation is calculated for last 12 months actual orders & next 12 months forecast of top SKUs.<br>
    #                            2. Filtered for SKUs with more than .95 correlation in actual orders.<br>
    #                            3. The SKUs shown above are SKUs with highest difference between actual orders & DF forecast correlation '''),
    #                            styles={'border':'dashed','margin':'15px','opacity':'70%'},align=('end','end'),flex_direction='row-reverse', sizing_mode='fixed', width=400,height=150 )

    grb = pn.FlexBox(pn.pane.HTML(''' <b>Criteria:</b><br>
                                1. First table lists brands with high difference in growth of next year compared to this year.<br>
                                2. Second table lists brands with high growth/decline in next year.<br>'''),
                                styles={'border':'dashed','margin-bottom':'-115px','opacity':'70%'},align=('end','end'),flex_direction='row-reverse', sizing_mode='fixed', width=400,height=100 )

    fvb = pn.FlexBox(pn.pane.HTML(''' <b>Criteria:</b><br>
                                1. First table lists high error contributing items sorted by high -ve FVA.<br>
                                2. Second table -ve FVA sorted by error contribution.<br>'''),
                                styles={'border':'dashed','opacity':'70%'},margin=(50,5,3,100),align=('end','end'),flex_direction='row-reverse', sizing_mode='fixed', width=400,height=100 )


    bb = pn.FlexBox(pn.pane.HTML(''' <b>Criteria:</b><br>
                                1. First table lists high error contributing items sorted by high -ve 3M Bias.<br>
                                2. Second table lists high error contributing items sorted by high +ve 3M Bias.<br>'''),
                                styles={'border':'dashed','opacity':'70%'},margin=(100,5,3,100),align=('end','end'),flex_direction='row-reverse', sizing_mode='fixed', width=400,height=100 )

    eb = pn.FlexBox(pn.pane.HTML(''' <b>Criteria:</b><br>
                                1. First table lists SKUs with highest error increase in last month.<br>
                                2. Second table lists SKUs where ratio of error contribution to order contribution is >1 last month sorted by Error contribution in last month.<br>'''),
                                styles={'border':'dashed','opacity':'70%'},margin=(100,5,3,100),align=('end','end'),flex_direction='row-reverse', sizing_mode='fixed', width=400,height=100 )


    he = pn.Row(pn.pane.HTML(f'<h2><span style="color:#FFCC33"><b>Automated Insights </b></span>for {loc} {prod}</h2>'))

    tabs = pn.Tabs(
        ('Growth/Decline', pn.Column(pn.Row(dfc1,dfc2),grb,margin=(5,5,5,5))),
        ('-ve FVA', pn.Column(pn.Row(row,fvt),fvb,margin=(5,5,25,5))),
        ('High Bias',pn.Column(pn.Row(row1,row2),bb,margin=(5,5,25,5))),
        ('High Error',pn.Column(pn.Row(et1,etp),eb,margin=(5,5,25,5))),
        #('Correlation', pn.Column(corr,corb,margin=(5,5,5,5))),
        tabs_location='left',styles={'font-size':'16px','height':'100%'},design=Material)

    cont=pn.Column(he,tabs,cha,styles={'height':'100%'},sizing_mode='stretch_width')

    from panel.io import save
    save.save(cont,f'{loc}-{prod}.html',title='Automated Insights')
    return bb1,bb2,bb3