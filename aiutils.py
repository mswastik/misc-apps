import polars as pl
from automatedinsights import genai
import win32com.client as win32
import os
import io
import altair as alt
import base64
from datetime import datetime, date

today=datetime.today().replace(day=1)
alt.themes.enable('powerbi')
maf=pl.read_excel("mapping.xlsx")
loch = 'Country'
loc = 'UNITED STATES'
prodh = 'Business Unit'
#prod = ['CMF','Joint Replacement','Trauma and Extremities','Endoscopy']
prod = 'Neurosurgical'

buf1,buf2,buf3 = io.BytesIO(),io.BytesIO(),io.BytesIO()

#for i in prod:
bb1,bb2,bb3=genai(loch,loc,prodh,prod)
bb1.save(buf1,format='png')
bb2.save(buf2,format='png')
bb3.save(buf3,format='png')


ht=f'''<html>
    <body style="font-family: 'Calibri','Arial', Times, serif,'Courier New', Courier, monospace;">
        <div style="display: flex; background-color: black;color:white;height: 40px; text-align: center;"><h1 style="margin: auto;color: #FFCC33;font-family:Arial">Auto Insights for {loc} {prod}</h1></div>
        <table style="margin-left:auto;margin-right:auto; box-shadow: 5px 5px 3px lightgrey;">
        <tr style="border: 1px solid lightgrey; width:99%">
        <td style="padding-left: 15px; width:50%; background-color:rgb(227,225,225); font:1.6em; border: 2px solid grey"">1. SilverGlide & Sonopet USS contributes highest </td>
        <td style="padding-left: 15px; width:50%; border: 2px solid grey"><img src="data:image/png;base64,{base64.b64encode(buf1.getvalue()).decode()} width="50" height="50"</img></td>
        </tr>
        <tr>
        <td style="padding-left: 15px; width:50%"><img src="data:image/png;base64,{base64.b64encode(buf2.getvalue()).decode()} width="10" height="10"</img></td>
        <td style="padding-left: 15px; width:50%; background-color:rgb(221,222,221); font:1.6em">1. SilverGlide & Sonopet USS contributes highest </td>
        </tr>
        <tr>
        <td style="padding-left: 15px; width:50%; background-color:rgb(221,222,221); font:1.6em">1. SilverGlide & Sonopet USS contributes highest </td>
        <td style="padding-left: 15px; width:50%"><img src="data:image/png;base64,{base64.b64encode(buf3.getvalue()).decode()} width="10" height="10"</img></td>
        </tr>
    </table>
    </body>
</html>'''

outlook=win32.Dispatch('outlook.application')
mail=outlook.CreateItem(0)
mail.To='swastik.mishra@stryker.com'
mail.Subject=f'[Auto Insights]-[{loc} {prod}]-{today.strftime('%b-%y')} '
mail.HTMLbody=ht

mail.Attachments.Add(os.getcwd()+f'\\{loc}-{prod}.html')
mail.Send()