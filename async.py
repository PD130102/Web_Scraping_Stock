import asyncio
import aiohttp
import pandas as pd
import time
import bs4

df = pd.read_excel('./Fin_Res_Q3_22_23_12_Feb_2023_in_V1.xlsx')
df = df.rename(columns=df.iloc[0]).drop(df.index[0])
urls = df["res_url"].tolist()

outdf = pd.DataFrame(columns=["SYMB", " DEC '22_R", " DEC '22_V", " DEC '22_DEPS", " DEC '22_Interest", " DEC '22_Gross NPA", " DEC '22_Net NPA", "LUU","RES_CATG_1", "RES_CATG_2", "L_RES_DT", "DATA_AVAILABLE", "Last Available Data", "TIME STAMP", "RES_URL", "SECTOR"])


def fill_df(url, data):
        if len(data) == 0:
                outdf.loc[urls.index(url), "RES_URL"] = df.loc[urls.index(url)+1, "res_url"]
                outdf.loc[urls.index(url), "DATA_AVAILABLE"] = "N"
                outdf.loc[urls.index(url), "TIME STAMP"] = time.strftime("%d/%m/%Y %H:%M:%S")
                outdf.loc[urls.index(url), "SECTOR"] = df.loc[urls.index(url)+1, "sectr"]
                outdf.loc[urls.index(url), 'SYMB'] = df.loc[urls.index(url)+1, "symb"]
                outdf.loc[urls.index(url), "LUU"] = df.loc[urls.index(url)+1, "LUU"]
                outdf.loc[urls.index(url), "RES_CATG_1"] = df.loc[urls.index(url)+1, "res_catg1"]
                outdf.loc[urls.index(url), "RES_CATG_2"] = df.loc[urls.index(url)+1, "res_catg2"]
                outdf.loc[urls.index(url), "L_RES_DT"] = df.loc[urls.index(url)+1, "l_res_dt"]
                outdf.loc[urls.index(url), "Last Available Data"] = "NONE"
        elif len(data) > 0:
                dg = pd.DataFrame(data, index=None,columns=None)
                dg.columns = dg.iloc[0]
                dg = dg.drop(dg.index[0])
                dg = dg.iloc[:, :2]
                outdf.loc[urls.index(url), 'SYMB'] = df.loc[urls.index(url)+1, "symb"]
                outdf.loc[urls.index(url), "LUU"] = df.loc[urls.index(url)+1, "LUU"]
                outdf.loc[urls.index(url), "RES_CATG_1"] = df.loc[urls.index(url)+1, "res_catg1"]
                outdf.loc[urls.index(url), "RES_CATG_2"] = df.loc[urls.index(url)+1, "res_catg2"]
                outdf.loc[urls.index(url), "L_RES_DT"] = df.loc[urls.index(url)+1, "l_res_dt"]
                outdf.loc[urls.index(url), "RES_URL"] = df.loc[urls.index(url)+1, "res_url"]
                outdf.loc[urls.index(url), "SECTOR"] = df.loc[urls.index(url)+1, "sectr"]
                outdf.loc[urls.index(url), " DEC '22_R"] = dg.loc[1][1]
                outdf.loc[urls.index(url), " DEC '22_V"] = dg.loc[28][1]
                outdf.loc[urls.index(url), " DEC '22_DEPS"] = dg.loc[37][1]
                outdf.loc[urls.index(url), " DEC '22_Interest"] = dg.loc[20][1]
                outdf.loc[urls.index(url), "DATA_AVAILABLE"] = "Y"
                outdf.loc[urls.index(url), "Last Available Data"] = dg.columns[1]
                outdf.loc[urls.index(url), "TIME STAMP"] = time.strftime("%d/%m/%Y %H:%M:%S")
        if outdf.loc[urls.index(url), "SECTOR"] == "Banks":
                outdf.loc[urls.index(url), " DEC '22_R"] = dg.loc[2][1]
                outdf.loc[urls.index(url), " DEC '22_V"] = dg.loc[20][1]
                outdf.loc[urls.index(url), " DEC '22_DEPS"] = dg.loc[30][1]
                outdf.loc[urls.index(url), " DEC '22_Interest"] = "0"
                outdf.loc[urls.index(url), " DEC '22_Gross NPA"] = dg.loc[35][1]
                outdf.loc[urls.index(url), " DEC '22_Net NPA"] = dg.loc[36][1]
results = []

def get_tasks(session):
        tasks = []
        for url in urls:
                tasks.append(asyncio.create_task(session.get(url,ssl=False)))
        return tasks


async def get_data():
        async with aiohttp.ClientSession(connector= aiohttp.TCPConnector(limit=50,force_close=True)) as session:
                tasks = get_tasks(session)
                responses = await asyncio.gather(*tasks)
                for response in responses:
                        print(response.status)
                        results.append(await response.text())

asyncio.run(get_data())

print(len(results))

for i in range(len(results)):
        soup = bs4.BeautifulSoup(results[i], 'html.parser')
        print(urls[i])
        data = []
        for tr in soup.find_all('tr'):
                data.append([td.text.strip() for td in tr.find_all('td')])
        fill_df(urls[i], data)

outdf.to_excel("script_output.xlsx", index=False)