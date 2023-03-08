import pandas as pd
import bs4 as bs
import aiohttp
import asyncio
import time
import numpy as np
import random
df = pd.read_excel('./Fin_Res_Q3_22_23_12_Feb_2023_in_V1.xlsx')  # input line
df = df.rename(columns=df.iloc[0]).drop(df.index[0])
df = df.fillna('0',inplace=True)
urls = df["res_url"].tolist()
outdf = pd.DataFrame(columns=["SYMB", " DEC '22_R", " DEC '22_V", " DEC '22_DEPS",
                              " DEC '22_Interest", " DEC '22_Gross NPA", " DEC '22_Net NPA", "LUU",
                              "RES_CATG_1", "RES_CATG_2", "L_RES_DT", "DATA_AVAILABLE",
                              "Last Available Data", "TIME STAMP", "RES_URL", "SECTOR"])

def delay():
    time.sleep(random.uniform(0, 1))


async def get_data(session, url):
    delay()
    async with session.get(url) as resp:
        soup = bs.BeautifulSoup(await resp.text(), 'html.parser')
        table = soup.find('table', {'class': 'mctable1'})
        data = []
        if table is None:
            return data
        for row in table.findAll('tr'):
            cols = row.findAll('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
        return data


async def fill_df(urls):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            task = asyncio.create_task(get_data(session, url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)

        for index, data in enumerate(results):
            url = urls[index]
            if len(data) == 0:
                outdf.loc[index, "RES_URL"] = df.loc[index+1, "res_url"]
                outdf.loc[index, "DATA_AVAILABLE"] = "N"
                outdf.loc[index, "TIME STAMP"] = time.strftime(
                    "%d/%m/%Y %H:%M:%S")
                outdf.loc[index, "SECTOR"] = df.loc[index+1, "sectr"]
                outdf.loc[index, 'SYMB'] = df.loc[index+1, "symb"]
                outdf.loc[index, "LUU"] = df.loc[index+1, "LUU"]
                outdf.loc[index, "RES_CATG_1"] = df.loc[index+1, "res_catg1"]
                outdf.loc[index, "RES_CATG_2"] = df.loc[index+1, "res_catg2"]
                outdf.loc[index, "L_RES_DT"] = df.loc[index+1, "l_res_dt"]
                outdf.loc[index, "Last Available Data"] = "NONE"
            elif len(data) > 0:
                dg = pd.DataFrame(data, index=None, columns=None)
                dg.columns = dg.iloc[0]
                dg = dg.drop(dg.index[0])
                dg = dg.iloc[:, :2]
                if dg.columns[1] == "Dec '22":
                    outdf.loc[urls.index(
                        url), 'SYMB'] = df.loc[urls.index(url) + 1, "symb"]
                    outdf.loc[urls.index(
                        url), "LUU"] = df.loc[urls.index(url) + 1, "LUU"]
                    if outdf.loc[index, "LUU"] == "0":
                        outdf.loc[index, "LUU"] = "AUTO"
                    outdf.loc[urls.index(url), "RES_CATG_1"] = df.loc[urls.index(
                        url) + 1, "res_catg1"]
                    outdf.loc[urls.index(url), "RES_CATG_2"] = df.loc[urls.index(
                        url) + 1, "res_catg2"]
                    outdf.loc[urls.index(url), "L_RES_DT"] = df.loc[urls.index(
                        url) + 1, "l_res_dt"]
                    outdf.loc[urls.index(url), "RES_URL"] = df.loc[urls.index(
                        url) + 1, "res_url"]
                    outdf.loc[urls.index(url), "SECTOR"] = df.loc[urls.index(
                        url) + 1, "sectr"]
                    outdf.loc[urls.index(url), " DEC '22_R"] = dg.loc[1][1]
                    outdf.loc[urls.index(url), " DEC '22_V"] = dg.loc[28][1]
                    outdf.loc[urls.index(url), " DEC '22_DEPS"] = dg.loc[37][1]
                    outdf.loc[urls.index(
                        url), " DEC '22_Interest"] = dg.loc[20][1]
                    outdf.loc[urls.index(url), "DATA_AVAILABLE"] = "Y"
                    outdf.loc[urls.index(
                        url), "Last Available Data"] = dg.columns[1]
                    outdf.loc[urls.index(url), "TIME STAMP"] = time.strftime(
                        "%d/%m/%Y %H:%M:%S")
                    if outdf.loc[urls.index(url), "SECTOR"] == "Banks":
                        outdf.loc[urls.index(url), " DEC '22_R"] = dg.loc[2][1]
                        outdf.loc[urls.index(
                            url), " DEC '22_V"] = dg.loc[20][1]
                        outdf.loc[urls.index(
                            url), " DEC '22_DEPS"] = dg.loc[30][1]
                        outdf.loc[urls.index(url), " DEC '22_Interest"] = "0"
                        outdf.loc[urls.index(
                            url), " DEC '22_Gross NPA"] = dg.loc[35][1]
                        outdf.loc[urls.index(
                            url), " DEC '22_Net NPA"] = dg.loc[36][1]
                else:
                    outdf.loc[urls.index(url), "RES_URL"] = df.loc[urls.index(
                        url) + 1, "res_url"]
                    outdf.loc[urls.index(url), "DATA_AVAILABLE"] = "N"
                    outdf.loc[urls.index(url), "TIME STAMP"] = time.strftime(
                        "%d/%m/%Y %H:%M:%S")
                    outdf.loc[urls.index(url), "SECTOR"] = df.loc[urls.index(
                        url) + 1, "sectr"]
                    outdf.loc[urls.index(
                        url), 'SYMB'] = df.loc[urls.index(url) + 1, "symb"]
                    outdf.loc[urls.index(
                        url), "LUU"] = df.loc[urls.index(url) + 1, "LUU"]

                    outdf.loc[urls.index(url), "RES_CATG_1"] = df.loc[urls.index(
                        url) + 1, "res_catg1"]
                    outdf.loc[urls.index(url), "RES_CATG_2"] = df.loc[urls.index(
                        url) + 1, "res_catg2"]
                    outdf.loc[urls.index(url), "L_RES_DT"] = df.loc[urls.index(
                        url) + 1, "l_res_dt"]
                    outdf.loc[urls.index(
                        url), "Last Available Data"] = dg.columns[1]
        return outdf


async def main():
    await fill_df(urls)
    return outdf

if __name__ == "__main__":
    start = time.perf_counter()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(main())
    loop.run_until_complete(future)
    print(f"Finished in {time.perf_counter() - start} second(s)")
    current_time = time.strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"output_{current_time}.xlsx"
    outdf = outdf.fillna("0", inplace=True)
    outdf.to_excel(filename, index=False)
