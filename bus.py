import asyncio
import aiohttp
from xml.etree import ElementTree
import pandas as pd
import time


start = time.time()


async def city_list():
    async with aiohttp.ClientSession() as session:
        async with session.get('https://pti.org.il/calculate.asmx/GetCities') as resp:
            cities = await resp.text()
            cities = ElementTree.XML(cities)
            columns_cities = [0] * 3
            for i in range(3):
                columns_cities[i] = cities[0][i].tag.replace('{trans-reform-calculate}', '')
            table = []
            for city in cities:
                row = [0] * 3
                for i in range(3):
                    row[i] = city[i].text
                table.append(row)
            cities = pd.DataFrame(table, columns=columns_cities)
    return cities


async def ticket_price_columns(session, url, params):
    async with session.get(url=url, params=params) as resp:
        ticket = await resp.text()
        ticket = ElementTree.XML(ticket)
        columns_tickets = [0] * 7
        for i in range(5):
            columns_tickets[i] = ticket[i].tag
        columns_tickets[5] = 'area1'
        columns_tickets[6] = 'area2'
        return columns_tickets


async def ticket_price(session, url, params):
    async with session.get(url=url, params=params) as resp:
        row = [0] * 7
        try:
            price = await resp.text()
            price = ElementTree.XML(price)
            for k in range(5):
                row[k] = price[k].text
        except:
            pass
        row[5] = params.get('a1')
        row[6] = params.get('a2')
        return row


async def prices(area_list):
    async with aiohttp.ClientSession() as session:
        tasks = []
        url = "https://pti.org.il/Calculate.asmx/GetCoverage"
        for i in range(len(area_list)):
            for j in range(i, len(area_list)):
                params = {'a1': area_list[i], 'a2': area_list[j], 'a3': ' '}
                tasks.append(asyncio.ensure_future(ticket_price(session, url, params)))
        table = await asyncio.gather(*tasks)
        columns = await ticket_price_columns(session, url, params)
        return pd.DataFrame(table, columns=columns)


async def main():
    cities = await city_list()
    table = await prices(list(cities['AreaID'].unique()))
    table.to_csv('bus.csv')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    print("--- %s seconds ---" % (time.time() - start))
