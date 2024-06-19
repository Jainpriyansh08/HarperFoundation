import requests
from bs4 import BeautifulSoup
import json

url = "https://give.do/discover/city/Bangalore/"
divs_list = []
data = []
for i in range(1, 77):
    r = requests.get(f'{url}{i}')
    soup = BeautifulSoup(r.text, 'html.parser')
    target_divs = soup.find_all('div', class_="section-results container-main")
    print(len(target_divs))
    divs_list.extend(target_divs)


def get_data(x):
    mp = {}
    mp["title"] = x.find('a', class_='vendorTile__title').text.strip() if x.find('a',
                                                                                 class_='vendorTile__title') else None
    mp["rating"] = x.find('div', class_='vendorTile__contentRating').text.strip() if x.find('div',
                                                                                            class_='vendorTile__contentRating') else None
    mp["location"] = x.find('span', class_='vendorTile__location').text.strip() if x.find('span',
                                                                                          class_='vendorTile__location') else None
    mp["description"] = x.find('p', class_='vendorTile__description').text.strip() if x.find('p',
                                                                                             class_='vendorTile__description') else None
    mp["price"] = x.find('div', class_='vendorTileFooter__info').text.strip() if x.find('div',
                                                                                        class_='vendorTileFooter__info') else None
    if x.find_all('div', class_='vendorTileFooter__info'):
        if len(x.find_all('div', class_='vendorTileFooter__info')) > 1:
            mp["guests"] = x.find_all('div', class_='vendorTileFooter__info')[1].text.strip()
        else:
            mp["guests"] = None
    else:
        mp["guests"] = None
    return mp


for div in divs_list:
    data.append(get_data(div))

print(len(data))
print(data[0])

with open("scrapped-data.json", "a+") as fp:
    json.dump(data, fp)
