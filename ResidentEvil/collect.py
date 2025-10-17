# %%

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd

# %%

# Mesmo que possa não precisar, é interessante a utilização de headers
# para caso seja necessária a autenticação ao acessar a web.

headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,ru;q=0.6,ja;q=0.5,pt-PT;q=0.4',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'referer': 'https://www.residentevildatabase.com/personagens/',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Opera GX";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0',
        # 'cookie': '_gid=GA1.2.1248761698.1760674347; _ga=GA1.1.659102705.1760674346; __gads=ID=194608fcc6041c99:T=1760674345:RT=1760679826:S=ALNI_MYOnmFPmh7_Ai3Tkl2lbBATuUiC9A; __gpi=UID=0000129f7126a893:T=1760674345:RT=1760679826:S=ALNI_MZCuqeYTVA2nYZ6mMbc1VMs7s_I1A; __eoi=ID=96b22aaf09ffbaa6:T=1760674345:RT=1760679826:S=AA-AfjYy13d6k6GObCQBT10tFTcW; _ga_DJLCSW50SC=GS2.1.s1760679826$o2$g1$t1760679828$j59$l0$h0; _ga_D6NF5QC4QT=GS2.1.s1760679826$o2$g1$t1760679828$j58$l0$h0; FCNEC=%5B%5B%22AKsRol-gUrNVGGJfrfyYkYN67dw0kW1lP0RdBHRW45-4VpqcEsBTkTmObPtprFFQpU-RMqC828W6PAP8lO0WRVycF3h7KGHu-mWD3nKf2wl2v9Q5nLl78CnIwqqNGxzwVvqRk2lLnuGXJI50fxR3GctrabIIm6ZJOA%3D%3D%22%5D%5D',
    }

def get_content(url: str):
    response = requests.get(url, headers=headers)
    return response

def get_basic_infos(soup):

    # Buscar a informação dentro da estrutura gerada pelo response.tex no BS
    div_page = soup.find("div", class_ = "td-page-content")
    # Acertando qual é o parágrafo onde estão as informações que buscamos
    paragrafo = div_page.find_all("p")[1]
    # Capturando e armazenando a lista contida no parágrafo
    ems = paragrafo.find_all("em")
    # Construindo um dicionário para armazenar as informações
    dict = {}
    for em in ems:
        chave, valor, *_ = em.text.split(":")
        dict[chave.strip(" ")] = valor.strip(" ")
    return dict

# Achando as informações seguidas de "h4" - as aparições dos personagens
def get_aparitions(soup):
    lis = (
        soup
        .find("div", class_ = "td-page-content")
        .find("h4")
        .find_next()
        .find_all("li")
    )

    aparicoes = [i.text for i in lis]
    return aparicoes

# Unindo tudo em uma única função
def get_character_info(url):
    response = get_content(url)
    if response.status_code != 200:
        print("Não foi possível estabelecer a conexão. Código de erro: " + str(response.status_code))
        return {}
    else:
        soup = BeautifulSoup(response.text, features="html.parser")
        data = get_basic_infos(soup)
        data["Aparições"] = get_aparitions(soup)
        return data
    
# Capturar o link que leva a página de informações de cada personagem
def get_characters_links():
    url = "https://www.residentevildatabase.com/personagens/"
    resp = requests.get(url, headers=headers)
    soup_character = BeautifulSoup(resp.text, features="html.parser")

    anchors = (
        soup_character
        .find("div", class_ = "td-page-content")
        .find_all("a")
    )

    anchors_link = [i["href"] for i in anchors]
    return anchors_link

def get_all_informations():
    links = get_characters_links()
    data = []
    for i in tqdm(links):
        d = get_character_info(i)
        d["Nome"] = (
            i.strip("/")
            .split("/")[-1]
            .replace("-", " ")
            .title()
        )
        data.append(d)

    return data

# %%

# Executando e transformando em .parquet
df_final = pd.DataFrame(get_all_informations())
df_final.to_parquet("raw_data_resident_evil.parquet", index=False)