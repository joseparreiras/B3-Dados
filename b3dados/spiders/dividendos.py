import scrapy
import pandas as pd
from scrapy.http import Request
import json
from scrapy.utils.response import open_in_browser


class DividendosSpider(scrapy.Spider):
    name = 'dividendos'
    allowed_domains = ['b3.com.br']
    start_urls = ['http://b3.com.br/']

    def start_requests(self):
        # Set the headers here. The important part is "application/json"
        with open("headers.json", "r") as read_headers:
            headers = json.load(read_headers)

        tabela = pd.read_csv('SPW_CIA_ABERTA.csv')
        cod_list = tabela.CD_CVM
        # cod_list = [24783]

        for cod_cvm in cod_list:
            url = 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoProventosDinheiro.aspx?codigoCvm=%i&tab=3.1&idioma=pt-br' % cod_cvm

            yield Request(url, headers=headers, callback=self.parse_div, meta={'cod_cvm': cod_cvm})

            yield Request(url, headers=headers, callback=open_in_browser, meta={'cod_cvm': cod_cvm})

    def parse_div(self, response):
        cod_cvm = response.meta.get('cod_cvm')

        # Check if there is any data to collect
        alert_text = response.xpath(
            '//div[@id="tabelaProventos"]/div/span/text()').extract_first()
        if alert_text == 'Não há dados de Proventos em Dinheiro disponíveis para essa empresa.':
            pass
        else:
            table_html = response.xpath('//table').extract_first()
            table = pd.read_html(table_html)[0]
            table['codCVM'] = cod_cvm
            yield table.to_dict()
