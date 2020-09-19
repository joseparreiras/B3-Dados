import scrapy
from scrapy.http import Request
from scrapy.utils.response import open_in_browser
import pandas as pd


class DadosSpider(scrapy.Spider):
    name = 'dados'
    allowed_domains = ['bvmf.bmfbovespa.com.br']
    start_urls = [
        'http://bvmf.bmfbovespa.com.br/']

    cod_list = [35, 60, 16705, 906]
    seq_years = range(2002, 2021)
    tabela = pd.read_csv('SPW_CIA_ABERTA.csv')
    cod_list = tabela.CD_CVM

    def start_requests(self, cod_list=cod_list, seq_years=seq_years):
        # Set the headers here. The important part is "application/json"
        with open("headers.json", "r") as read_headers:
            headers = json.load(read_headers)

        for cod_cvm in cod_list:
            for year in seq_years:
                url = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoRelevantes.asp?codCVM=%s&ano=%i' % (
                    cod_cvm, year)
                yield Request(url, headers=headers, callback=self.parse_year,
                              meta={'cod_cvm': cod_cvm})

    def parse_year(self, response):
        try:
            cod_cvm = response.meta.get('cod_cvm')
            import pandas as pd
            table_list = response.xpath('//*[@class="large-12 columns"]')

            for table in table_list[2:]:
                table_data = table.xpath('.//table').extract_first()
                table_data = pd.read_html(table_data, index_col=0)[0].T.iloc[0]
                file_link = table.xpath('.//*[@class="primary-text"]')[0]
                file_link = file_link.xpath('.//@href').extract_first()
                table_data['Link'] = file_link
                table_data['codCVM'] = cod_cvm
                from os import getcwd
                yield table_data.to_dict()
        except TypeError:
            pass
