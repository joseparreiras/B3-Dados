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
        headers = {
            'Accept': 'application/json,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
            # 'Cache-Control': 'max-age=0',
            # 'Connection': 'keep-alive',
            # 'Cookie': 'idioma=pt-br; rxVisitor=1600452545914DQPF4IR8RS84BJU6JUBIGV0T29LR4MOS; _ga=GA1.4.914086125.1600452546; _gid=GA1.4.128440118.1600452546; ASPSESSIONIDCQDSDTQC=ICLDNKHCHHMIMLMBLGFKJMFM; TS01e3f871=011d592ce1e1b4e23674e48af1c753fa085dc857546f829977fe9a022093fff409cf487689af5f5212ae54f1a1669a94e992fac644583ee74e07f53ab8ff43d49c9a067f3877af1c0b1f6e65ac8f69144056037b667bc0db8f2b216c789d43649f1430ffdf; ASPSESSIONIDQACRSSQD=IIFCNKHCANFNGAGKEMMFJLBG; ASPSESSIONIDAABTDCST=KMLDNKHCAGNJIBPDGJDCIEFI; ASPSESSIONIDCQBTBRRC=BPCLNBICKNAKKFKIBAJFHOAK; ASPSESSIONIDQABSCBQQ=MKCJNBICHLJKICJCEOBHAIBL; dtSa=-; TabSelecionada=2; ASPSESSIONIDQCDTTQQC=FGEJNBICMJEDKOKJEONEEKKJ; TS01871345=016e3b076f00b9b7e6a0776bc0e9f012dcfde0d68374dc88bded20df8141db2844ab6d0b9f06d0d7cd0d1050b928e5138e4af0d65386c1381f9ed4a1d61283c833cdec4951cf099b186e285591cba6fd4121cc1ec022b98f783c75d11903ec5c843bad00445cc413d11f1e4f3e295db54f7a7fa70c7ff0afdbf132ad9e514cda0f56f7fcfe7f481adfd8fef42953f78f5bfdc460f677edf365e81bc168cac1d18c21752faac242edc221b8ed2a2409026de5b6d5c3; _dc_gtm_UA-43178799-13=1; rxvt=1600457581383|1600452545920; dtPC=16$255780997_957h-vVLNRIULNCQUOQJHIFAKMVNHTDHHJQBAM-0e65; dtLatC=2; dtCookie=v_4_srv_16_sn_9A6DD4035C9430AEDAA29E14DFA878AC_perc_100000_ol_0_mul_1_app-3A5286dfffe4e737f8_1_app-3Ad94293f33bf4a8d1_1',
            # 'Host': 'bvmf.bmfbovespa.com.br',
            # 'Upgrade-Insecure-Requests': 1,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'
        }

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
