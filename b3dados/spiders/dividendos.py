import scrapy


class DividendosSpider(scrapy.Spider):
    name = 'dividendos'
    allowed_domains = ['b3.com.br']
    start_urls = ['http://b3.com.br/']

    def parse(self, response):
        pass
