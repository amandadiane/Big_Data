# Amanda Baker
# scrapy runspider a3-adbaker-itunes-topapps-2.py –t csv –o - > a3-adbaker-itunes-topapps-2.csv


from scrapy import Request
from scrapy.spiders import Spider

class get_popular_apps(Spider):
    name = 'get_popular_apps'
    handle_httpstatus_list = [404, 403]
    custom_settings = { 'DOWNLOAD_DELAY': 0.5 }
    start_urls = [ "https://www.apple.com/itunes/charts/free-apps" ]
    img_url_base = "https://www.apple.com"
    
    def parse(self, response):
        rows = response.xpath("//section[@class='section apps chart-grid']/div[@class='section-content']/ul/li")
        items = []
        for row in rows:
            item = {}
            img_url_path = row.xpath("./a/img/@src")[0].extract()
            item['app_name'] = row.xpath("./h3/a/text()").extract()
            item['category'] = row.xpath("./h4/a/text()").extract()              
            item['appstore_link_url'] = row.xpath("./h3/a/@href")[0].extract()
            item['img_src_url'] = self.img_url_base + img_url_path
            url = item['appstore_link_url']
            req = Request(url, callback=self.parse_2)
            req.meta['data'] = item
            items.append(req)
        return items

    def parse_2(self, response):
        item = response.meta['data']
        item['star_rating'] = response.xpath(".//span[@class='we-customer-ratings__averages__display']/text()").extract()
        item['count_ratings'] = response.xpath(".//div[@class='we-customer-ratings__count small-hide medium-show']/text()").extract()
        return item
    

