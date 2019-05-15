# Amanda Baker
# scrapy runspider a3-adbaker-itunes-topapps-1.py –t csv –o - > a3-adbaker-itunes-topapps-1.csv


from scrapy import Request
from scrapy.spiders import Spider

class get_popular_apps(Spider):
    name = 'get_popular_apps'
    start_urls = [ "https://www.apple.com/itunes/charts/free-apps" ]
    
    def parse(self, response):
        rows = response.xpath("//section[@class='section apps chart-grid']/div[@class='section-content']/ul/li")
        img_url_base = "https://www.apple.com"
        items = []
        for row in rows:
            item = {}
            item['app_name'] = row.xpath("./h3/a/text()").extract()
            item['category'] = row.xpath("./h4/a/text()").extract()              
            item['appstore_link_url'] = row.xpath("./h3/a/@href").extract()
            img_url_path = str(row.xpath("./a/img/@src").extract())
            item['img_src_url'] = img_url_base + img_url_path[2:-2]
            items.append(item)
        return items
