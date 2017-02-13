from scrapy.spider import Spider
from scrapy.http.request import Request
from joke.items import JianDanItem


class JianDanSpider(Spider):
    name = "jiandan"
    domain = "http://jandan.net/"
    url_tmeplate = "http://jandan.net/duan/page-{0}"

    def start_requests(self):
        page = 1
        yield Request(url=self.url_tmeplate.format(page),
                      meta={"page": page},
                      callback=self.parse)

    def parse(self, response):
        item = JianDanItem()
        try:
            for piece in response.xpath("//ol[@class='commentlist']//li"):
                item["author"] = piece.xpath(".//div[@class='author']/strong//text()").extract()[0]
                content = "".join(piece.xpath(".//div[@class='text']//p[1]//text()").extract())
                item["content"] = content
                item["popular_value"] = piece.xpath(".//div[@class='vote']//span[2]/text()").extract()[0]
                item["joke_id"] = piece.xpath("./@id").extract()[0].split("-")[-1]
                yield item
        except Exception as e:
            self.logger.exception({"the page is {0} and the exception is {1}".format(response.meta["page"], e)})
            self.logger.error(response.url)

        # next page
        if not response.xpath("//a[@class='next-comment-page']"):
            return
        page = response.meta["page"] + 1
        url = self.url_tmeplate.format(page)
        yield Request(url=url, callback=self.parse, meta={"page": page})


if __name__ == '__main__':
    # test
    from scrapy.crawler import CrawlerProcess
    # 初始化一个下载中间件的配置
    process = CrawlerProcess(settings={"User-Agent": "Mozilla/5.0",
                                       "LOG_LEVEL": "INFO",
                                       "MONGO_URI": "localhost:27017",
                                       "MONGO_DATABASE": "joke",
                                       "ITEM_PIPELINES": {"joke.pipelines.MongoPipeline": "300",
                                                          "joke.pipelines.ElasticsearchPipeline": "400"}})
    process.crawl(JianDanSpider)
    process.start()
