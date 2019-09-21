# -*- coding: utf-8 -*-
import fnmatch
import os
import re
import scrapy

from dateutil import parser
from dateutil import tz

def _mkdirs(path):
    if os.path.exists(path):
        return

    d_name = os.path.dirname(path)
    _mkdirs(d_name)

    os.mkdir(path)

class M_NewsmthSpider(scrapy.Spider):
    name = 'm_newsmth'
    data_home = os.path.expanduser('~/smth/data')

    custom_settings = {
        'COLLECTION_NAME' : 'smth'
    }

    def _spide_local(self):
        urls = []

        for path, directories, files in os.walk(self.data_home):
            for file in files:
                #if fnmatch.fnmatch(file, '.m.*.html'):
                if fnmatch.fnmatch(file, '.m.AutoWorld.1942810068.4.html'):
                    urls.append('file://%s/%s' % (path, file))

        for url in urls:
            yield scrapy.FormRequest(url=url, callback=self.parse_article)

    def _spide_artile(self, board, aid):
        yield scrapy.FormRequest(
            url="http://m.newsmth.net/article/%s/%s" % (board, aid),
            callback=self.parse_article,
            meta={'board': board, 'last_page_chksum': 0}
        )

    def _spide_board(self, board):
        url = "http://m.newsmth.net/board/%s?p=1" % board

        yield scrapy.FormRequest(
            url=url, callback=self.parse_board,
            meta={'board': board, 'last_page_chksum': 0}
        )

    def start_requests(self):
        boards = [ "Movie", "FamilyLife", "MilitaryView", "Divorce", "Love", "Occupier", "AutoWorld", "ITExpress", "History", "GreenAuto", "Tooooold", "ChildEducation", "OurEstate", "PocketLife", "BasketballForum", "RealEstate", "WorkLife", "Single", "PieLove", "Geography", "Food", "SoftEng", "Bull", "OldSongs", "Mentality", "Joke"]

        for board in boards:
            _mkdirs("%s/%s" % (self.data_home, board))

            yield from self._spide_board(board)

    def _parse_article_response(self, response):
        board = response.meta.get('board')
        title = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[@class='f']/text()").get()

        posts = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li")


        for i in range(1, len(posts)+1):
            a_content = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[%s]/div[@class='sp']" % i).get()

            a_id = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[%s]/div/div[1]/a[2]/text()" % i).get()

            if not a_id:
                # it's title
                continue

            # ip
            a_ips  = re.match(".*<br>FROM ([0-9.]+).*", a_content)
            a_ip = '127.0.0.1'

            if a_ips :
                a_ip = a_ips.group(1)

                if a_ip.endswith('.'):
                    a_ip = '%s1' % (a_ip)

            # date
            a_date  = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[%s]/div[@class='nav hl']/div[1]/a[@class='plant'][2]/text()" % i).get()

            '''
            a_date = a_date.replace("\u00A0"," ")
            '''

            doc = {
                'id': a_id,
                'ip': a_ip,
                'date': parser.parse('%s CST' % a_date, tzinfos={'CST': tz.gettz('Asia/Shanghai')}),
                'title': title,
                'content': a_content,
                'board': board
                }

            yield doc

    def parse_article(self, response):
        last_page_chksum = response.meta.get('last_page_chksum')
        board = response.meta.get('board')
        aid = response.url.split('/')[-1].split("?")[0]

        ids = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li")
        page_chksum = ''

        for i in range(1, len(ids)+1):
            id = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[%s]/div[@class='nav hl']/div[1]/a[2]/text()" % i).get()
            page_chksum = '%s_%s' % (page_chksum, id)

        if last_page_chksum == page_chksum:
            return

        if "?p="  in response.url:
            page = response.url.split('=')[1]
        else:
            page = 1

        f_name = "%s/%s/.m.%s.%s.%s.html" % (self.data_home, board, board, aid, page)

        if not response.url.startswith('file') and not os.path.exists(f_name):
            with open(f_name, "wb") as f:
                f.write(response.body)

        yield from self._parse_article_response(response)

        if page == "1" or response.url.startswith('file'):
            return

        next_page = "http://m.newsmth.net/article/%s/%s?p=%s" % (board, aid, int(page)-1)
        yield response.follow(next_page, self.parse_article, meta={'board': board, 'last_page_chksum': page_chksum})

    def parse_board(self, response):
        last_page_chksum = response.meta.get('last_page_chksum')

        board = response.meta.get('board')

        if "?p="  in response.url:
            page = response.url.split('=')[1]
        else:
            page = 1

        f_name = "%s/%s/.m.%s.%s.txt" % (self.data_home, board, board, page)

        ids = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li")

        page_chksum = ''

        for i in range(1, len(ids)+1):
            id = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[%s]/div[2]/a[1]/text()" % i).get()
            page_chksum = '%s_%s' % (page_chksum, id)

        if last_page_chksum == page_chksum:
            return

        with open(f_name, "wb") as f:
            f.write(response.body)

        articles = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li")

        for i in range(1, len(articles)+1):
            # first page
            article_href = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[%s]/div[1]/a/@href" % i ).get()
            pages = response.xpath("/html/body/div[@id='wraper']/div[@id='m_main']/ul[@class='list sec']/li[%s]/div[1]/text()" % i ).get()
            r = re.match('\(([0-9]+)\)', pages)

            max_page = 1

            if r:
                max_page = (int(r.group(1))+9)//10

            yield response.follow("%s?p=%s" % (article_href, max_page), self.parse_article, meta={'board': board})

        next_page = "http://m.newsmth.net/board/%s?p=%s" % (board, int(page)+1)
        yield response.follow(next_page, self.parse_board, meta={'board': board, 'last_page_chksum': page_chksum})
