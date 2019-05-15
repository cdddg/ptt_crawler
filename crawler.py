# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# __version__ = '1.2'

import os
import re
import json
import requests
import datetime
import codecs
import sys
from bs4 import BeautifulSoup
from six import u
from multiprocessing import Pool
import pandas as pd
from itertools import takewhile
from time import sleep
import logging

NOT_EXIST = BeautifulSoup('<a>本文已被刪除</a>', 'lxml').a
MONTH = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12',
}


class Ptt(object):

    def __init__(self):

        self.log = logging.getLogger('ptt')
        self.log.setLevel(10)
        formatter = logging.Formatter(
            fmt='%(asctime)s, %(levelname)s, %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        file_handler = logging.FileHandler(
            filename='ptt.log',
            mode='a'
        )
        file_handler.setFormatter(formatter)  # 可以通過setFormatter指定輸出格式
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter  # 也可以直接給formatter賦值
        self.log.addHandler(file_handler)
        self.log.addHandler(console_handler)

        self.info = dict()
        self.articles = list()
        self.log.info('start, {}'.format(os.path.basename(__file__)))
        self.rs = requests.session()
        self.rs.post('https://www.ptt.cc/ask/over18', verify=True, data={'from': '', 'yes': 'yes'})

    def search_all_boards(self, home_url):
        name, url = self.board_master(home_url)
        with Pool(processes=8) as pool:
            # 返回一個list的值 每個值為function返回的值
            # [board_branch.value, board_branch.value, ....]
            resp = pool.map(self.board_branch, url)
        info = dict()
        for v in resp:
            info.update(v)
        info = dict((sorted(info.items(), key=lambda d: d[0])))
        info = list(zip(*info.items()))
        df = pd.DataFrame({"board": info[0], "url": info[1]})
        df.to_csv(os.path.join(os.getcwd(), "PTTurl.csv"))

    @staticmethod
    def board_master(home_url):
        sleep(0.5)
        payload = {'from': '', 'yes': 'yes'}
        headers = {
            'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:22.0) Gecko/20100101 Firefox/22.0'
        }
        rs = requests.session()
        rs.post('https://www.ptt.cc/ask/over18', verify=True, data=payload)
        response = rs.get(home_url, headers=headers)
        rs.close()
        soup = BeautifulSoup(response.text, 'lxml')
        text = soup.find_all('div', 'b-ent')
        name, url = [], []
        for i, v in enumerate(text):
            v = v.find('a')
            try:
                name.append(v.find('div', 'board-name').getText())
            except IndexError:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                name.append('{}, {}'.format(exc_type, exc_obj))
            try:
                url.append("https://www.ptt.cc" + v.get('href'))
            except IndexError:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                url.append('{}, {}'.format(exc_type, exc_obj))
        return name, url

    def board_branch(self, url):
        payload = {'from': '', 'yes': 'yes'}
        headers = {
            'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:22.0) Gecko/20100101 Firefox/22.0'
        }
        sleep(0.3)
        rs = requests.session()
        rs.post('https://www.ptt.cc/ask/over18', verify=True, data=payload)
        sleep(0.3)
        response = rs.get(url, headers=headers)
        rs.close()
        soup = BeautifulSoup(response.text, 'lxml')
        text = soup.find_all('div', 'b-ent')
        if (text == []) is True:
            try:
                board = soup.find('a', 'board')
                if board is not None:  # 如果不是空白討論區的話
                    name = board.getText().split()  # 看板 XXXXXXX
                    name = name[1] if len(name) == 2 else board.getText()
                    link = "https://www.ptt.cc{}".format(board.get('href'))
                    self.info[name] = link
                    print('{}, {}'.format(name, link))
                    return self.info
            except Exception:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                print(f'{exc_type}, L{exc_tb.tb_lineno}, {exc_obj}')
        else:
            for v in text:
                v = v.find('a')
                name = None if v.find('div', 'board-name') is None \
                    else v.find('div', 'board-name').getText()
                link = "https://www.ptt.cc{}".format(v.get('href'))
                if name != '0ClassRoot':
                    if name is None or link is None:
                        print(str(v))
                    else:
                        self.board_branch(link)
            return self.info

    def crawler_articles(
            self, board=None, page=None, date=None, push=None, author=None, title=None):

        # 判斷是否有搜尋條件
        search_condition = ""
        if push is not None:
            search_condition += 'recommend%3A{}+'.format(str(push))
        if author is not None:
            search_condition += 'author%3A{}+'.format(str(author))
        if title is not None:
            search_condition += '{}+'.format(str(title))

        # 判斷最末段的網頁
        _part = f'-{date[0]}_{date[1]}' if page is None else f'-p{page[0]}_p{page[1]}'
        _title = '' if title is None else '-' + str(title)
        _author = '' if author is None else '-' + str(author)
        _push = '' if push is None else '-' + str(push)
        filename = f'{board}{_part}{_title}{_author}{_push}.json'
        print(filename)
        url = ""
        page_index = 0
        page_qty_limit = 1
        while 1:
            page_index += 1
            # 第一次迴圈執行時，url 預設值為 ""，所以先進行一次搜尋日期或搜尋條件判斷，以便找尋下一個 url
            if url == "":
                self.store(filename, u'{"articles":[', 'w+')
                # 頁數條件
                if page is not None and date is None:
                    page_qty_limit = 1 if abs(page[0] - page[1]) == 0 \
                        else abs(page[0] - page[1]) + 1

                    if search_condition == "":
                        url = f'https://www.ptt.cc/bbs/{board}/index{str(page[1])}.html'
                    else:
                        url = f'https://www.ptt.cc/bbs/{board}/' \
                              f'search?page={str(page[0])}&q={search_condition}'

                # 日期條件
                elif page is None and date is not None:
                    if search_condition == "":
                        url = f'https://www.ptt.cc/bbs/{board}/index.html'
                        url = self.button_link(url=url, state='‹ 上頁')
                        url = self.button_link(url=url, state='下頁 ›')
                        url = self.specification_date(url=url, state='all', date_stop=date[1])
                    else:
                        url = f'https://www.ptt.cc/bbs/{board}/' \
                              f'search?page=1&q={search_condition}'
                        url = self.specification_date(url=url, state='search', date_stop=date[1])
                else:
                    print('parameter error!')
                    return

            # url 已更新利用 if 判斷 是否應該到達末頁，或是還有下一個 url
            else:
                # 判斷頁數是否已到達
                if page is not None and page_index > page_qty_limit:
                    self.log.info(f'頁數已到達, 最後頁數 {page_index}, 規定之頁數 {page_qty_limit}')
                    f = open(filename).read()[:-1] + ']}'
                    self.store(filename, f, 'w+')
                    break
                # 頁數未到達 依條件設定下一個網址
                if search_condition == "":
                    index = int(os.path.split(url)[1].replace('index', '').replace('.html', ''))
                    url = f'https://www.ptt.cc/bbs/{board}/index{str(index-1)}.html'
                else:
                    index = int(os.path.split(url)[1].split('&')[0].split(u'=')[1])
                    url = f'https://www.ptt.cc/bbs/{board}/' \
                          f'search?page={str(index+1)}&q={search_condition}'

            # print(f'\nparse articles : {url}')
            self.log.info('---')
            self.log.info(f'parse articles: {url}')
            resp = requests.get(url=url, cookies={'over18': '1'}, verify=True)
            if resp.status_code != 200:
                # print('invalid url:', resp.url)
                self.log.debug(f'invalid url: {resp.url}')
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')

            if soup.find('div', 'r-list-sep') is not None:
                divs = soup.find('div', class_='r-list-container action-bar-margin bbs-screen')
                divs = divs.find_all('div', {'class': ['r-ent', 'r-list-sep']})
                divs = list(takewhile(lambda x: 'r-list-sep' not in x['class'], divs))
            else:
                divs = soup.find_all("div", "r-ent")

            for i in range(len(divs)):
                div = divs[-i - 1] if search_condition == "" else divs[i]
                article_title = div.find('div', 'title').getText().strip()
                try:
                    # ex. link would be
                    # <a href="/bbs/PublicServan/M.1127742013.A.240.html">Re: [問題] 職等</a>
                    href = div.find('a')['href']
                    link = f'https://www.ptt.cc{href}'
                    article_id = re.sub('\.html', '', href.split('/')[-1])
                    text = self.parse(link=link, articleid=article_id, board=board, limitdate=date)
                    if text == "next":
                        pass
                    elif text is None:
                        f = open(filename).read()[:-1] + ']}'
                        self.store(filename, f, 'w+')
                        self.log.info('return')
                        return
                    else:
                        self.store(filename, f'{text},', 'a')
                except Exception:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    self.log.error(f'{exc_type}, L{exc_tb.tb_lineno}, {exc_obj}')

        return filename

    def button_link(self, url, state):
        response = self.rs.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        for v in soup.find('div', 'btn-group btn-group-paging').find_all('a'):
            if v.getText() == state:
                link = f"https://www.ptt.cc{v.get('href')}"
                return link

    def specification_date(self, url, state, date_stop):
        self.log.info('search articles date')
        sleep(0.5)
        if state == 'all':
            old = self.button_link(url=url, state='最舊')
            new = url
            bbs_url, index = os.path.split(new)
            index = int(index.replace('index', '').replace('.html', ''))
            # 找最舊日期的文章
            date_old = self.parse_page_of_articles(url=old, state=state, add1reduce1=+1)[0]
            # 找最新日期的文章
            date_new = self.parse_page_of_articles(url=new, state=state, add1reduce1=-1)[0]
            #
            dateSTOP_1D = datetime.date(
                int(str(date_stop)[0:4]),
                int(str(date_stop)[4:6]),
                int(str(date_stop)[6:8])
            )
            num = 0
            while True:
                num += 1
                date_old_1D = datetime.date(
                    int(str(date_old)[0:4]),
                    int(str(date_old)[4:6]),
                    int(str(date_old)[6:8])
                )
                date_new_1D = datetime.date(
                    int(str(date_new)[0:4]),
                    int(str(date_new)[4:6]),
                    int(str(date_new)[6:8])
                )
                total_days = (date_new_1D - date_old_1D).days
                total_page = index
                switch_page = (total_page / total_days)  # float
                reduce_days = (dateSTOP_1D - date_new_1D).days  # 小於目標天數則是負數
                # print(
                #     num,
                #     f'總頁數:{total_page}',
                #     "", f'距離目標天數:{reduce_days}',
                #     "",
                #     f'一天應含幾頁:{switch_page}',
                # )
                self.log.info(
                    f'{num}, 總頁數:{total_page}, 離目標天數:{reduce_days}, 一天應含幾頁:{switch_page}'
                )

                if reduce_days >= 0:
                    if new == url:
                        self.log.info(f'{num}, homepage: {new}')
                        return new
                    #
                    new_prev = new
                    index += 1
                    new = f'{bbs_url}/index{str(index)}.html'
                    val = self.parse_page_of_articles(
                        url=new,
                        state=state,
                        add1reduce1=int(
                            1 if abs(int(reduce_days * switch_page)) == 0 else
                            abs(int(reduce_days * switch_page))
                        )
                    )
                    if val[0] <= date_stop:
                        self.log.info(f'{num}, ~=, {new}')
                        new = val[1]
                    else:
                        self.log.info(f'{num}, =, {new_prev}')
                        return new_prev
                else:
                    if int(reduce_days * switch_page) == 0:
                        index = 1 if (index - 1) <= 0 else (index - 1)
                    else:
                        index = index + int(reduce_days * switch_page)
                    new = f'{bbs_url}/index{str(index)}.html'
                    self.log.info(f'{num}, <, {new}')
                    # print(int(1 if abs(int(reduce_days * switch_page)) == 0 else
                    #           abs(int(reduce_days * switch_page))))
                    val = self.parse_page_of_articles(
                        url=new,
                        state=state,
                        add1reduce1=int(
                            1
                            if abs(int(reduce_days * switch_page)) == 0 else
                            abs(int(reduce_days * switch_page))
                        )
                    )
                    date_new = val[0]
                    new = val[1]

        elif state == 'search':
            old = self.button_link(url=url, state='最舊')
            index_old = int(os.path.split(old)[1].split('&')[0].split(u'=')[1])
            new = url
            bbs_url, search = os.path.split(new)
            index = int(search.split('&')[0].split(u'=')[1])
            search = search.split('&')[1]
            # 找最舊日期的文章
            date_old = self.parse_page_of_articles(url=old, state=state, add1reduce1=-1)[0]
            # 找最新日期的文章
            date_new = self.parse_page_of_articles(url=new, state=state, add1reduce1=+1)[0]
            #
            dateSTOP_1D = datetime.date(
                int(str(date_stop)[0:4]),
                int(str(date_stop)[4:6]),
                int(str(date_stop)[6:8])
            )
            num = 0
            while True:
                num += 1
                date_old_1D = datetime.date(
                    int(str(date_old)[0:4]),
                    int(str(date_old)[4:6]),
                    int(str(date_old)[6:8])
                )
                date_new_1D = datetime.date(
                    int(str(date_new)[0:4]),
                    int(str(date_new)[4:6]),
                    int(str(date_new)[6:8])
                )
                total_days = (date_new_1D - date_old_1D).days
                total_page = index_old - index
                switch_page = (total_page / total_days)  # float
                reduce_days = (dateSTOP_1D - date_new_1D).days
                self.log.info(
                    f'{num}, 總頁數:{total_page}, 離目標天數:{reduce_days}, 一天應含幾頁:{switch_page}'
                )
                if reduce_days >= 0:
                    if new == url:
                        self.log.info(f'{num}, homepage: {new}')
                        return new
                    #
                    new_prev = new
                    index -= 1
                    new = f'{bbs_url}/search?page={str(index)}&{search}'
                    val = self.parse_page_of_articles(
                        url=new,
                        state=state,
                        add1reduce1=int(
                            1 if int(reduce_days * switch_page) == 0 else
                            int(reduce_days * switch_page)
                        )
                    )
                    if val[0] <= date_stop:
                        self.log.info(f'{num}, ~=, {new}')
                        new = val[1]
                    else:
                        self.log.info(f'{num}, =, {new_prev}')
                        return new_prev
                else:
                    if int(reduce_days * switch_page) == 0:
                        index = index_old if (index + 1) >= index_old else (index + 1)
                    else:
                        index = index + abs(int(reduce_days * switch_page))

                    new = f'{bbs_url}/search?page={str(index)}&{search}'
                    self.log.info(f'{num}, <, {new}')
                    val = self.parse_page_of_articles(
                        url=new,
                        state=state,
                        add1reduce1=int(
                            1 if int(reduce_days * switch_page) == 0 else
                            int(reduce_days * switch_page)
                        )
                    )
                    date_new = val[0]
                    new = val[1]

    def parse_page_of_articles(self, url, state, add1reduce1):
        while True:
            resp = self.rs.get(url)
            soup = BeautifulSoup(resp.text, 'lxml')
            if soup.find('div', 'r-list-sep') is not None:
                divs = soup.find('div', class_='r-list-container action-bar-margin bbs-screen')
                divs = divs.find_all('div', {'class': ['r-ent', 'r-list-sep']})
                text = list(takewhile(lambda x: 'r-list-sep' not in x['class'], divs))
            else:
                text = soup.find_all("div", "r-ent")

            if text == list():
                print('No text matching the search criteria')
                quit()
            else:
                if state == 'all':
                    for v in text:
                        meta = v.find('div', 'title').find('a') or NOT_EXIST
                        if meta.get('href') is not None:
                            link = f"https://www.ptt.cc{meta.get('href')}"
                            # return bool,board,url
                            torf, board, _ = self.parse_date_in_article(link)
                            if torf is True:
                                return board, url
                    url, index = os.path.split(url)
                    index = int(index.replace('index', '').replace('.html', ''))
                    url = url + '/index%s.html' % str(index - add1reduce1)
                elif state == 'search':
                    for i in range(len(text) - 1, -1, -1):
                        v = text[i]
                        meta = v.find('div', 'title').find('a') or NOT_EXIST
                        if meta.get('href') is not None:
                            link = f"https://www.ptt.cc{meta.get('href')}"
                            # return bool,board,url
                            torf, board, _ = self.parse_date_in_article(link)
                            if torf is True:
                                return board, url
                    url, search = os.path.split(url)
                    page = int(search[0].split('page=')[1])  # page=1&q=recommend%3A30
                    search = search.split('&')[1]
                    url = f'{url}/page={str(int(page) + int(add1reduce1))}&{search}'
                # print(url)

    def parse_date_in_article(self, url):
        response = self.rs.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        if soup.find('div', 'bbs-screen bbs-content').getText() == '404 - Not Found.':
            return False, "", url
        else:
            try:
                info = soup.find_all('span', 'article-meta-tag')
                info_vaule = soup.find_all('span', 'article-meta-value')
                if len(info) == len(info_vaule):
                    for v in range(len(info)):
                        if info[v].getText() == '時間':
                            re_date = info_vaule[v].getText()
                            re_date = re_date.split()  # 星期,月份,日期,時間,年分
                            date = int(re_date[4] + MONTH[re_date[1]] + (
                                re_date[2] if len(re_date[2]) == 2 else '0' + re_date[2]))
                            return True, date, url
                    pass
                else:
                    pass
            except Exception:
                # re_date = soup_findDATE.find('div', 'bbs-screen bbs-content')
                # .getText().split('\n')[3].split(': ')[1]
                #
                # 作者: ch89710 (Glucose) 看板: sex
                # 標題: [心得] 最初&最後
                # 時間: Tue Feb 14 16:36:01 2017
                #
                pass
            return False, 'date-error', url

    def parse(self, link, articleid, board, limitdate, timeout=60):
        self.log.info(f'Processing article: {articleid}')
        sleep(0.5)
        resp = requests.get(url=link, cookies={'over18': '1'}, verify=True, timeout=timeout)
        if resp.status_code != 200:
            self.log.debug(f'invalid url: {resp.url}')
            return json.dumps({"error": "invalid url"}, sort_keys=True, ensure_ascii=False)
        soup = BeautifulSoup(resp.text, 'html.parser')
        main_content = soup.find(id="main-content")
        metas = main_content.select('div.article-metaline')
        author = title = date = ''
        if metas:
            author = metas[0].select('span.article-meta-value')[0].string if \
                metas[0].select('span.article-meta-value')[0] else author
            title = metas[1].select('span.article-meta-value')[0].string if \
                metas[1].select('span.article-meta-value')[0] else title
            date = metas[2].select('span.article-meta-value')[0].string if \
                metas[2].select('span.article-meta-value')[0] else date
            # remove meta nodes
            for meta in metas:
                meta.extract()
            for meta in main_content.select('div.article-metaline-right'):
                meta.extract()

        # 判斷文章日期是否超過限制的數字
        if date != "":
            week, mon, day, hour, year = date.split()  # 星期,月份,日期,時間,年分
            date = int(f"{year}{MONTH[mon]}{day if len(day) == 2 else f'0{day}'}")
            if limitdate is not None:
                if limitdate[0] <= date <= limitdate[1]:
                    pass
                elif date < limitdate[0] and date <= limitdate[1]:
                    return
                else:
                    return "next"

            # remove and keep push nodes
            pushes = main_content.find_all('div', class_='push')
            for push in pushes:
                push.extract()

            # noinspection PyBroadException
            try:
                ip = main_content.find(text=re.compile(u'※ 發信站:'))
                ip = re.search('[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*', ip).group()
            except Exception:
                ip = "None"

            # 移除 '※ 發信站:' (starts with u'\u203b'),
            # '◆ From:' (starts with u'\u25c6'), 空行及多餘空白
            # 保留英數字, 中文及中文標點, 網址, 部分特殊符號
            filtered = [
                v for v in main_content.stripped_strings if
                v[0] not in [u'※', u'◆'] and v[:2] not in [u'--']
            ]
            expr = re.compile(u(
                r'[^\u4e00-\u9fa5\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\s\w:/-_.?~%()]'))
            for i in range(len(filtered)):
                filtered[i] = re.sub(expr, '', filtered[i])

            # remove empty strings
            filtered = [_ for _ in filtered if _]
            # remove last line containing the url of the article
            filtered = [x for x in filtered if articleid not in x]
            content = ' '.join(filtered)
            content = re.sub(r'(\s)+', ' ', content)

            # push messages
            p, b, n = 0, 0, 0
            messages = []
            for push in pushes:
                if not push.find('span', 'push-tag'):
                    continue
                push_tag = push.find('span', 'push-tag').string.strip(' \t\n\r')
                push_userid = push.find('span', 'push-userid').string.strip(' \t\n\r')
                # if find is None: find().strings -> list -> ' '.join; else the current way
                push_content = push.find('span', 'push-content').strings
                push_content = ' '.join(push_content)[1:].strip(' \t\n\r')  # remove ':'
                push_ipdatetime = push.find('span', 'push-ipdatetime'). \
                    string.strip(' \t\n\r')
                messages.append(
                    {'push_tag': push_tag, 'push_userid': push_userid,
                     'push_content': push_content,
                     'push_ipdatetime': push_ipdatetime})
                if push_tag == u'推':
                    p += 1
                elif push_tag == u'噓':
                    b += 1
                else:
                    n += 1
            message_count = {
                'all': p + b + n,
                'count': p - b,
                'push': p,
                'boo': b,
                'neutral': n
            }

            # json data
            data = {
                'url': link,
                'board': board,
                'article_id': articleid,
                'article_title': title,
                'author': author,
                'date': date,
                'content': content,
                'ip': ip,
                'message_count': message_count,
                'messages': messages
            }
            self.log.info(f"{link}, {message_count['count']}, {title}")
            return json.dumps(data, sort_keys=True, ensure_ascii=False)

    @staticmethod
    def store(filename, data, mode):
        with codecs.open(filename, mode, encoding='utf-8') as f:
            f.write(data)

    @staticmethod
    def get(filename, mode='r'):
        with codecs.open(filename, mode, encoding='utf-8') as f:
            return json.load(f)


if __name__ == '__main__':
    print(os.path.basename(__file__))
    ptt = Ptt()
    ptt.crawler_articles(
        board="Nba",
        date=[20190501, 20190701],
        push=10,
        title='總冠軍'
    )
