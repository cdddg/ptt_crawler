# PTT-CRAWLER

因為 PTT網頁版 (https://www.ptt.cc/bbs/index.html)，不可以透過條件搜尋特定日期內的文章，所以開發一隻腳本爬取 PTT，並可以自由增加其他條件例如「作者」、「標題」、「推數」。

```python
if __name__ == '__main__':
    print(os.path.basename(__file__))
    ptt = Ptt()
    ptt.crawler_articles(
        board="Nba",
        date=[20190501, 20190701],
        push=10,
        title='總冠軍'
    )
```



