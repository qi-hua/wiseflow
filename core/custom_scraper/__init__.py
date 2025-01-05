from .mp import mp_scraper
from .arxiv import arxiv_scraper

custom_scraper_map = {
    'mp.weixin.qq.com': mp_scraper,
    'arxiv.org': arxiv_scraper
}
