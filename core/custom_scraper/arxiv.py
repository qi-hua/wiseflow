from bs4 import BeautifulSoup
from datetime import datetime
import os, re
import logging
from typing import Dict, Pattern

from utils.custom_markdownify import CustomMarkdownify
from utils.general_utils import extract_and_convert_dates

# 设置项目目录
project_dir = os.environ.get("PROJECT_DIR", "")
if project_dir:
    os.makedirs(project_dir, exist_ok=True)

# 配置日志记录器
log_formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 创建日志记录器并设置级别为DEBUG
logger = logging.getLogger('arxiv_scraper')
logger.handlers = []
logger.setLevel('DEBUG')
logger.propagate = False

# 创建文件处理器并设置级别为INFO
log_file = os.path.join(project_dir, 'arxiv_scraper.log')
file_handler = logging.FileHandler(log_file, 'a', encoding='utf-8')
file_handler.setLevel('INFO')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# 创建控制台处理器并设置级别为DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel('DEBUG')
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

custom_markdownify =CustomMarkdownify()

ARXIV_SCRAPER_MODE = os.getenv("ARXIV_SCRAPER_MODE",'abs')  # abs | html

# 预编译正则表达式，避免每次调用函数时重复编译
ARXIV_URL_PATTERNS: Dict[str, Pattern] = {
    'abs': re.compile(r"^https?://arxiv\.org/abs/.*"),
    'pdf': re.compile(r"^https?://arxiv\.org/pdf/.*"),
    'html': re.compile(r"^https?://arxiv\.org/html/.*"),
    'new': re.compile(r"^https?://arxiv\.org/list/.*/new(?:\?.*)?$"),
    'recent': re.compile(r"^https?://arxiv\.org/list/.*/recent(?:\?.*)?$"),
    'search': re.compile(r"^https?://arxiv\.org/search/.*"),
}

def classify_arxiv_url(url: str) -> str:
    """
    根据 arXiv URL 区分页面类型。
    返回 'abs', 'html', 'new', 'recent', 'search' 或 'other'。
    """
    # 遍历字典，匹配 URL
    for page_type, pattern in ARXIV_URL_PATTERNS.items():
        if pattern.match(url):
            return page_type

    # 如果没有匹配到任何模式，返回 'other'
    return 'other'

def url_inspect(url:str, base_url='https://arxiv.org') -> str:
    """
    检查并修复 URL，确保它是一个完整的 arXiv URL。
    """
    url = url.strip()
    # 检查是否是相对路径
    if url.startswith('/'):
        return base_url + url
    # 检查是否已经是完整的 URL
    elif url.startswith(base_url):
        return url
    else:
        raise ValueError(f"Invalid URL: {url}. Expected a full arXiv URL or a path starting with '/'.")


# 全局变量：当前年份的前两位
YEAR = str(datetime.now().year)[:2]

def extract_date(url:str):
    """# 在URL中搜索匹配dddd.的模式

    日期默认设置 为每月的第一日
    /abs/2412.19784  ->  2024-12-01
    /abs/2501.00364  ->  2025-01-01
    """
    matches = re.compile(r'(\d{2})(\d{2})\.').findall(url)

    for match in matches:
        year, month = match
        # 检查月份是否有效（01-12）
        if 1 <= int(month) <= 12:
            return f'{YEAR + year}-{month}-01'
    return None


class ArxivScraper:
    """
    arXiv 页面处理
    """
    async def arxiv_scraper(self, html: str, url: str) -> tuple[dict, set, list]:
        # 替换HTTP为HTTPS
        url = url.replace("http://", "https://", 1)
        url_type = classify_arxiv_url(url)
        if url_type == 'other':
            # 检查URL是否是arXiv链接
            logger.warning(f'{url} is not an arXiv URL, should not use this function.')
            return {}, set(), []
        scraper = getattr(self,'%s_scraper' % url_type, None)
        if not scraper:
            # 不支持处理该URL类型
            logger.warning(f'Processing this URL type({url_type}) is not supported.')
            return {}, set(), []
        try:
            return await scraper(html, url)
        except Exception as e:
            logger.warning(f"Failed to parse {url}\n{e}")
            return {}, set(), []

    async def abs_scraper(self, html: str, url: str) -> tuple[dict, set, list]:
        def abs_scraper_(parsed_soup) -> tuple[dict, set, list]:
            authors_div = soup.find('div', class_='authors')
            authors = authors_div.get_text()
            if authors.startswith('Authors:'):
                authors = authors[8:]
            citation_title = parsed_soup.find('meta', attrs={'name': 'citation_title'})['content']
            citation_date = parsed_soup.find('meta', attrs={'name': 'citation_date'})['content']
            if citation_date:
                citation_date = extract_and_convert_dates(citation_date)
            citation_abstract = parsed_soup.find('meta', attrs={'name': 'citation_abstract'})['content']
            article = {
                'title': citation_title,
                'author': authors,
                'publish_date': citation_date,
                'content': citation_abstract
            }
            # exit()
            return article, set(), []

        soup = BeautifulSoup(html, 'html.parser')
        if ARXIV_SCRAPER_MODE == 'abs':
            return abs_scraper_(soup)
        else:
            # 获取html页面链接
            links = set()
            html_url = soup.find('a', attrs={'id': 'latexml-download-link'})['href']
            if html_url:
                html_url = url_inspect(html_url)
                links.add(html_url)
                return {}, links, []
            else:
                return abs_scraper_(soup)

    async def html_scraper(self, html: str, url: str) -> tuple[dict, set, list]:
        def extract_authors(html_soup):
            authors = []
            # 查找所有包含作者信息的标签
            author_tags = html_soup.find('div', class_='ltx_authors').find_all('span', class_='ltx_personname')

            for tag in author_tags:
                # 创建一个标签的副本
                tag_copy = tag.__copy__()
                # 删除所有 <sup> 标签及其内容
                for sup in tag_copy.find_all(class_=['ltx_sup', 'ltx_note']):
                    sup.decompose()
                # 删除所有 <a> 标签及其内容
                for a in tag_copy.find_all('a'):
                    a.decompose()
                # 使用 '\t' 作为分隔符，获取标签内的文本内容
                text = tag_copy.get_text(separator='\t', strip=True)
                # 根据分隔符分割文本内容，去除空白字符，得到作者名称列表
                author_list = [author.strip() for author in text.split('\t') if author.strip()]

                cleaned_authors = []
                for author in author_list:
                    # 去除纯数字的单词
                    author = ' '.join([word for word in author.split() if not word.isdigit() and not word == '&'])
                    # 去除多余的空格
                    author = ' '.join(author.split())
                    if author:
                        cleaned_authors.append(author)

                # 将清理后的作者名称添加到列表中
                authors.extend(cleaned_authors)

            # 将多个作者用逗号连接
            return ', '.join(authors).replace(', ,', ',')

        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title').text.strip()

        # 提取作者名字
        authors = extract_authors(soup)

        content_element = soup.find_all(class_=['ltx_abstract', 'ltx_section'])
        content_ = [custom_markdownify.convert_soup(c) for c in content_element]
        content = ''.join(content_)

        publish_date = extract_date(url)

        article = {
            'title': title,
            'author': authors,
            'publish_date': publish_date,
            'content': content
        }
        return article, set(), []

    async def new_scraper(self, html: str, url: str) -> tuple[dict, set, list]:
        soup = BeautifulSoup(html, 'html.parser')
        scraper_mode = f'/{ARXIV_SCRAPER_MODE}/'
        links = set()
        for dt in soup.find_all('dt'):
            href = dt.find('a', href=lambda href: href and scraper_mode in href)
            if href:
                links.add(url_inspect(href['href']))
            else:   # 有的论文没有 html 格式
                href = dt.find('a', href=lambda href: href and '/abs/' in href)
                links.add(url_inspect(href['href']))

        return {}, links, []

    async def recent_scraper(self, html: str, url: str) -> tuple[dict, set, list]:
        return await self.new_scraper(html, url)

    async def search_scraper(self, html: str, url: str) -> tuple[dict, set, list]:
        soup = BeautifulSoup(html, 'html.parser')
        # 搜索结果的链接只有 /abs/  /pdf/  /format/  没有/html/格式，如果需要/html/格式，会在 /abs/页面跳转到/html/
        links = soup.find_all('a', href=lambda href: href and '/abs/' in href)
        links = set([url_inspect(l['href']) for l in links])
        return {}, links, []



arxiv_scraper_object = ArxivScraper()

async def arxiv_scraper(html: str, url: str) -> tuple[dict, set, list]:
    return await arxiv_scraper_object.arxiv_scraper(html, url)
