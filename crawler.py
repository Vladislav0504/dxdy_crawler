import json
import requests
import os.path
import logging
from bs4 import BeautifulSoup, element

logging.basicConfig(filename='crawler.log', format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def json_empty():
    return json.loads('{}')

def json_of(kv_pairs):
    result = json_empty()
    for key, value in kv_pairs.items():
        result[key] = value
    return result

def as_bytes(json_data):
    return (json.dumps(json_data, ensure_ascii=False) + '\n').encode('utf-8')

class Topic:
    def __init__(self, id, url):
        self.id_ = id

        self.data_ = json_empty()
        self.data_['posts'] = []
        self.url_ = self.data_['url'] = url


    def add_post(self):
        self.data_['posts'].append(json_of({'content': []}))

    def add_passage(self, text):
        self.data_['posts'][-1]['content'].append(json_of({'passage': text}))

    def add_quote(self, text):
        self.data_['posts'][-1]['content'].append(json_of({'quote': text}))


class SubForum:
    def __init__(self, id, basedir, url):
        self.id_ = id
        self.dir_ = None
        self.basedir_ = basedir

        self.data_ = json_empty()
        self.data_['topics'] = []
        self.data_['url'] = url

    def dump_topic(self, topic_data):
        if self.dir_ is None:
            self.dir_ = os.path.join(self.basedir_, f'forum-{self.id_:08d}')
            os.mkdir(self.dir_)
        self.data_['topics'].append(json_of({'id': topic_data.id_, 'url': topic_data.url_}))
        with open(os.path.join(self.dir_, f'topic-{topic_data.id_:08d}.data'), 'wb') as topic_out:
            topic_out.write(as_bytes(topic_data.data_))

    def dump(self):
        if self.dir_ is not None:
            with open(os.path.join(self.basedir_, f'forum-{self.id_:08d}.data'), 'wb') as sf_out:
                sf_out.write(as_bytes(self.data_))

class Crawler:
    def __init__(self, dirname):
        self.dirname_ = dirname

    @staticmethod
    def pages_count(link, soup):
        """
        Detect number of pages in sub-forum on in topic

        Assume that there is HTML tag 'nav' under which
        there is something like
        Страница <strong>1</string> из <strong>15</strong>
        """
        for navigation in soup.find_all(class_='nav'):
            strong_tags = list(navigation.find_all('strong'))
            nav_text = navigation.text
            if 'Страница' in nav_text and len(strong_tags) == 2:
                result = int(strong_tags[1].text)
                logging.info(f'{result} page(s) at {link}')
                return result
        logging.warning(f'Pages info not found at {link}. 1 is assumed')
        return 1

    @staticmethod
    def page_links(base_link, count, step):
        return [f'{base_link[:-5]}-{i * step}.html' for i in range(count - 1, 0, -1)]


    def process_topic(self, link, topic=None):
        response = requests.get(link)
        if response.status_code == 200:
            logging.info(f'Got {link}')

            soup = BeautifulSoup(response.text, 'html.parser')
            if topic is None:
                total_pages = Crawler.pages_count(link, soup)
                topic = Topic(self.new_id(), link)

                for page_link in Crawler.page_links(link, total_pages, 15):
                    self.process_topic(page_link, topic)

            for post in soup.find_all(class_='postbody')[::-1]:
                topic.add_post()
                count_br = 0
                passage = []
                for child in post.children:
                    if type(child) == element.Comment:
                        continue
                    if type(child) == element.Tag and child.name == 'br':
                        count_br += 1
                    else:
                        if passage and count_br == 2:
                            topic.add_passage(' '.join(passage).strip())
                            passage = []
                        count_br = 0
                    if type(child) == element.NavigableString:
                        passage.append(child)
                    else:
                        if child.name == 'div' and 'quotecontent' in child.get('class', []):
                            topic.add_quote(child.text)
                        elif child.name == 'img':
                            if child.attrs['alt']:
                                passage.append(f'[[[img:{child.attrs["alt"]}]]]')
                        elif child.name == 'a':
                            if child.attrs['href']:
                                passage.append(f'[[[link:{child.attrs["href"]};;;text:{child.text}]]]')
                        else:
                            passage.append(child.text)
                if passage:
                    topic.add_passage(' '.join(passage).strip())
        else:
            logging.warning(f'Response on {link}: ${response}')

        return topic


    def process_level(self, upper, link, sub_forum=None):
        """
        Traverse tree of sub-forums

        It should work correctly when at some level topics are mixed with
        subforums of yet another level

        It cares about contents spread though several pages

        upper - array of names of parent levels
        link - current link
        sub_forum - None if it is head page of subforum, SubForum instance otherwise
        """
        if link in self.visited_:
            return
        self.visited_.add(link)

        response = requests.get(link)
        logging.info(f'Response on {link}: ${response}')

        if response.status_code == 200:
            logging.info(f'Got {link}')

            soup = BeautifulSoup(response.text, 'html.parser')
            is_head = False
            if sub_forum is None:
                is_head = True
                total_pages = Crawler.pages_count(link, soup)
                sub_forum = SubForum(self.new_id(), self.dirname_, link)

                for page_link in Crawler.page_links(link, total_pages, 50):
                    print(sub_forum.id_, page_link)
                    self.process_level(upper, page_link, sub_forum)

            for a_tag in soup.find_all('a')[::-1]:
                found_link = a_tag.get('href')

                if found_link is None:
                    logging.warning(f'a tag without href attribute at {link}')

                class_list = a_tag.get('class', [])

                if 'forumlink' in class_list:
                    logging.info(f'found sub-forum {found_link} at {link}')
                    self.process_level(upper + [a_tag.text], found_link)

                elif 'topictitle' in a_tag.get('class', []):
                    logging.info(f'found topic {found_link} at {link}')
                    topic_data = self.process_topic(found_link)
                    sub_forum.dump_topic(topic_data)

            print(is_head, sub_forum.id_)
            if is_head:
                sub_forum.dump()

        else:
            logging.warning(f'Response on {link}: ${response}')

    def new_id(self):
        result = self.next_id_
        self.next_id_ += 1
        return result

    def run(self):
        self.visited_ = set()
        self.next_id_ = 1

        if os.path.exists(self.dirname_):
            raise ValueError(f'{self.dirname_} already exists')
        os.mkdir(self.dirname_)

        self.process_level([], 'https://dxdy.ru')

Crawler('dataset').run()
