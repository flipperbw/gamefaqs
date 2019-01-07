#!/usr/bin/env python3

from datetime import datetime
# import json
import re
import sys
from time import sleep

# noinspection PyProtectedMember
from bs4 import (
    #Comment,
    SoupStrainer
)
import soupy
from dateutil.parser import parse as dateparse
#from pprint import pprint
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from sqlalchemy import inspect
from sqlalchemy.orm.dynamic import AppenderQuery
from termcolor import colored

from sqla import (
    session,
    Developer,
    Esrb_Content,
    Game,
    Game_Expansion,
    Game_Stat,
    Game_Release,
    Game_Recommendation,
    Game_Compilation
)


# -- GLOBALS --

base_url = 'https://gamefaqs.gamespot.com'
main_url = base_url + '/games/rankings?platform={system}&list_type=rate&dlc=0&min_votes={minvotes}'

sleep_delay = 0.34

# -------------

# PlayStation 4, Nintendo Switch, 3DS, DS, Wii, GameCube, PC, PlayStation 2, PlayStation, Nintendo 64, PSP, iOS (iPhone/iPad)
# Dreamcast, PlayStation 3, PlayStation Vita, Wii U, Xbox, Xbox 360, Xbox One
systems = ['PlayStation 4']
#systems = [
#    'PlayStation 4', 'Nintendo Switch', '3DS', 'DS', 'Wii', 'GameCube',
#    'PC', 'PlayStation 2', 'PlayStation', 'Nintendo 64', 'PSP', 'iOS (iPhone/iPad)',
#    'Dreamcast', 'PlayStation 3', 'PlayStation Vita', 'Wii U', 'Xbox', 'Xbox 360', 'Xbox One'
#]

start_page = 0
#num_pages = False
num_pages = 40
#max_games = None
max_games = 1

rerun = True

echo_changes = True
do_commit = True

db = psycopg2.connect(host="localhost", user="brett", password="", database="gamefaqs")
cursor = db.cursor(cursor_factory=RealDictCursor)

strainer = SoupStrainer('div', {'id': 'content'})

# 1 = 50+, 2 = 5+ votes
# todo: make all 1s

system_dict = {
    '3DO': ('61', 2),
    '3DS': ('116', 2),
    'APF-*1000/IM': ('12', 2),
    'Acorn Archimedes': ('48', 2),
    'Adventurevision': ('32', 2),
    'Amazon Fire TV': ('122', 2),
    'Amiga': ('39', 2),
    'Amiga CD32': ('70', 2),
    'Amstrad CPC': ('46', 2),
    'Android': ('106', 2),
    'Apple II': ('8', 2),
    'Arcade Games': ('2', 2),
    'Arcadia 2001': ('28', 2),
    'Astrocade': ('7', 2),
    'Atari 2600': ('6', 2),
    'Atari 5200': ('20', 2),
    'Atari 7800': ('51', 2),
    'Atari 8-bit': ('13', 2),
    'Atari ST': ('38', 2),
    'BBC Micro': ('22', 2),
    'BBS Door': ('50', 2),
    'Bandai Pippin': ('81', 2),
    'BlackBerry': ('107', 2),
    'CD-I': ('60', 2),
    'CPS Changer': ('75', 2),
    'Casio Loopy': ('80', 2),
    'Cassette Vision': ('26', 2),
    'Channel F': ('4', 2),
    'Colecovision': ('29', 2),
    'Commodore 64': ('24', 2),
    'Commodore PET': ('15', 2),
    'CreatiVision': ('23', 2),
    'DS': ('108', 1),
    'DVD Player': ('87', 2),
    'Dedicated Console': ('125', 2),
    'Dreamcast': ('67', 2),
    'EACA Colour Genie 2000': ('31', 2),
    'FM Towns': ('55', 2),
    'FM-7': ('30', 2),
    'Famicom Disk System': ('47', 2),
    'Flash': ('102', 2),
    'GP32': ('100', 2),
    'Game Boy': ('59', 2),
    'Game Boy Advance': ('91', 2),
    'Game Boy Color': ('57', 2),
    'Game.com': ('86', 2),
    'GameCube': ('99', 2),
    'GameGear': ('62', 2),
    'Genesis': ('54', 2),
    'Gizmondo': ('110', 2),
    'Intellivision': ('16', 2),
    'Interton VC4000': ('10', 2),
    'Jaguar': ('72', 2),
    'Jaguar CD': ('82', 2),
    'LaserActive': ('71', 2),
    'Linux': ('33', 2),
    'Lynx': ('58', 2),
    'MSX': ('40', 2),
    'Macintosh': ('27', 2),
    'Mattel Aquarius': ('36', 2),
    'Microvision': ('17', 2),
    'Mobile': ('85', 2),
    'N-Gage': ('105', 2),
    'NEC PC88': ('21', 2),
    'NEC PC98': ('42', 2),
    'NES': ('41', 2),
    'Neo-Geo CD': ('68', 2),
    'NeoGeo': ('64', 2),
    'NeoGeo Pocket Color': ('89', 2),
    'Nintendo 64': ('84', 2),
    'Nintendo 64DD': ('92', 2),
    'Nintendo Switch': ('124', 2),
    'Nuon': ('93', 2),
    'OS/2': ('73', 2),
    'Odyssey': ('3', 2),
    'Odyssey^2': ('9', 2),
    'Online/Browser': ('69', 2),
    'Oric 1/Atmos': ('44', 2),
    'Ouya': ('119', 2),
    'PC': ('19', 1),
    'PC-FX': ('79', 2),
    'PSP': ('109', 1),
    'Palm OS Classic': ('96', 2),
    'Palm webOS': ('97', 2),
    'Pinball': ('1', 2),
    'PlayStation': ('78', 1),
    'PlayStation 2': ('94', 1),
    'PlayStation 3': ('113', 1),
    'PlayStation 4': ('120', 1),
    'PlayStation Vita': ('117', 2),
    'Playdia': ('77', 2),
    'RCA Studio II': ('5', 2),
    'Redemption': ('104', 2),
    'SG-1000': ('43', 2),
    'Saturn': ('76', 2),
    'Sega 32X': ('74', 2),
    'Sega CD': ('65', 2),
    'Sega Master System': ('49', 2),
    'Sharp X1': ('37', 2),
    'Sharp X68000': ('52', 2),
    'Sinclair ZX81/Spectrum': ('35', 2),
    'Sord M5': ('25', 2),
    'Super Cassette Vision': ('45', 2),
    'Super Nintendo': ('63', 2),
    'SuperVision': ('66', 2),
    'TI-99/4A': ('14', 2),
    'Tandy Color Computer': ('18', 2),
    'Tomy Tutor': ('123', 2),
    'Turbo CD': ('56', 2),
    'TurboGrafx-16': ('53', 2),
    'VIC-20': ('11', 2),
    'Vectrex': ('34', 2),
    'Virtual Boy': ('83', 2),
    'Wii': ('114', 1),
    'Wii U': ('118', 2),
    'Windows Mobile': ('88', 2),
    'WonderSwan': ('90', 2),
    'WonderSwan Color': ('95', 2),
    'Xbox': ('98', 2),
    'Xbox 360': ('111', 1),
    'Xbox One': ('121', 2),
    'Zeebo': ('115', 2),
    'Zodiac': ('103', 2),
    'e-Reader': ('101', 2),
    'iOS (iPhone/iPad)': ('112', 2)
}


last_request = None

headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'}
r_session = requests.Session()
r_session.headers.update(headers)


def wait_request(href):
    global last_request
    if not last_request:
        sdiff = 1
    else:
        curr_time = datetime.now()
        sdiff = (curr_time - last_request).total_seconds()

    sleep_time = sleep_delay - sdiff
    if sleep_time > 0:
        sleep(sleep_time)

    txt = r_session.get(href).text

    last_request = datetime.now()

    return txt


# todo: go through xtext and see if can set to default


class XNode(soupy.Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def xtext(self, default=False):
        t = self.text
        if t:
            return t.val().strip()
        else:
            if default:
                return ''
            else:
                return None

    def xget(self, key, default=None):
        return self.attrs.val().get(key, default)

    def select_one(self, selector):
        value = self.select(selector)
        return value[0]


class XNullNode(soupy.NullNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def xtext(default=False):
        if default:
            return ''
        else:
            return None

    @staticmethod
    def xget(*_args, **_kwargs):
        return None

    def select_one(self, selector):
        value = self.select(selector)
        return value[0]


soupy.Node = XNode
soupy.NullNode = XNullNode


def clean_soup(soup):
    for script in soup(["script", "link", "style", "noscript", "meta"]):
        script.val().extract()
    #for comment in soup.find_all(text=lambda t: isinstance(t, Comment)):
    #    comment.val().extract()
    for bad in soup.select('#mygames_nouser_dialog'):
        bad.val().extract()
    return soup


def get_soup(url):
    if not url:
        return soupy.Soupy('', 'lxml')
    d = wait_request(url)
    soup = clean_soup(soupy.Soupy(d, 'lxml', parse_only=strainer))
    return soup.find('div', attrs={'class': 'main_content'})


def get_new_page(i, system_url):
    if i:
        sys_page = '{}&page={}'.format(system_url, i)
    else:
        sys_page = system_url

    return get_soup(sys_page)


def get_game_id(url):
    return int(url.split('/')[-1].split('-')[0])


def add_base_url(url):
    if not url:
        return None
    if base_url not in url:
        url = base_url + url
    return url


re_comp = {
    'genre': re.compile(r'^Genre'),
    'developer': re.compile(r'^Developer'),
    'wiki': re.compile(r'^Wikipedia \(EN\):'),
    'esrb': re.compile(r'^ESRB Descriptor'),
    'release': re.compile(r'^Release:'),
    'franchise': re.compile(r'^Franchise:'),
    'also': re.compile(r'^Also on:'),
    'aka': re.compile(r'^Also Known As:'),
    'expansion': re.compile(r'^Expansion for:'),
    'local_players': re.compile(r'^Local Players:'),
    'multi_players': re.compile(r'^Online Players:'),
    'quarters': re.compile(r'Q[1-4]\s+'),
}


def get_changes():
    print('-> New')
    for obj in session.new:
        o_type = obj.__class__.__name__.lower()
        print('  {}'.format(o_type))
        #for attr in inspect(obj).attrs:
        #    if attr.history.has_changes():
        #        print('  {}: "{}" to "{}"'.format(attr.key, attr.history.deleted, attr.history.added))

    print('-> Deleted')
    for obj in session.deleted:
        o_type = obj.__class__.__name__.lower()
        print('  {}: {}'.format(o_type, obj.id))

    print('-> Changes')
    for obj in session.dirty:
        o_type = obj.__class__.__name__.lower()
        print('  {}: {}'.format(o_type, obj.id))
        for attr in inspect(obj).attrs:
            if attr.history.has_changes():
                print('    {}: deleted "{}" and added "{}"'.format(attr.key, attr.history.deleted, attr.history.added))

    session.flush()


def get_game_details(db_game):
    p = Parser(db_game)
    p.update_all()
    #pprint(vars(p.obj))

    return p


def commit(p):
    if p.is_valid:
        session.add(p.obj)
        p.commit()


class Parser:
    def __init__(self, obj):
        self.obj = obj
        self.is_valid = True
        self.is_dlc = isinstance(obj, Game_Expansion)

        self._set_soup()

    @staticmethod
    def _log(t, e='yellow'):
        if e:
            print(colored(t, e))
        else:
            print(t)
        return None

    def _get_url(self, typ):
        if self.is_dlc:
            if typ in ('main', 'data'):
                return self.obj.url
            else:
                return None
        else:
            if typ == 'main':
                return None
            else:
                return '{}/{}'.format(self.obj.url, typ)

    def _set_soup(self):
        self.soup_main = {
            'main': get_soup(self._get_url('main'))
        }
        self.soup_data = {
            'main': get_soup(self._get_url('data'))
        }
        self.soup_stats = {
            'main': get_soup(self._get_url('stats'))
        }
        self.soup_recommendations = {
            'main': get_soup(self._get_url('playing'))
        }

    def _setif(self, k, v, obj=None):
        if obj is None:
            obj = self.obj
        if v is not None:
            if hasattr(obj, k):
                old = getattr(obj, k)
                if isinstance(old, AppenderQuery):
                    old = set(old.all())
                    comp_v = set(v)
                else:
                    comp_v = v
                if comp_v != old:
                    setattr(obj, k, v)
            else:
                self._log('-> Error: Attribute does not exist ({}) for ({})'.format(k, obj), 'red')

    def _setdictif(self, d, o=None):
        for k, v in d.items():
            self._setif(k, v, o)

    def update_all(self):
        #TODO: create a class for expansion and regular game
        if self.is_dlc:
            # TODO: add boxart
            self._soup_data_upper_right()
            self.update_main()
        else:
            self._soup_data_upper_right()
            if self._check_expansion() is None:
                self.is_valid = False
                return
            
            self.update_data()
            self.update_stats()
            self.update_recommendations()

    # TODO: passing words to find makes a soupstrainer every time, better to compile

    def update_main(self):  # only for dlc right now
        self._setif('name', self.name())
        self._setif('description', self.description())

        self._setif('release_date', self.release_date())
        self._setdictif(self.metacritic())
        self._setif('esrb_rating', self.esrb_rating())

        self.soup_main['overview'] = self.soup_main['main'].select_one('form#js_mygames .pod_split .body')

        self._setif('owners', self.owners_overview())

        self._setdictif(self.rating_overview())
        self._setdictif(self.difficulty_overview())
        self._setdictif(self.playtime_overview())
        self._setdictif(self.completed_overview())

    def _soup_data_upper_right(self):
        self.soup_data['up_right'] = self.soup_data['main'].find('div', {'class': 'pod_gameinfo'}).find('div', {'class': 'body'}).find('ul')

    def name(self):
        return self.soup_data['main'].select_one('header.page-header .page-title a').xtext()

    def description(self):
        return self.soup_data['main'].select_one('div.pod_gamespace .game_desc .desc').xtext()

    def release_date(self):
        release = self.soup_data['up_right'].find('b', text=re_comp.get('release')).find_next_sibling('a').xtext()
        if release is None:
            return None

        release = re.sub(re_comp.get('quarters'), '', release)
        if release.endswith(' »'):
            release = release.replace(' »', '').replace('  ', ' ')

        try:
            release_date = dateparse(release)
        except ValueError:
            return self._log('-> Error: could not parse release date ({})'.format(release))
        else:
            return release_date.date()

    def metacritic(self):
        v = {}
        data = self.soup_data['up_right'].find('li', {'class': 'metacritic'})

        rating = data.find('div', {'class': 'score'}).xtext()
        if rating is not None:
            v['metacritic_rating'] = int(rating)

        metacritic_a = data.find('div', {'class': 'review_link'}).find('a')

        url = metacritic_a.xget('href')
        if url is not None:
            v['metacritic_url'] = url

        reviews = metacritic_a.xtext()
        if reviews is not None:
            v['metacritic_reviews'] = int(reviews.replace('From ', '').replace(' reviews', '').replace(' review', ''))

        return v

    def esrb_rating(self):
        v = self.soup_data['up_right'].find('span', {'class': 'esrb_logo'}).xtext()
        if v is not None:
            v = v.replace(' - ', '').replace(' -', '')
        return v

    def owners_overview(self):
        v = self.soup_main['overview'].select_one('.mygames_stats_own a').xtext()
        if v is not None:
            v = int(v.replace(' users', '').replace(' user', ''))
        return v

    def rating_overview(self):
        v = {}

        rating_div = self.soup_main['overview'].select_one('.mygames_rate .gamerater_label')

        score_div = rating_div.select_one('.mygames_stats_rate a').xtext()
        if score_div is not None:
            v['rating_score'] = float(score_div.split(' ')[0])

        num_rate_div = rating_div.select_one('.rate').xtext()
        if num_rate_div is not None:
            v['rating_votes'] = int(num_rate_div.split(' ')[0])

        return v

    def difficulty_overview(self):
        v = {}

        difficulty_div = self.soup_main['overview'].select_one('.mygames_diff .gamerater_label')

        difficulty_desc_div = difficulty_div.select_one('.mygames_stats_diff a').xtext()
        if difficulty_desc_div is not None:
            v['difficulty_desc'] = difficulty_desc_div

        difficulty_num_div = difficulty_div.select_one('.rate').xtext()
        if difficulty_num_div is not None:
            diff_num_spl = [i for i in difficulty_num_div.split(' ') if i not in ('of', 'total', 'votes')]
            if len(diff_num_spl) == 2:
                v['difficulty_pct'] = round(float(diff_num_spl[0].replace('%', '')) / 100.0, 6)
                v['difficulty_votes'] = int(diff_num_spl[1])

        return v

    def playtime_overview(self):
        v = {}

        playtime_div = self.soup_main['overview'].select_one('.mygames_time .gamerater_label')

        playtime_len_a = playtime_div.select_one('.mygames_stats_time a').xtext()
        if playtime_len_a is not None:
            playtime_len_str = playtime_len_a.replace(' hours', '').replace(' hour', '')
            if '80+' in playtime_len_str:
                playtime_len_str = 90
            try:
                playtime_len = float(playtime_len_str)
            except ValueError:
                self._log('-> Error: could not convert playtime ({})'.format(playtime_len_str))
            else:
                v['playtime_hours'] = playtime_len

        playtime_num_div = playtime_div.select_one('.rate').xtext()
        if playtime_num_div is not None:
            v['playtime_votes'] = int(playtime_num_div.split(' ')[0])

        return v

    def completed_overview(self):
        v = {}

        completed_div = self.soup_main['overview'].select_one('.mygames_play .gamerater_label .rate').xtext()
        if completed_div is not None:
            complted_div_spl = [v for v in completed_div.split(' ') if v not in ('of', 'total', 'votes')]
            if len(complted_div_spl) == 2:
                v['completed_pct'] = round(float(complted_div_spl[0].replace('%', '')) / 100.0, 6)
                v['completed_votes'] = int(complted_div_spl[1])

        return v

    def update_data(self):
        self._setdictif(self.boxart())
        self._setdictif(self.franchise())
        self._setif('also_on', self.also_on())
        self._setif('aka', self.aka())

        self._setif('metacritic_url', self.metacritic().get('metacritic_url'))

        self.soup_data['general'] = self.soup_data['main'].find('div', {'class': 'pod_titledata'}).find('div', {'class': 'body'}).find('dl')

        self._setif('genre', self.genre())
        self._setif('local_players', self.local_players())
        self._setif('multi_players', self.multi_players())
        self._setif('wiki', self.wiki())
        self._setif('developers', self.developers())
        self._setif('esrb_contents', self.esrb_contents())

        release_list = self.releases()
        if release_list:
            for rel in self.obj.game_releases:
                if rel not in release_list:
                    session.delete(rel)
        self._setif('game_releases', release_list)

        self._setdictif(self._release_us(release_list))

        comp_list = self.compilations()
        if comp_list:
            for com in self.obj.game_compilations:
                if com not in comp_list:
                    session.delete(com)
        self._setif('game_compilations', comp_list)

        exp_list = self.expansions()
        if exp_list:
            for exp in self.obj.game_expansions:
                if exp not in exp_list:
                    session.delete(exp)
        self._setif('game_expansions', exp_list)

        # TODO: make sure soups are set first
        
        #TODO: function replacing if X is none...

    def _check_expansion(self):
        expansion_b = self.soup_data['up_right'].find('b', text=re_comp.get('expansion'))
        if expansion_b:
            return self._log('=> Error: is DLC, skipping...', 'magenta')
        else:
            return True

    def boxart(self):
        v = {}

        boxart_b = self.soup_data['up_right'].find('img', {'class': 'boxshot'}).xget('src')
        if boxart_b is not None and 'akamaized.net/images/platform-' not in boxart_b:
            v['boxart_thumb'] = boxart_b
            v['boxart_front'] = boxart_b.replace('thumb.', 'front.')  # todo: check if exists

            boxart_c = self.soup_data['up_right'].find('a', {'class': 'imgboxart'}).xget('href')
            if boxart_c is not None:
                v['boxart_all'] = add_base_url(boxart_c)

        return v

    def franchise(self):
        v = {}

        franchise_b = self.soup_data['up_right'].find('b', text=re_comp.get('franchise')).find_next_sibling('a')
        f_txt = franchise_b.xtext()
        if f_txt is not None:
            v['franchise_name'] = f_txt
        fhref = franchise_b.xget('href')
        if fhref is not None:
            v['franchise_url'] = add_base_url(fhref)

        return v

    def also_on(self):
        # todo get system aliases
        alsoon = None
        alsoon_b = self.soup_data['up_right'].find('b', text=re_comp.get('also')).find_next_siblings()
        if alsoon_b:
            alsoon = [a.xtext() for a in alsoon_b]

        return alsoon

    def aka(self):
        # todo: multiple? (they are split on commas, but the string contains commas anyway
        # todo: combine this somehow
        return self.soup_data['up_right'].find('b', text=re_comp.get('aka')).find_next_sibling('i').xtext()

    def genre(self):
        return self.soup_data['general'].find('dt', text=re_comp.get('genre')).find_next_sibling('dd').xtext()
    
    def local_players(self):
        return self.soup_data['general'].find('dt', text=re_comp.get('local_players')).find_next_sibling('dd').xtext()

    def multi_players(self):
        return self.soup_data['general'].find('dt', text=re_comp.get('multi_players')).find_next_sibling('dd').xtext()

    def wiki(self):
        return self.soup_data['general'].find('dt', text=re_comp.get('wiki')).find_next_sibling('dd').xtext()

    def developers(self):
        v = []

        developer_bs = self.soup_data['general'].find_all('dt', text=re_comp.get('developer')).orelse([])
        for developer_b in developer_bs:
            developer_data = developer_b.find_next_sibling('dd')

            developer_name = developer_data.xtext()
            if not developer_name:
                continue

            if developer_name in developer_list:
                dev_obj = developer_list[developer_name]
            else:
                developer_url = developer_data.find('a').xget('href')
                if not developer_url:
                    continue

                developer_url = add_base_url(developer_url)

                dev_obj = Developer(name=developer_name, url=developer_url)

                developer_list[developer_name] = dev_obj

            v.append(dev_obj)

        return v

    def esrb_contents(self):
        v = []

        esrb_b = self.soup_data['general'].find('dt', text=re_comp.get('esrb')).find_next_sibling('dd').xtext()
        if esrb_b is not None:
            for ec in esrb_b.split(','):
                ec_name = ec.strip()

                if ec_name in esrb_content_list:
                    ec_obj = esrb_content_list[ec_name]
                else:
                    ec_obj = Esrb_Content(name=ec_name)

                    esrb_content_list[ec_name] = ec_obj

                v.append(ec_obj)

        return v

    def releases(self):
        release_data = self.soup_data['main'].find('table', {'class': 'contrib'}).find('tbody').find_all('tr').orelse([])
    
        v = []
        existing = self.obj.game_releases

        for r in release_data[::2]:
            r_title = r.find('td', {'class': 'ctitle'}).xtext()

            rnext = r.find_next_sibling()

            region = rnext.find('td', {'class': 'cregion'}).xtext()

            publisher_b = rnext.find('td', {'class': "datacompany"}).find('a')
            publisher = publisher_b.xtext()
            if publisher is None:
                publisher_url = None
            else:
                publisher_url = add_base_url(publisher_b.xget('href'))

            product_ids = rnext.find_all('td', {'class': "datapid"})
            product_id = product_ids[0].xtext()
            distribution = product_ids[1].xtext()

            release_date_text = rnext.find('td', {'class': "cdate"}).xtext()
            release_date = None
            if release_date_text is not None:
                release_date_text = re.sub(re_comp.get('quarters'), '', release_date_text)
                try:
                    release_date = dateparse(release_date_text)
                except ValueError:
                    self._log('-> Error: could not parse release date ({})'.format(release_date_text))

            esrb_rating_r = rnext.find('td', {'class': "datarating"}).xtext()

            rel_dict = {
                'region': region,
                'title': r_title,
                'publisher_name': publisher,
                'publisher_url': publisher_url,
                'product_uid': product_id,
                'distribution_uid': distribution,
                'release_date': release_date,
                'esrb_rating': esrb_rating_r
            }

            filtered = existing.filter_by(**rel_dict)
            filtered_len = filtered.count()
            if filtered_len == 0:
                release_obj = Game_Release()
                self._setdictif(rel_dict, release_obj)
            else:
                release_obj = filtered.first()
                if filtered_len > 1:
                    self._log('-> Error: found more than one entry for existing releases in db, using ID {}'.format(release_obj.id), 'red')

            v.append(release_obj)

        return v

    @staticmethod
    def _release_us(rels=()):
        for rel in rels:
            if rel.region == 'US' and all([rel.publisher_name, rel.product_uid, rel.release_date]):
                # todo only set nonnull
                return {
                    'release_distribution_uid': rel.distribution_uid,
                    'release_product_uid': rel.product_uid,
                    'release_publisher_name': rel.publisher_name,
                    'release_publisher_url': rel.publisher_url,
                    'release_date': rel.release_date,
                    'release_esrb_rating': rel.esrb_rating,
                    'release_title': rel.title
                }

        return {}

    def compilations(self):
        comp_dict = {
            'Included in Compilation': 'within',
            'Compilation Of': 'contains'
        }

        v = []
        existing = self.obj.game_compilations

        for comp_type, comp_name in comp_dict.items():
            compl_included = self.soup_data['main'].find('h2', text=comp_type).parent.find_next_sibling('div', {'class': 'body'}).find_all('tr').orelse([])
            for tr in compl_included:
                tds = tr.find_all('td')
                if len(tds) >= 2:
                    c_game = tds[0].find('a')
                    c_game_name = c_game.xtext()
                    c_game_link = c_game.xget('href')
                    if c_game_link is not None:
                        c_game_link = add_base_url(c_game_link).lower()
                    c_plat = tds[1].xtext()

                    filter_dict = {
                        'typ': comp_name,
                        'name': c_game_name,
                        'platform': c_plat,
                        'url': c_game_link,
                    }

                    filtered = existing.filter_by(url=c_game_link)
                    filtered_len = filtered.count()
                    if filtered_len == 0:
                        comp_obj = Game_Compilation()
                        self._setdictif(filter_dict, comp_obj)
                    else:
                        comp_obj = filtered.first()
                        if filtered_len > 1:
                            self._log('-> Error: found more than one entry for existing compilations in db, using ID {}'.format(comp_obj.id), 'red')

                    v.append(comp_obj)

        return v

    def expansions(self):
        v = []
        existing = self.obj.game_expansions

        expansion_trs = self.soup_data['main'].find('div', {'id': 'dlc'}).select('.body table tr').orelse([])
        for tr in expansion_trs:
            cell_a = tr.select_one('td a')
            game_href = cell_a.xget('href')
            if game_href is None:
                self._log('No URL found for expansion: {}'.format(cell_a.xtext()))
                continue

            game_href = add_base_url(game_href)

            filtered = existing.filter_by(url=game_href)
            filtered_len = filtered.count()
            if filtered_len == 0:
                exp_obj = Game_Expansion(url=game_href)
                set_dict = {
                    'name': cell_a.xtext(),
                    'gamefaqs_uid': get_game_id(game_href)
                }
                self._setdictif(set_dict, exp_obj)
                get_game_details(exp_obj)
            else:
                exp_obj = filtered.first()
                if filtered_len > 1:
                    self._log('-> Error: found more than one entry for existing compilations in db, using ID {}'.format(exp_obj.id), 'red')

                if rerun is False:
                    self._log('-> Exists, skipping insert for dlc ({})'.format(game_href), '')
                else:
                    self._log('-> Found dlc entry, now rerunning ({})'.format(game_href), '')
                    get_game_details(exp_obj)

            v.append(exp_obj)

        return v

    def update_stats(self):
        # todo: ADD INSTDEV to all
        existing = self.obj.game_stats
        o = None
        if existing.count():
            if rerun is False:
                return self._log('-> Stats exists, skipping insert for ({})'.format(self.obj.url), '')
            else:
                self._log('-> Found stats entry, now rerunning ({})'.format(self.obj.url), '')
                o = existing.first()

        self.soup_stats['figure'] = self.soup_stats['main'].find_all('figure', {'class': 'mygames_section bar'})
        len_stats = len(self.soup_stats['figure'])
        if len_stats != 5:
            return self._log(colored('-> Error: could not find stats: {} sections'.format(len_stats)))

        if o is None:
            o = Game_Stat()

        self._setdictif(self.ratings(), o)
        self._setif('owners', self.ownership(), obj=o)
        self._setdictif(self.progress(), o)
        self._setdictif(self.difficulty(), o)
        self._setdictif(self.playtime(), o)

        self._setif('game_stats', [o])

    def ratings(self):
        #todo combine these similar ones
        v = {}

        rating_section = self.soup_stats['figure'][0]

        num_ratings_tot = rating_section.find('figcaption').xtext()
        if num_ratings_tot is not None:
            num_ratings_tot = int(num_ratings_tot[num_ratings_tot.index(":") + 2: num_ratings_tot.index("-") - 1])

        avg_rating = 0.0
        terrible_pct = 0.0
        amazing_pct = 0.0

        star_dict = {
            '0.5': 'half',
            '1.0': 'one',
            '1.5': 'one_half',
            '2.0': 'two',
            '2.5': 'two_half',
            '3.0': 'three',
            '3.5': 'three_half',
            '4.0': 'four',
            '4.5': 'four_half',
            '5.0': 'five'
        }

        tbody = rating_section.find('tbody')
        for i in range(1, 11):
            stars = i / 2.0
            dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
            if dataspan:
                datapoint = float(dataspan.find('span').xtext(True).replace('%', '')) / 100.0
            else:
                datapoint = 0.0

            conv_rating = datapoint * stars

            star_str = 'rating_{}'.format(star_dict[str(stars)])

            v[star_str] = round(datapoint, 6)
            avg_rating += conv_rating

            if i in (1, 2):
                terrible_pct += datapoint
            elif i in (9, 10):
                amazing_pct += datapoint

        avg_rating = round(avg_rating, 6)
        terrible_pct = round(terrible_pct, 6)
        amazing_pct = round(amazing_pct, 6)
        diff_pct = round(amazing_pct - terrible_pct, 6)

        if num_ratings_tot is not None:
            v['rating_votes'] = num_ratings_tot
        v['rating_avg'] = avg_rating
        v['rating_pct_amazing'] = amazing_pct
        v['rating_pct_terrible'] = terrible_pct
        v['rating_pct_diff'] = diff_pct

        return v

    def ownership(self):
        own_section = self.soup_stats['figure'][1]
        total_owns = own_section.find('figcaption').xtext()
        if total_owns is not None:
            total_owns = int(total_owns[total_owns.index(":") + 2:])

        return total_owns

    def progress(self):
        v = {}

        play_section = self.soup_stats['figure'][2]

        total_plays = play_section.find('figcaption').xtext()
        if total_plays is not None:
            total_plays = int(total_plays[total_plays.index(":") + 2: total_plays.index("-") - 1])

        play_complete = 0.0
        play_incomplete = 0.0
        play_avg = 0.0

        tbody = play_section.find('tbody')
        # todo check if some of these are missing
        for i in range(1, 6):
            dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
            if dataspan:
                datapoint = float(dataspan.find('span').xtext(True).replace('%', '')) / 100.0
            else:
                datapoint = 0.0

            play_avg += (datapoint * i)

            k = 'unk'
            if i == 1:
                k = 'progress_pct_once'
            elif i == 2:
                k = 'progress_pct_some'
            elif i == 3:
                k = 'progress_pct_half'
            elif i == 4:
                k = 'progress_pct_finish'
            elif i == 5:
                k = 'progress_pct_platinum'

            v[k] = round(datapoint, 6)

            if i in (1, 2, 3):
                play_incomplete += datapoint
            elif i in (4, 5):
                play_complete += datapoint

        if total_plays is not None:
            v['progress_votes'] = total_plays
        v['progress_avg'] = round(play_avg, 6)
        v['progress_pct_complete'] = round(play_complete, 6)
        v['progress_pct_incomplete'] = round(play_incomplete, 6)

        return v

    def difficulty(self):
        v = {}

        difficulty_section = self.soup_stats['figure'][3]

        total_difficulty = difficulty_section.find('figcaption').xtext()
        if total_difficulty is not None:
            total_difficulty = int(total_difficulty[total_difficulty.index(":") + 2: total_difficulty.index("-") - 1])

        avg_difficulty = 0.0

        tbody = difficulty_section.find('tbody')
        for i in range(1, 6):
            dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
            if dataspan:
                datapoint = float(dataspan.find('span').xtext(True).replace('%', '')) / 100.0
            else:
                datapoint = 0.0

            avg_difficulty += (datapoint * i)

            k = 'unk'
            if i == 1:
                k = 'difficulty_easy'
            elif i == 2:
                k = 'difficulty_fine'
            elif i == 3:
                k = 'difficulty_moderate'
            elif i == 4:
                k = 'difficulty_hard'
            elif i == 5:
                k = 'difficulty_extreme'

            v[k] = round(datapoint, 6)

        v['difficulty_votes'] = total_difficulty
        v['difficulty_avg'] = round(avg_difficulty, 6)

        return v

    def playtime(self):
        v = {}

        times_section = self.soup_stats['figure'][4]

        total_times = times_section.find('figcaption').xtext()
        if total_times is not None:
            total_times = int(total_times[total_times.index(":") + 2: total_times.index("-") - 1])

        avg_times = 0.0

        hours_dict = {
            '1': (0.5, 'playtime_pct_half_hour'),
            '2': (1.0, 'playtime_pct_one_hour'),
            '3': (2.0, 'playtime_pct_two_hour'),
            '4': (4.0, 'playtime_pct_four_hour'),
            '5': (8.0, 'playtime_pct_eight_hour'),
            '6': (12.0, 'playtime_pct_twelve_hour'),
            '7': (20.0, 'playtime_pct_twenty_hour'),
            '8': (40.0, 'playtime_pct_forty_hour'),
            '9': (60.0, 'playtime_pct_sixty_hour'),
            '10': (90.0, 'playtime_pct_ninety_hour')
        }

        tbody = times_section.find('tbody')
        for i in range(1, 11):
            dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
            if dataspan:
                datapoint = float(dataspan.find('span').xtext(True).replace('%', '')) / 100.0
            else:
                datapoint = 0.0

            hours, hours_str = hours_dict.get(str(i), (0.0, None))

            v[hours_str] = round(datapoint, 6)
            avg_times += (datapoint * hours)

        if total_times is not None:
            v['playtime_votes'] = total_times
        v['playtime_avg'] = round(avg_times, 6)

        return v

    def update_recommendations(self):
        self._setif('game_recommendations', self.obj.game_recommendations.all() + self.also_playing())

    def also_playing(self):
        v = []

        existing = self.obj.game_recommendations

        general_data = self.soup_recommendations['main'].select('div div.pod').orelse([])
        for g in general_data:
            header = g.select_one('div.head h2').xtext(True).lower()
            if 'also playing' in header:
                typ = 'playing'
            elif 'also own' in header:
                typ = 'own'
            elif 'also love' in header:
                typ = 'love'
            else:
                self._log('-> Error: unknown header type for recommendation section: {}'.format(header), 'red')
                continue

            cols = g.find_all('div', {'class': 'table_col_5'}).orelse([])
            for c in cols:
                game_name = c.xtext()

                game_url = c.find('a').xget('href')
                if game_url is None:
                    self._log('-> Error: could not get link for recommendation {}'.format(game_name))
                    continue

                game_url = add_base_url(game_url)

                filter_dict = {
                    'typ': typ,
                    'url': game_url
                }

                filtered = existing.filter_by(**filter_dict)
                filtered_len = filtered.count()
                if filtered_len == 0:
                    rec_obj = Game_Recommendation(**filter_dict)
                    filter_dict['name'] = game_name
                    self._setdictif(filter_dict, rec_obj)

                    v.append(rec_obj)

        typ = 'related'
        rel_lis = self.soup_recommendations['main'].find('div', {'class': 'pod_related'}).select('div.body ul li').orelse([])
        for rel_li in rel_lis:
            rel_a = rel_li.find('a')

            game_name = rel_a.find('h3').xtext()

            game_url = rel_a.xget('href')
            if game_url is None:
                self._log('-> Error: could not get link for recommendation {}'.format(game_name))
                continue

            game_url = add_base_url(game_url)

            filter_dict = {
                'typ': typ,
                'url': game_url
            }

            filtered = existing.filter_by(**filter_dict)
            filtered_len = filtered.count()
            if filtered_len == 0:
                rec_obj = Game_Recommendation(**filter_dict)
                filter_dict['name'] = game_name
                self._setdictif(filter_dict, rec_obj)

                v.append(rec_obj)

        return v

    @staticmethod
    def commit():
        if echo_changes:
            get_changes()
        if do_commit:
            session.commit()


def get_row_data(row_data):
    td = row_data.find_all('td')

    game = td[1]
    name = game.xtext()

    ghref = game.find('a').xget('href')
    href = add_base_url(ghref)
    gid = get_game_id(ghref)

    glen = td[4].xtext()

    rdata = {
        'name': name,
        'gid': gid,
        'url': href,
        'glen': glen
    }

    return rdata


# todo add game description?


def db_get_exist_game(url):
    db_game = session.query(Game).filter_by(url=url).first()
    if db_game:
        if rerun is False:
            print('-> Exists, skipping insert ({})'.format(url))
            db_game = None
        else:
            print('-> Found entry, rerunning ({})'.format(url))
    else:
        db_game = Game(url=url)

    return db_game


def get_system_pages(s):
    system_num = system_dict[s][0]
    min_votes = system_dict[s][1]
    system_url = main_url.format(system=system_num, minvotes=min_votes)

    main_body = get_new_page(start_page, system_url)

    if num_pages:
        last_page = start_page + num_pages
    else:
        max_page = main_body.select_one('ul.paginate').xtext()
        if max_page:
            last_page = int(max_page.split('of ')[-1].split()[0])
        else:
            print(colored('=> Error: Could not get max pages, using 1', 'red'))
            last_page = 1

    print('\n= {} =\n'.format(s))
    print(' Total pages: {}'.format(last_page))
    games_parsed = 0

    for i in range(start_page, last_page):
        print('\n- Page {} -\n'.format(i + 1))
        if i == 0:
            page_data = main_body
        else:
            page_data = get_new_page(i, system_url)

        table_data = page_data.find('table', {'class': 'results'})
        if not table_data:
            print(colored('=> Error: could not parse main results page', 'red'))
            break

        row_data = table_data.find('tbody').find_all('tr').orelse([])
        for r in row_data:
            # noinspection PyTypeChecker
            if max_games is not None and games_parsed >= max_games:
                print('-> max games hit')
                return

            game_info = get_row_data(r)

            print(game_info['name'])

            game_len = game_info['glen']
            if game_len == '---' and s != 'iOS (iPhone/iPad)':
                print('-> Length is blank, skipping...')
                continue

            db_game = db_get_exist_game(game_info['url'])
            if db_game is None:
                continue

            if not db_game.name:
                db_game.name = game_info['name']
                db_game.gamefaqs_uid = game_info['gid']
                db_game.platform = s  # todo: put in db and make reference

            p = get_game_details(db_game)
            commit(p)

            games_parsed += 1


def get_one_page(url):
    url = add_base_url(url)

    db_game = db_get_exist_game(url)
    if db_game is None:
        return

    if not db_game.name:
        one_data = get_new_page(False, url)

        page_header = one_data.select_one('header.page-header')
        if not page_header:
            print(colored('=> Error: could not find game info', 'red'))
            return

        game_title = page_header.select_one('.page-title a').xtext()
        if not game_title:
            print(colored('=> Error: could not find game name', 'red'))
            return

        game_sys = page_header.select_one('ol.crumbs li.top-crumb a').xtext()
        if not game_sys:
            print(colored('=> Error: could not find game system', 'red'))
            return

        db_game.name = game_title
        db_game.platform = game_sys  # todo: put in db and make reference
        db_game.gamefaqs_uid = get_game_id(url)

    p = get_game_details(db_game)

    commit(p)


esrb_content_list = {}
for ro in session.query(Esrb_Content):
    esrb_content_list[ro.name] = ro

developer_list = {}
for ro in session.query(Developer):
    developer_list[ro.name] = ro


if __name__ == '__main__':
    if len(sys.argv) == 1:
        for syst in systems:
            get_system_pages(syst)

    elif len(sys.argv) == 2:
        game_urls = sys.argv[1].split(',')
        for g_url in game_urls:
            get_one_page(g_url)

    else:
        print('Too many arguments.')
        sys.exit(1)
