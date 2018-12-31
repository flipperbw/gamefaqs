#!/usr/bin/env python3

from datetime import datetime
# import json
import re
import sys
from time import sleep

# noinspection PyProtectedMember
from bs4 import BeautifulSoup, Comment, SoupStrainer
from dateutil.parser import parse as dateparse
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import requests


# -- GLOBALS --

base_url = 'https://gamefaqs.gamespot.com'
main_url = base_url + '/games/rankings?platform={system}&list_type=rate&dlc=0&min_votes={minvotes}'

sleep_delay = 0.5

# -------------

# PlayStation 4, Nintendo Switch, 3DS, DS, Wii, GameCube, PC, PlayStation 2, PlayStation, Nintendo 64, PSP, iOS (iPhone/iPad)
# Dreamcast, PlayStation 3, PlayStation Vita, Wii U, Xbox, Xbox 360, Xbox One
systems = ['Nintendo Switch', '3DS', 'DS', 'Wii', 'GameCube', 'PC',  'PlayStation 2', 'PlayStation', 'Nintendo 64', 'PSP', 'iOS (iPhone/iPad)']

start_page = 0
num_pages = False
# num_pages = 2
max_games = None
# max_games = 1

db = psycopg2.connect(host="localhost", user="brett", password="", database="gamefaqs")
cursor = db.cursor(cursor_factory=RealDictCursor)

strainer = SoupStrainer('div', {'id': 'content'})

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
session = requests.Session()
session.headers.update(headers)


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

    txt = session.get(href).text

    last_request = datetime.now()

    return txt


def clean_soup(soup):
    for script in soup(["script", "link", "style", "noscript", "meta"]):
        script.extract()
    for x in soup.find_all(text=lambda text: isinstance(text, Comment)):
        x.extract()
    return soup


def get_new_page(i, system_url):
    if i:
        sys_page = '{}&page={}'.format(system_url, i)
    else:
        sys_page = system_url

    d = wait_request(sys_page)
    soup = clean_soup(BeautifulSoup(d, 'lxml', parse_only=strainer))
    main_body = soup.find('div', {'class': 'main_content'})

    return main_body


def get_game_details(href):
    data_url = href + '/data'
    stats_url = href + '/stats'
    playing_url = href + '/playing'

    game_data = get_game_data(data_url)
    if game_data is False:
        return False
    game_stats = get_game_stats(stats_url)
    game_playing = get_game_playing(playing_url)

    all_details = {}
    all_details.update(game_data)
    all_details.update(game_stats)
    all_details.update(game_playing)

    return all_details


def get_game_id(href):
    return int(href.split('/')[-1].split('-')[0])


def add_base_url(href):
    if not href:
        return None
    if base_url not in href:
        href = base_url + href
    return href


re_comp = {
    'genre': re.compile(r'^Genre'),
    'developer': re.compile(r'^Developer'),
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


def get_game_data(data_url, dlc=False):
    g_data = get_new_page(False, data_url)

    if dlc is True:
        # - Header - #

        name = None
        game_title = g_data.select_one('header.page-header .page-title a')
        if game_title:
            name = game_title.get_text(strip=True)

        # - Upper Right Data - #

        more_data = g_data.find('div', {'class': 'pod_gameinfo'}).find('div', {'class': 'body'}).find('ul')

        release_date_str = None
        release_date_b = more_data.find('b', text=re_comp.get('release'))
        if release_date_b:
            release_date_a = release_date_b.find_next_sibling('a')
            release = release_date_a.get_text(strip=True)
            release = re.sub(re_comp.get('quarters'), '', release)

            if release.endswith(' »'):
                release = release.replace(' »', '').replace('  ', ' ')
            try:
                release_date = dateparse(release)
            except ValueError:
                print('-> Error: could not parse release date ({}) for DLC: {}'.format(release, data_url))
            else:
                release_date_str = '{d.month}/{d.day:02}/{d.year}'.format(d=release_date)

        metacritic = {}
        metacritic_b = more_data.find('li', {'class': 'metacritic'})
        if metacritic_b:
            metacritic['rating'] = int(metacritic_b.find('div', {'class': 'score'}).get_text(strip=True))
            metacritic_rev = metacritic_b.find('div', {'class': 'review_link'})
            metacritic_a = metacritic_rev.find('a')
            metacritic['url'] = metacritic_a.get('href')
            metacritic['reviews'] = int(metacritic_a.get_text(strip=True).replace('From ', '').replace(' reviews', '').replace(' review', ''))

        esrb_rating = None
        esrb_rating_b = more_data.find('span', {'class': 'esrb_logo'})
        if esrb_rating_b:
            esrb_rating = esrb_rating_b.get_text(strip=True).replace(' - ', '').replace(' -', '')

        # - Middle Data - #

        description = None
        desc_div = g_data.select_one('div.pod_gamespace .game_desc .desc')
        if desc_div:
            description = desc_div.get_text(strip=True)

        ownership_num = None
        rating = {}
        difficulty = {}
        playtime = {}
        completed = {}

        overview_data = g_data.select_one('form#js_mygames .pod_split .body')
        if overview_data:
            ownership_div = overview_data.select_one('.mygames_stats_own a')
            if ownership_div:
                ownership_num = int(ownership_div.get_text(strip=True).replace(' users', '').replace(' user', ''))

            rating_div = overview_data.select_one('.mygames_rate .gamerater_label')
            if rating_div:
                score_div = rating_div.select_one('.mygames_stats_rate a')
                if score_div:
                    rating['avg'] = float(score_div.get_text(strip=True).split(' ')[0])
                num_rate_div = rating_div.select_one('.rate')
                if num_rate_div:
                    rating['total'] = int(num_rate_div.get_text(strip=True).split(' ')[0])

            difficulty_div = overview_data.select_one('.mygames_diff .gamerater_label')
            if difficulty_div:
                difficulty_desc_div = difficulty_div.select_one('.mygames_stats_diff a')
                if difficulty_desc_div:
                    difficulty['desc'] = difficulty_desc_div.get_text(strip=True)
                difficulty_num_div = difficulty_div.select_one('.rate')
                if difficulty_num_div:
                    diff_num_spl = difficulty_num_div.get_text(strip=True).split(' ')
                    diff_num_spl = [v for v in diff_num_spl if v not in ('of', 'total', 'votes')]
                    if len(diff_num_spl) == 2:
                        difficulty['pct'] = round(float(diff_num_spl[0].replace('%', '')) / 100.0, 6)
                        difficulty['votes'] = int(diff_num_spl[1])

            playtime_div = overview_data.select_one('.mygames_time .gamerater_label')
            if playtime_div:
                playtime_len_a = playtime_div.select_one('.mygames_stats_time a')
                if playtime_len_a:
                    playtime_len_str = playtime_len_a.get_text(strip=True).replace(' hours', '').replace(' hour', '')
                    if '80+' in playtime_len_str:
                        playtime_len_str = 90
                    try:
                        playtime_len = float(playtime_len_str)
                    except ValueError:
                        print('-> Error: could not convert playtime for DLC: {}'.format(data_url))
                    else:
                        playtime['len'] = playtime_len
                playtime_num_div = playtime_div.select_one('.rate')
                if playtime_num_div:
                    playtime['total'] = int(playtime_num_div.get_text(strip=True).split(' ')[0])

            completed_div = overview_data.select_one('.mygames_play .gamerater_label .rate')
            if completed_div:
                complted_div_spl = completed_div.get_text(strip=True).split(' ')
                complted_div_spl = [v for v in complted_div_spl if v not in ('of', 'total', 'votes')]
                if len(complted_div_spl) == 2:
                    completed['pct'] = round(float(complted_div_spl[0].replace('%', '')) / 100.0, 6)
                    completed['votes'] = int(complted_div_spl[1])

        game_id = get_game_id(data_url)

        exp_data = {
            'href': data_url,
            'game_id': game_id,
            'name': name,
            'release_date': release_date_str,
            'metacritic': metacritic,
            'esrb_rating': esrb_rating,
            'description': description,
            'ownership_num': ownership_num,
            'rating': rating,
            'difficulty': difficulty,
            'playtime': playtime,
            'completed': completed
        }

        return exp_data

    # - More Data - #

    more_data = g_data.find('div', {'class': 'pod_gameinfo'}).find('div', {'class': 'body'}).find('ul')

    expansion_b = more_data.find('b', text=re_comp.get('expansion'))
    if expansion_b:
        print('=> Error: is DLC, skipping...')
        return False

    boxart = {}
    boxart_b = more_data.find('img', {'class': 'boxshot'})
    if boxart_b:
        thumb_src = boxart_b.get('src')
        boxart['thumb'] = thumb_src
        boxart['front'] = thumb_src.replace('thumb.', 'front.')  # todo: check if exists
    boxart_c = more_data.find('a', {'class': 'imgboxart'})
    if boxart_c:
        bhref = boxart_c.get('href')
        boxart['all'] = add_base_url(bhref)

    franchise = {}
    franchise_b = more_data.find('b', text=re_comp.get('franchise'))
    if franchise_b:
        franchise_a = franchise_b.find_next_sibling('a')
        franchise['name'] = franchise_a.get_text(strip=True)
        fhref = franchise_a.get('href')
        franchise['url'] = add_base_url(fhref)

    # todo get system aliases
    alsoon = []
    alsoon_b = more_data.find('b', text=re_comp.get('also'))
    if alsoon_b:
        alsoon_as = alsoon_b.find_next_siblings()
        alsoon = [a.get_text(strip=True) for a in alsoon_as]

    aka = None
    aka_b = more_data.find('b', text=re_comp.get('aka'))
    if aka_b:
        aka_a = aka_b.find_next_sibling('i')
        aka = aka_a.get_text(strip=True)
        # todo: multiple?
        # todo: combine this somehow

    metacritic = {}
    metacritic_b = more_data.find('li', {'class': 'metacritic'})
    if metacritic_b:
        metacritic['rating'] = int(metacritic_b.find('div', {'class': 'score'}).get_text(strip=True))
        metacritic_rev = metacritic_b.find('div', {'class': 'review_link'})
        metacritic_a = metacritic_rev.find('a')
        metacritic['url'] = metacritic_a.get('href')
        metacritic['reviews'] = int(metacritic_a.get_text(strip=True).replace('From ', '').replace(' reviews', '').replace(' review', ''))

    # - Data - #

    general_data = g_data.find('div', {'class': 'pod_titledata'}).find('div', {'class': 'body'}).find('dl')

    genre = None
    genre_b = general_data.find('dt', text=re_comp.get('genre'))
    if genre_b:
        genre = genre_b.find_next_sibling('dd').get_text(strip=True)

    developers = {}
    developer_bs = general_data.find_all('dt', text=re_comp.get('developer'))
    for developer_b in developer_bs:
        developer_data = developer_b.find_next_sibling('dd')

        developer_name = developer_data.get_text(strip=True)

        developer_url = None
        dev_a = developer_data.find('a')
        if dev_a:
            dhref = dev_a.get('href')
            developer_url = add_base_url(dhref)

        developers[developer_name] = developer_url

    esrb_description = []
    esrb_b = general_data.find('dt', text=re_comp.get('esrb'))
    if esrb_b:
        esrb_description = [ed.strip() for ed in esrb_b.find_next_sibling('dd').get_text(strip=True).split(',')]

    local_players = None
    local_players_b = general_data.find('dt', text=re_comp.get('local_players'))
    if local_players_b:
        local_players = local_players_b.find_next_sibling('dd').get_text(strip=True)

    multi_players = None
    multi_players_b = general_data.find('dt', text=re_comp.get('multi_players'))
    if multi_players_b:
        multi_players = multi_players_b.find_next_sibling('dd').get_text(strip=True)

    # - Releases - #

    release_data = g_data.find('table', {'class': 'contrib'}).find('tbody').find_all('tr')

    releases = []
    release_us = {}

    for r in release_data[::2]:
        r_title = r.find('td', {'class': 'ctitle'}).get_text(strip=True)

        rnext = r.nextSibling
        region = rnext.find('td', {'class': 'cregion'}).get_text(strip=True)
        publisher_b = rnext.find('td', {'class': "datacompany"}).find('a')
        publisher = publisher_b.get_text(strip=True)
        publisher_url = add_base_url(publisher_b.get('href'))
        product_ids = rnext.find_all('td', {'class': "datapid"})
        product_id = product_ids[0].get_text(strip=True)
        distribution = product_ids[1].get_text(strip=True)
        release_date_text = rnext.find('td', {'class': "cdate"}).get_text(strip=True)
        release_date_text = re.sub(re_comp.get('quarters'), '', release_date_text)
        try:
            release_date = dateparse(release_date_text)
        except ValueError:
            print('-> Error: could not parse release date ({})'.format(release_date_text))
            release_date_str = None
        else:
            release_date_str = '{d.month}/{d.day:02}/{d.year}'.format(d=release_date)

        esrb_rating_r = rnext.find('td', {'class': "datarating"}).get_text(strip=True)

        this_release = {
            'title': r_title,
            'region': region,
            'publisher': {
                'name': publisher,
                'url': publisher_url
            },
            'product_id': product_id,
            'distribution_id': distribution,
            'release_date': release_date_str,
            'esrb_rating': esrb_rating_r
        }

        releases.append(this_release)

        if not release_us and region == 'US' and all([publisher, product_id, release_date_str]):
            release_us = this_release.copy()
            del release_us['region']

    # - Append all the data -#

    game_data = {
        'genre': genre,
        'developers': developers,
        'esrb_descriptions': esrb_description,
        'boxart': boxart,
        'franchise': franchise,
        'also_on': alsoon,
        'metacritic': metacritic,
        'release_us': release_us,
        'releases': releases,
        'local_players': local_players,
        'multi_players': multi_players,
        'aka': aka
    }

    # - Expansions - #

    expansion_data = {}
    expansion_div = g_data.find('div', {'id': 'dlc'})
    if expansion_div:
        expansion_trs = expansion_div.select('.body table tr')
        for tr in expansion_trs:
            cell_data = tr.find_all('td')[0]
            cell_a = cell_data.find('a')
            game_href = add_base_url(cell_a.get('href'))
            game_name = cell_a.get_text(strip=True)

            expansion_info = get_game_data(game_href, dlc=True)

            expansion_data[game_name] = expansion_info

    # - Return all - #

    game_data_all = {
        'data': game_data,
        'expansions': expansion_data
    }

    return game_data_all


def get_game_stats(data_url):
    # todo: ADD INSTDEV to all
    g_stats = get_new_page(False, data_url)

    stats_data = g_stats.find_all('figure', {'class': 'mygames_section bar'})
    if len(stats_data) != 5:
        print('-> Error: could not find stats: {}'.format(len(stats_data)))
        return {'stats': {}}

    # - Ratings -#

    rating_section = stats_data[0]
    total_ratings = rating_section.find('figcaption').get_text(strip=True)
    num_ratings_tot = int(total_ratings[total_ratings.index(":") + 2: total_ratings.index("-") - 1])

    ratings = {}
    avg_rating = 0.0
    terrible_pct = 0.0
    amazing_pct = 0.0

    tbody = rating_section.find('tbody')
    for i in range(1, 11):
        stars = i / 2.0
        dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
        if dataspan:
            datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%', '')) / 100.0
        else:
            datapoint = 0.0

        conv_rating = datapoint * stars

        ratings[str(stars)] = round(datapoint, 6)
        avg_rating += conv_rating

        if i in (1, 2):
            terrible_pct += datapoint
        elif i in (9, 10):
            amazing_pct += datapoint

    avg_rating = round(avg_rating, 6)
    terrible_pct = round(terrible_pct, 6)
    amazing_pct = round(amazing_pct, 6)
    diff_pct = round(amazing_pct - terrible_pct, 6)

    ratings['avg'] = avg_rating
    ratings['total'] = num_ratings_tot
    ratings['amazing_pct'] = amazing_pct
    ratings['terrible_pct'] = terrible_pct
    ratings['diff_pct'] = diff_pct

    # - Ownership -#

    own_section = stats_data[1]
    total_owns = own_section.find('figcaption').get_text(strip=True)
    num_owns_tot = int(total_owns[total_owns.index(":") + 2:])

    ownership = {'num_owners': num_owns_tot}

    # - Play -#

    play_section = stats_data[2]
    total_plays = play_section.find('figcaption').get_text(strip=True)
    num_plays_tot = int(total_plays[total_plays.index(":") + 2: total_plays.index("-") - 1])

    plays = {}
    play_complete = 0.0
    play_incomplete = 0.0
    play_avg = 0.0

    tbody = play_section.find('tbody')
    # todo check if some of these are missing
    for i in range(1, 6):
        dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
        if dataspan:
            datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%', '')) / 100.0
        else:
            datapoint = 0.0

        play_avg += (datapoint * i)

        k = 'unk'
        if i == 1:
            k = 'once'
        elif i == 2:
            k = 'some'
        elif i == 3:
            k = 'half'
        elif i == 4:
            k = 'finish'
        elif i == 5:
            k = 'platinum'

        plays[k] = round(datapoint, 6)

        if i in (1, 2, 3):
            play_incomplete += datapoint
        elif i in (4, 5):
            play_complete += datapoint

    plays['total'] = num_plays_tot
    plays['complete_pct'] = round(play_complete, 6)
    plays['incomplete_pct'] = round(play_incomplete, 6)
    plays['avg'] = round(play_avg, 6)

    # - Difficulty - #

    difficulty_section = stats_data[3]
    total_difficulty = difficulty_section.find('figcaption').get_text(strip=True)
    num_difficulty_tot = int(total_difficulty[total_difficulty.index(":") + 2: total_difficulty.index("-") - 1])

    difficulty = {}
    avg_difficulty = 0.0

    tbody = difficulty_section.find('tbody')
    for i in range(1, 6):
        dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
        if dataspan:
            datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%', '')) / 100.0
        else:
            datapoint = 0.0

        avg_difficulty += (datapoint * i)

        k = 'unk'
        if i == 1:
            k = 'easy'
        elif i == 2:
            k = 'fine'
        elif i == 3:
            k = 'moderate'
        elif i == 4:
            k = 'hard'
        elif i == 5:
            k = 'extreme'

        difficulty[k] = round(datapoint, 6)

    difficulty['total'] = num_difficulty_tot
    difficulty['avg'] = round(avg_difficulty, 6)

    # - Time - #

    times_section = stats_data[4]
    total_times = times_section.find('figcaption').get_text(strip=True)
    num_times_tot = int(total_times[total_times.index(":") + 2: total_times.index("-") - 1])

    times = {}
    avg_times = 0.0

    tbody = times_section.find('tbody')
    for i in range(1, 11):
        dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
        if dataspan:
            datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%', '')) / 100.0
        else:
            datapoint = 0.0

        hours = 0.0
        if i == 1:
            hours = 0.5
        elif i == 2:
            hours = 1.0
        elif i == 3:
            hours = 2.0
        elif i == 4:
            hours = 4.0
        elif i == 5:
            hours = 8.0
        elif i == 6:
            hours = 12.0
        elif i == 7:
            hours = 20.0
        elif i == 8:
            hours = 40.0
        elif i == 9:
            hours = 60.0
        elif i == 10:
            hours = 90.0

        times[str(hours)] = round(datapoint, 6)
        avg_times += (datapoint * hours)

    times['avg'] = round(avg_times, 6)
    times['total'] = num_times_tot

    # - all - #

    stats = {
        'ratings': ratings,
        'ownership': ownership,
        'plays': plays,
        'difficulty': difficulty,
        'times': times
    }

    stats_all = {'stats': stats}

    return stats_all


def get_game_playing(data_url):
    g_data = get_new_page(False, data_url)

    also_playing = []
    also_own = []
    also_love = []

    general_data = g_data.find_all('div', {'class': 'pod'})

    for g in general_data:
        cols = g.find_all('div', {'class': 'table_col_5'})
        header = g.find('div', {'class': 'head'}).find('h2').text.lower()
        for c in cols:
            chref = c.find('a').get('href')
            game_url = add_base_url(chref)
            game_name = c.get_text(strip=True)
            cols_list = {
                'name': game_name,
                'url': game_url
            }

            if 'also playing' in header:
                also_playing.append(cols_list)
            elif 'also own' in header:
                also_own.append(cols_list)
            elif 'also love' in header:
                also_love.append(cols_list)

    related_games = []
    related_data = g_data.find('div', {'class': 'pod_related'})
    if related_data:
        rel_lis = related_data.select('div.body ul li')
        for rel_li in rel_lis:
            rel_a = rel_li.find('a')
            rel_info = {
                'name': rel_a.find('h3').get_text(strip=True),
                'url': add_base_url(rel_a.get('href'))
            }
            related_games.append(rel_info)

    playing = {
        'recommendations': {
            'playing': also_playing,
            'own': also_own,
            'love': also_love,
            'related': related_games
        }
    }

    return playing


def get_row_data(row_data):
    td = row_data.find_all('td')

    game = td[1]
    name = game.get_text(strip=True)

    ghref = game.find('a').get('href')
    href = add_base_url(ghref)
    gid = get_game_id(ghref)

    glen = td[4].get_text(strip=True)

    rdata = {
        'name': name,
        'gid': gid,
        'href': href,
        'glen': glen
    }

    return rdata


def parse_game_info(game_info, s=None):
    game_href = game_info.get('href')
    game_len = game_info.get('glen')
    # todo description?

    if game_len == '---' and s != 'iOS (iPhone/iPad)':
        print('-> Length is blank, skipping...')
        return False

    del game_info['glen']
    game_details = get_game_details(game_href)
    if game_details is False:
        return False
    game_info.update(game_details)
    game_info['platform'] = s  # todo: remove and also name

    # print(json.dumps(game_info, indent=4, sort_keys=True))

    return game_info


def update_game(i):
    cursor.execute('SELECT 1 FROM game WHERE href = %s', (i['href'],))
    if cursor.rowcount != 0:
        print('-> Exists, skipping insert.')
        return

    # - game - #

    col_list = [
        'name', 'href', 'gamefaqs_uid', 'platform', 'genre', 'aka', 'franchise_name', 'franchise_url', 'local_players', 'multi_players', 'metacritic_url', 'boxart_thumb', 'boxart_front', 'boxart_all', 'also_on', 'release_distribution_uid', 'release_product_uid', 'release_publisher_name', 'release_publisher_url', 'release_date', 'release_esrb_rating', 'release_title'
    ]

    da = i.get('data', {})

    _franchise = da.get('franchise', {})
    _boxart = da.get('boxart', {})
    _release_us = da.get('release_us', {})

    inputs = [
        i['name'], i['href'], i['gid'], i['platform'], da['genre'], da['aka'], _franchise.get('name'), _franchise.get('url'), da['local_players'], da['multi_players'], da.get('metacritic', {}).get('url'), _boxart.get('thumb'), _boxart.get('front'), _boxart.get('all'), da['also_on'], _release_us.get('distribution_id'), _release_us.get('product_id'), _release_us.get('publisher', {}).get('name'), _release_us.get('publisher', {}).get('url'), _release_us.get('release_date'), _release_us.get('esrb_rating'), _release_us.get('title')
    ]

    input_s = ', '.join(['%s' for _ in col_list])

    _game = sql.SQL('''
        INSERT INTO game ({}) VALUES ({}) RETURNING id
    '''.format('{}', input_s)).format(sql.SQL(', ').join([sql.Identifier(x) for x in col_list]))

    cursor.execute(_game, inputs)
    game_id = cursor.fetchone()['id']

    # - game_stat - #

    col_list = [
        'game_id', 'owners', 'metacritic_rating', 'metacritic_reviews', 'difficulty_votes', 'difficulty_avg', 'difficulty_easy', 'difficulty_fine', 'difficulty_moderate', 'difficulty_hard', 'difficulty_extreme', 'progress_votes', 'progress_avg', 'progress_pct_complete', 'progress_pct_incomplete', 'progress_pct_platinum', 'progress_pct_finish', 'progress_pct_half', 'progress_pct_some', 'progress_pct_once', 'rating_votes', 'rating_avg', 'rating_half', 'rating_one', 'rating_one_half', 'rating_two', 'rating_two_half', 'rating_three', 'rating_three_half', 'rating_four', 'rating_four_half', 'rating_five', 'rating_pct_amazing', 'rating_pct_terrible', 'rating_pct_diff', 'playtime_votes', 'playtime_avg', 'playtime_pct_halfhour', 'playtime_pct_onehour', 'playtime_pct_twohour', 'playtime_pct_four_hour', 'playtime_pct_eight_hour', 'playtime_pct_twelve_hour', 'playtime_pct_twenty_hour', 'playtime_pct_forty_hour', 'playtime_pct_sixty_hour', 'playtime_pct_ninety_hour'
    ]

    st = i.get('stats', {})

    _diff = st.get('difficulty', {})
    _prog = st.get('plays', {})
    _rate = st.get('ratings', {})
    _play = st.get('times', {})

    inputs = [
        game_id, st.get('ownership', {}).get('num_owners'), da.get('metacritic', {}).get('rating'), da.get('metacritic', {}).get('reviews'), _diff.get('total'), _diff.get('avg'), _diff.get('easy'), _diff.get('fine'), _diff.get('moderate'), _diff.get('hard'), _diff.get('extreme'), _prog.get('total'), _prog.get('avg'), _prog.get('complete_pct'), _prog.get('incomplete_pct'), _prog.get('platinum'), _prog.get('finish'), _prog.get('half'), _prog.get('some'), _prog.get('once'), _rate.get('total'), _rate.get('avg'), _rate.get('0.5'), _rate.get('1.0'), _rate.get('1.5'), _rate.get('2.0'), _rate.get('2.5'), _rate.get('3.0'), _rate.get('3.5'), _rate.get('4.0'), _rate.get('4.5'), _rate.get('5.0'), _rate.get('amazing_pct'), _rate.get('terrible_pct'), _rate.get('diff_pct'), _play.get('total'), _play.get('avg'), _play.get('0.5'), _play.get('1.0'), _play.get('2.0'), _play.get('4.0'), _play.get('8.0'), _play.get('12.0'), _play.get('20.0'), _play.get('40.0'), _play.get('60.0'), _play.get('90.0')
    ]

    input_s = ', '.join(['%s' for _ in col_list])

    _gamestat = sql.SQL('''
        INSERT INTO game_stat ({}) VALUES ({})
    '''.format('{}', input_s)).format(sql.SQL(', ').join([sql.Identifier(x) for x in col_list]))

    cursor.execute(_gamestat, inputs)

    # - game_esrb_content - #

    eds = da.get('esrb_descriptions')
    for ed in eds:
        ed_id = esrb_content_list.get(ed)
        if not ed_id:
            cursor.execute('INSERT INTO esrb_content (name) VALUES (%s) RETURNING id', (ed,))
            ed_id = cursor.fetchone()['id']
            esrb_content_list[ed] = ed_id

        cursor.execute('INSERT INTO game_esrb_content (game_id, esrb_content_id) VALUES (%s, %s)', (game_id, ed_id))

    # - game_developer - #

    devs = da.get('developers')
    for dev, dev_url in devs.items():
        dev_id = developer_list.get(dev)
        if not dev_id:
            cursor.execute('INSERT INTO developer (name, url) VALUES (%s, %s) RETURNING id', (dev, dev_url))
            dev_id = cursor.fetchone()['id']
            developer_list[dev] = dev_id

        cursor.execute('INSERT INTO game_developer (game_id, developer_id) VALUES (%s, %s)', (game_id, dev_id))

    # - game_release - #

    col_list = [
        'game_id', 'region', 'distribution_uid', 'product_uid', 'publisher_name', 'publisher_url', 'release_date', 'esrb_rating', 'title'
    ]

    input_s = ', '.join(['%s' for _ in col_list])

    _game_release = sql.SQL('''
        INSERT INTO game_release ({}) VALUES ({})
    '''.format('{}', input_s)).format(sql.SQL(', ').join([sql.Identifier(x) for x in col_list]))

    _releases = da.get('releases')
    for rele in _releases:
        inputs = [
            game_id, rele.get('region'), rele.get('distribution_id'), rele.get('product_id'), rele.get('publisher', {}).get('name'), rele.get('publisher', {}).get('url'), rele.get('release_date'), rele.get('esrb_rating'), rele.get('title')
        ]
        cursor.execute(_game_release, inputs)

    # - game_expansion - #

    col_list = [
        'game_id', 'gamefaqs_uid', 'name', 'description', 'esrb_rating', 'href', 'metacritic_rating', 'metacritic_reviews', 'metacritic_url', 'release_date', 'owners', 'rating_score', 'rating_votes', 'completed_pct', 'completed_votes', 'difficulty_desc', 'difficulty_pct', 'difficulty_votes', 'playtime_hours', 'playtime_votes'
    ]

    input_s = ', '.join(['%s' for _ in col_list])

    _game_expansion = sql.SQL('''
        INSERT INTO game_expansion ({}) VALUES ({})
    '''.format('{}', input_s)).format(sql.SQL(', ').join([sql.Identifier(x) for x in col_list]))

    ex = i.get('expansions', {})
    for exp, exp_val in ex.items():
        _metacritic = exp_val.get('metacritic', {})
        _rating = exp_val.get('rating', {})
        _completed = exp_val.get('completed', {})
        _difficulty = exp_val.get('difficulty', {})
        _playtime = exp_val.get('playtime', {})
        inputs = [
            game_id, exp_val.get('game_id'), exp, exp_val.get('description'), exp_val.get('esrb_rating'), exp_val.get('href'), _metacritic.get('rating'), _metacritic.get('reviews'), _metacritic.get('url'), exp_val.get('release_date'), exp_val.get('ownership_num'), _rating.get('avg'), _rating.get('total'), _completed.get('pct'), _completed.get('votes'), _difficulty.get('desc'), _difficulty.get('pct'), _difficulty.get('votes'), _playtime.get('len'), _playtime.get('total')
        ]

        cursor.execute(_game_expansion, inputs)

    # - game_recommendation - #

    col_list = [
        'game_id', 'typ', 'name', 'url'
    ]

    input_s = ', '.join(['%s' for _ in col_list])

    _game_recommendation = sql.SQL('''
        INSERT INTO game_recommendation ({}) VALUES ({})
    '''.format('{}', input_s)).format(sql.SQL(', ').join([sql.Identifier(x) for x in col_list]))

    recs = i.get('recommendations', {})
    for rec, d in recs.items():
        for rec_item in d:
            inputs = [
                game_id, rec, rec_item.get('name'), rec_item.get('url')
            ]
            cursor.execute(_game_recommendation, inputs)

    db.commit()


def get_system_pages(s):
    system_num = system_dict[s][0]
    min_votes = system_dict[s][1]
    system_url = main_url.format(system=system_num, minvotes=min_votes)

    main_body = get_new_page(start_page, system_url)

    if num_pages:
        last_page = start_page + num_pages
    else:
        max_page = main_body.select_one('ul.paginate')
        if max_page:
            last_page = int(max_page.get_text().strip().split('of ')[-1].split()[0])
        else:
            print('=> Error: Could not get max pages, using 1')
            last_page = 1

    print('= {} ='.format(s))
    print(' Total pages: {}'.format(last_page))
    games_parsed = 0

    for i in range(start_page, last_page):
        print('- Page {} -'.format(i + 1))
        if i == 0:
            page_data = main_body
        else:
            page_data = get_new_page(i, system_url)

        table_data = page_data.find('table', {'class': 'results'})
        if not table_data:
            print('=> Error: could not parse main results page')
            break

        row_data = table_data.find('tbody').find_all('tr')
        for r in row_data:
            # noinspection PyTypeChecker
            if max_games is not None and games_parsed > max_games:
                print('-> max games hit')
                return

            game_info = get_row_data(r)

            print(game_info['name'])

            cursor.execute('SELECT 1 FROM game WHERE href = %s', (game_info['href'],))
            if cursor.rowcount != 0:
                print('-> Exists, skipping insert.')
                continue

            game_full_info = parse_game_info(game_info, s)
            if game_full_info is False:
                continue

            update_game(game_full_info)

            games_parsed += 1


def get_one_page(url):
    url = add_base_url(url)

    cursor.execute('SELECT 1 FROM game WHERE href = %s', (url,))
    if cursor.rowcount != 0:
        print('-> Exists, skipping insert ({})'.format(url))
        return

    one_data = get_new_page(False, url)

    page_header = one_data.select_one('header.page-header')
    if not page_header:
        print('=> Error: could not find game info')
        return False

    gid = get_game_id(url)

    game_title = page_header.select_one('.page-title a')
    if not game_title:
        print('=> Error: could not find game name')
        return False

    name = game_title.get_text(strip=True)

    game_sys = page_header.select_one('ol.crumbs li.top-crumb a')
    if not game_sys:
        print('=> Error: could not find game system')
        return False

    s = game_sys.get_text(strip=True)

    game_info = {
        'name': name,
        'gid': gid,
        'href': url,
        'glen': None
    }

    game_full_info = parse_game_info(game_info, s)
    if game_full_info is False:
        return

    update_game(game_full_info)


esrb_content_list = {}
cursor.execute('SELECT id, name FROM esrb_content')
for ro in cursor.fetchall():
    esrb_content_list[ro['name']] = ro['id']

developer_list = {}
cursor.execute('SELECT id, name FROM developer')
for ro in cursor.fetchall():
    developer_list[ro['name']] = ro['id']


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
