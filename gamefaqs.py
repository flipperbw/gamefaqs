#!/usr/local/bin/python2.7
# -*- coding: utf-8 -*-

import re
from time import sleep
import requests
from bs4 import BeautifulSoup
import json

base_url = 'http://www.gamefaqs.com'
main_url = base_url + '/games/rankings?platform={system}&list_type=rate&dlc=1&min_votes={minvotes}'					 

systems = ['Wii U'] #ps4, 3ds, ds, wii, pc, gamecube, ps3, ps2, ps, psp, ios, vita, wiiu
start_page = 0
num_pages = False #1

system_dict = {
	"3DS": ("116", "2"),
	"Android": ("106", "2"),
	"Arcade Games": ("2", "2"),
	"BlackBerry": ("107", "2"),
	"Commodore 64": ("24", "2"),
	"Dreamcast": ("67", "2"),
	"DS": ("108", "1"),
	"Game Boy": ("59", "2"),
	"Game Boy Advance": ("91", "2"),
	"Game Boy Color": ("57", "2"),
	"GameCube": ("99", "2"),
	"GameGear": ("62", "2"),
	"Genesis": ("54", "2"),
	"iOS": ("112", "2"),
	"Macintosh": ("27", "2"),
	"Mobile": ("85", "2"),
	"NES": ("41", "2"),
	"Nintendo 64": ("84", "2"),
	"Online/Browser": ("69", "2"),
	"Palm OS Classic": ("96", "2"),
	"Palm webOS": ("97", "2"),
	"PC": ("19", "1"),
	"PlayStation": ("78", "1"),
	"PlayStation 2": ("94", "1"),
	"PlayStation 3": ("113", "1"),
	"PlayStation 4": ("120", "2"),
	"PlayStation Vita": ("117", "2"),
	"PSP": ("109", "1"),
	"Saturn": ("76", "2"),
	"Super Nintendo": ("63", "2"),
	"Vectrex": ("34", "2"),
	"Wii": ("114", "1"),
	"Wii U": ("118", "2"),
	"Windows Mobile": ("88", "2"),
	"Xbox": ("98", "2"),
	"Xbox 360": ("111", "1"),
	"Xbox One": ("121", "2")
}


def get_game_details(href):
	data_url = href + '/data'
	stats_url = href + '/stats'
	playing_url = href + '/playing'
	
	game_data = get_game_data(data_url)
	game_stats = get_game_stats(stats_url)
	game_playing = get_game_playing(playing_url)
	
	all_details = {}
	all_details.update(game_data)
	all_details.update(game_stats)
	all_details.update(game_playing)
	
	return all_details


def get_new_page(i, system_url):
	if i:
		sys_page = system_url + '&page={}'.format(i)
	else:
		sys_page = system_url
	
	sleep(0.5)
	q = requests.get(sys_page)
	s = BeautifulSoup(q.text)
	main_body = s.find('div', id='content').find('div', {'class': 'main_content'})
	return main_body


def get_system_pages(s):
	system_num = system_dict[s][0]
	min_votes = system_dict[s][1]
	system_url = main_url.format(system=system_num, minvotes=min_votes)

	main_body = get_new_page(start_page, system_url)
	
	if num_pages:
		last_page = start_page + num_pages
	else:
		max_page = main_body.find('select', id='pagejump')
		if max_page:
			last_page = int(max_page.find_all('option')[-1].get_text(strip=True))
		else:
			last_page = 1
	game_data = []
	for i in range(start_page, last_page):
		if (i==0):
			page_data = main_body
		else:
			page_data = get_new_page(i, system_url)
		
		row_data = page_data.find('table', {'class': 'results'}).find('tbody').find_all('tr')
		for r in row_data:
			game_info = get_row_data(r)
			game_href = game_info.get('href')
			game_len = game_info.get('glen')
			
			if filter(lambda inlist: inlist['href'] == game_href, game_data) or (game_len == '---' and system_num != '112'):
				#print('----->   skipping...') #remove
				pass
			else:
				del(game_info['glen'])
				game_details = get_game_details(game_href)
				game_info.update(game_details)
				game_info['platform'] = s
				
				#print(game_info)
				print(json.dumps(game_info))
				
				game_data.append(game_info)

	return game_data


def get_row_data(row_data):
	td = row_data.find_all('td')

	game = td[1]
	name = game.get_text(strip=True)
	href = base_url + game.find('a').get('href')
	
	glen = td[4].get_text(strip=True)
	
	rdata = {}
	rdata['name'] = name
	rdata['href'] = href
	rdata['glen'] = glen
	
	return rdata


def get_game_data(data_url):
	g_data = get_new_page(False, data_url)
	
	# - Data - #
	general_data = g_data.find('div', {'class': 'pod_titledata'}).find('div', {'class': 'body'}).find('dl')
	
	genre = None
	genre_b = general_data.find('dt', text=re.compile('Genre'))
	if genre_b:
		genre = genre_b.find_next_sibling('dd').get_text(strip=True)
		
	developer = None
	developer_url = None
	developer_b = general_data.find('dt', text=re.compile('^Developer'))
	if developer_b:
		developer_data = developer_b.find_next_sibling('dd')
		developer = developer_data.get_text(strip=True)
		dev_a = developer_data.find('a')
		if dev_a:
			developer_url = base_url + dev_a.get('href')

	esrb_description = None
	esrb_b = general_data.find('dt', text=re.compile('^ESRB Descriptor'))
	if esrb_b:
		esrb_description = esrb_b.find_next_sibling('dd').get_text(strip=True)  #TODO array?

	#- More Data - #
	
	more_data = g_data.find('div', {'class': 'pod_gameinfo'}).find('div', {'class': 'body'}).find('ul')

	boxart = None
	boxart_all = None
	boxart_b = more_data.find('img', {'class': 'boxshot'})
	if boxart_b:
		boxart = boxart_b.get('src')
		#boxart = boxart_t.replace('thumb.', 'front.')
	boxart_c = more_data.find('a', {'class': 'imgboxart'})
	if boxart_c:
		boxart_all = base_url + boxart_c.get('href')
	
	release = None
	release_b = more_data.find('b', text=re.compile('^Release:'))
	if release_b:
		release_a = release_b.find_next_sibling('a')
		release_t = release_a.get_text(strip=True).encode('utf-8')
		if release_t.endswith(' »'):
			release = release_t.replace(' »', '')
		else:
			release = release_t

	"""
	same as above for Also Known As
	<i>
	"""
	
	franchise = None
	franchise_url = None
	franchise_b = more_data.find('b', text=re.compile('^Franchise:'))
	if franchise_b:
		franchise_a = franchise_b.find_next_sibling('a')
		franchise = franchise_a.get_text(strip=True)
		franchise_url = base_url + franchise_a.get('href')
	
	alsoon = None
	alsoon_b = more_data.find('b', text=re.compile('^Also on:'))
	if alsoon_b:
		alsoon_as = alsoon_b.find_next_siblings()
		alsoon = [a.get_text(strip=True) for a in alsoon_as]

	expansion = None
	expansion_url = None
	expansion_b = more_data.find('b', text=re.compile('^Expansion for:'))
	if expansion_b:
		expansion_a = expansion_b.find_next_sibling('a')
		expansion = expansion_a.get_text(strip=True)
		expansion_url = base_url + expansion_a.get('href')

	"""
	esrb_rating = None
	esrb_rating_b = more_data.find('span', {'class': 'esrb_logo'})
	if esrb_rating_b:
		esrb_rating_t = esrb_rating_b.get_text(strip=True)
		if esrb_rating_t.endswith(' - '):
			esrb_rating = esrb_rating_t[:-3]
		else:
			esrb_rating = esrb_rating_t
	"""
	
	metacritic_rating = None  #TODO is this right?
	metacritic_reviews = None
	metacritic_url = None
	metacritic_b = more_data.find('li', {'class': 'metacritic'})
	if metacritic_b:
		metacritic_rating = int(metacritic_b.find('div', {'class': 'score'}).get_text(strip=True))
		metacritic_rev = metacritic_b.find('div', {'class': 'review_link'})
		metacritic_a = metacritic_rev.find('a')
		metacritic_reviews = int(metacritic_a.get_text(strip=True).replace('From ','').replace(' reviews', ''))
		metacritic_url = metacritic_a.get('href')
	
	#- Releases - #
	
	release_data = g_data.find('table', {'class': 'contrib'}).find('tbody').find_all('tr')

	releases = []
	
	found_us = False
	publisher_us = None
	release_date_us = None
	esrb_rating_us = None
	
	for r in release_data[::2]:
		this_release = {}
		r_title = r.find('td', {'class': 'ctitle'}).get_text(strip=True)
		
		rnext = r.nextSibling
		region = rnext.find('td', {'class': 'cregion'}).get_text(strip=True)
		publisher_b = rnext.find('td', {'class': "datacompany"}).find('a')
		publisher = publisher_b.get_text(strip=True)
		publisher_url = publisher_b.get('href')
		product_ids = rnext.find_all('td', {'class': "datapid"})
		product_id = product_ids[0].get_text(strip=True)
		distribution = product_ids[1].get_text(strip=True)
		release_date = rnext.find('td', {'class': "cdate"}).get_text(strip=True)
		esrb_rating = rnext.find('td', {'class': "datarating"}).get_text(strip=True)
		
		this_release['title'] = r_title
		this_release['region'] = region
		this_release['publisher'] = publisher
		this_release['publisher_url'] = publisher_url
		this_release['product_id'] = product_id
		this_release['distribution'] = distribution
		this_release['release_date'] = release_date
		this_release['esrb_rating'] = esrb_rating
		
		releases.append(this_release)
		
		if not found_us and region == 'US':
			found_us = True
			publisher_us = publisher
			release_date_us = release_date
			esrb_rating_us = esrb_rating
		
	
	#- Append all the data -#
	
	game_data = {}
	
	game_data['genre'] = genre
	game_data['developer'] = developer
	game_data['developer_url'] = developer_url
	game_data['esrb_description'] = esrb_description
	game_data['boxart'] = boxart
	game_data['boxart_all'] = boxart_all
	game_data['franchise'] = franchise
	game_data['franchise_url'] = franchise_url
	game_data['also_on'] = alsoon
	game_data['expansion'] = expansion
	game_data['expansion_url'] = expansion_url
	game_data['metacritic_rating'] = metacritic_rating
	game_data['metacritic_reviews'] = metacritic_reviews
	game_data['metacritic_url'] = metacritic_url
	game_data['publisher_us'] = publisher_us
	game_data['release_date_us'] = release_date_us
	game_data['esrb_rating_us'] = esrb_rating_us
	game_data['releases'] = releases
	game_data['release_date'] = release

	game_data_all = {'data': game_data}
	
	return game_data_all


def get_game_stats(data_url):
	# ADD INSTDEV to all
	g_stats = get_new_page(False, data_url)
	
	stats_data = g_stats.find_all('figure', {'class': 'mygames_section bar'})
	if len(stats_data) != 5:
		return {'stats': {}}

	# - Ratings -#
	rating_section = stats_data[0]
	total_ratings = rating_section.find('figcaption').get_text(strip=True)
	num_ratings_tot = int(total_ratings[total_ratings.index(":") + 2 : total_ratings.index("-") - 1])
	
	ratings = {}
	avg_rating = 0
	amazing_pct = 0
	terrible_pct = 0

	tbody = rating_section.find('tbody')
	for i in range(1,11):
		stars = i/2.0
		dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
		if dataspan:
			datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%',''))
		else:
			datapoint = 0.0
		
		conv_rating = round((datapoint/100.0) * stars, 4)

		ratings[str(stars)] = datapoint
		avg_rating += conv_rating

		if i in (9,10):
			amazing_pct += datapoint
		elif i in (1,2):
			terrible_pct += datapoint
	
	avg_rating = round(avg_rating, 4)
	amazing_pct = round(amazing_pct, 4)
	terrible_pct = round(terrible_pct, 4)
	diff_pct = round(amazing_pct - terrible_pct, 4)
	
	ratings['avg'] = avg_rating
	ratings['total'] = num_ratings_tot
	ratings['amazing_pct'] = amazing_pct
	ratings['terrible_pct'] = terrible_pct
	ratings['diff_pct'] = diff_pct

	# - Ownership -#
	own_section = stats_data[1]
	total_owns = own_section.find('figcaption').get_text(strip=True)
	num_owns_tot = int(total_owns[total_owns.index(":") + 2 :])

	ownership = {'num_owners': num_owns_tot}
	
	# - Play -#
	play_section = stats_data[2]
	total_plays = play_section.find('figcaption').get_text(strip=True)
	num_plays_tot = int(total_plays[total_plays.index(":") + 2 : total_plays.index("-") - 1])
	
	plays = {}
	play_complete = 0
	play_incomplete = 0
	play_avg = 0

	tbody = play_section.find('tbody')
	for i in range(1,6):
		dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
		if dataspan:
			datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%',''))
		else:
			datapoint = 0.0

		play_avg += (datapoint * i / 100.0)
		
		if i == 1:
			k = 'once'
		elif i == 2:
			k = 'some'
		elif i == 3:
			k = 'half'
		elif i == 4:
			k = 'finish'
		elif i == 5:
			k = 'complete'

		plays[k] = datapoint

		if i in (1,2,3):
			play_incomplete += datapoint
		elif i in (4,5):
			play_complete += datapoint
	
	plays['total'] = num_plays_tot
	plays['complete_pct'] = round(play_complete, 4)
	plays['incomplete_pct'] = round(play_incomplete, 4)
	plays['avg'] = round(play_avg, 4)

	# - Difficulty - #
	difficulty_section = stats_data[3]
	total_difficulty = difficulty_section.find('figcaption').get_text(strip=True)
	num_difficulty_tot = int(total_difficulty[total_difficulty.index(":") + 2 : total_difficulty.index("-") - 1])
	
	difficulty = {}
	avg_difficulty = 0

	tbody = difficulty_section.find('tbody')
	for i in range(1,6):
		dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
		if dataspan:
			datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%',''))
		else:
			datapoint = 0.0

		avg_difficulty += (datapoint * i / 100.0)
		
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

		difficulty[k] = datapoint

	difficulty['total'] = num_difficulty_tot
	difficulty['avg'] = round(avg_difficulty, 4)
	
	# - Time - #
	
	times_section = stats_data[4]
	total_times = times_section.find('figcaption').get_text(strip=True)
	num_times_tot = int(total_times[total_times.index(":") + 2 : total_times.index("-") - 1])
	
	times = {}
	avg_times = 0

	tbody = times_section.find('tbody')
	for i in range(1,11):
		dataspan = tbody.find('span', {'class': 'mygames_stat{}'.format(i)})
		if dataspan:
			datapoint = float(dataspan.findChild('span').get_text(strip=True).replace('%',''))
		else:
			datapoint = 0.0
		
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
		
		times[str(hours)] = datapoint
		avg_times += (datapoint * hours / 100.0)
	
	times['avg'] = round(avg_times, 4)
	times['total'] = num_times_tot
	
	
	stats = {}
	stats['ratings'] = ratings
	stats['ownership'] = ownership
	stats['plays'] = plays
	stats['difficulty'] = difficulty
	stats['times'] = times

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
		header = g.find('div', {'class': 'head'}).find('h2').text
		for c in cols:
			game_url = base_url + c.find('a').get('href')
			game_name = c.get_text(strip=True)
			cols_list = {
				'game_url': game_url,
				'game_name': game_name
			}
		
			if 'also playing' in header:
				also_playing.append(cols_list)
			elif 'also own' in header:
				also_own.append(cols_list)
			elif 'also love' in header:
				also_love.append(cols_list)
	
	playing = {
		'also': {
			'playing': also_playing,
			'own': also_own,
			'love': also_love
		}
	}
	
	return playing


for s in systems:
	all_games = get_system_pages(s)
	#print all_games
	#TODO merge game data from all systems

