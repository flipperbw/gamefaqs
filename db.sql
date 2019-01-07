begin;


create table game (
  id serial not null primary key,
  name varchar(1023) not null,
  url varchar(1023) not null,
  platform varchar(256) not null,
  gamefaqs_uid int not null,
  genre varchar(1023) not null,
  aka varchar(1023),
  wiki varchar(1023),
  franchise_name varchar(1023),
  franchise_url varchar(1023),
  local_players varchar(1023),
  multi_players varchar(1023),
  metacritic_url varchar(1023),
  boxart_thumb varchar(1023),
  boxart_front varchar(1023),
  boxart_all varchar(1023),
  also_on text[], --separate this into the actual id for platform
  -- change this whole thing to reference a specific release
  release_distribution_uid varchar(512),
  release_product_uid varchar(512),
  release_publisher_name varchar(1023),
  release_publisher_url varchar(1023),
  release_date date,
  release_esrb_rating varchar(31), -- change this to ID
  release_title varchar(1023),

  UNIQUE(url),
  UNIQUE(name, platform)
);

create table game_stat (
  id serial not null primary key,
  game_id int not null references game(id) ON DELETE CASCADE,
  owners int,
  metacritic_rating int,
  metacritic_reviews int,
  difficulty_votes int,
  difficulty_avg real,
  difficulty_easy real,
  difficulty_fine real,
  difficulty_moderate real,
  difficulty_hard real,
  difficulty_extreme real,
  progress_votes int,
  progress_avg real,
  progress_pct_complete real,
  progress_pct_incomplete real,
  progress_pct_platinum real,
  progress_pct_finish real,
  progress_pct_half real,
  progress_pct_some real,
  progress_pct_once real,
  rating_votes int,
  rating_avg real,
  rating_half real,
  rating_one real,
  rating_one_half real,
  rating_two real,
  rating_two_half real,
  rating_three real,
  rating_three_half real,
  rating_four real,
  rating_four_half real,
  rating_five real,
  rating_pct_amazing real,
  rating_pct_terrible real,
  rating_pct_diff real, -- no real point to these, can be calced (1+2, 9+10, then diff)
  playtime_votes int,
  playtime_avg real,
  playtime_pct_half_hour real,
  playtime_pct_one_hour real,
  playtime_pct_two_hour real,
  playtime_pct_four_hour real,
  playtime_pct_eight_hour real,
  playtime_pct_twelve_hour real,
  playtime_pct_twenty_hour real,
  playtime_pct_forty_hour real,
  playtime_pct_sixty_hour real,
  playtime_pct_ninety_hour real,

  UNIQUE(game_id)
);

create table esrb_content (
  id serial not null primary key,
  name varchar(255) not null,
  description varchar(2047),

  UNIQUE(name)
);

create table game_esrb_content (
  id serial not null primary key,
  game_id int not null references game(id) ON DELETE CASCADE,
  esrb_content_id int not null references esrb_content(id) ON DELETE CASCADE,

  UNIQUE(game_id, esrb_content_id)
);

create table developer (
  id serial not null primary key,
  name varchar(255) not null,
  url varchar(1023) not null,

  UNIQUE(url),
  UNIQUE(name)
);

create table game_developer (
  id serial not null primary key,
  game_id int not null references game(id) ON DELETE CASCADE,
  developer_id int not null references developer(id) ON DELETE CASCADE,

  UNIQUE(game_id, developer_id)
);

create table game_release (
  id serial not null primary key,
  game_id int not null references game(id) ON DELETE CASCADE,
  title varchar(1023) not null,
  region varchar(31) not null,
  distribution_uid varchar(512),
  product_uid varchar(512),
  publisher_name varchar(1023),
  publisher_url varchar(1023),
  release_date date,
  esrb_rating varchar(31) -- change this to ID
);

create unique index idx_game_release_unique on game_release
  (game_id, region, title, coalesce(distribution_uid, ''), coalesce(product_uid, ''), coalesce(publisher_name, ''), coalesce(publisher_url, ''), coalesce(release_date, ''), coalesce(esrb_rating, ''));

create table game_expansion (
  id serial not null primary key,
  game_id int not null references game(id) ON DELETE CASCADE,
  name varchar(1023) not null,
  url varchar(1023) not null,
  gamefaqs_uid int not null,
  description varchar(2047) not null,
  esrb_rating varchar(31), -- change this to ID
  metacritic_rating int,
  metacritic_reviews int,
  metacritic_url varchar(1023),
  release_date date, --add a new table for this, get the info
  owners int,
  rating_score real, -- new table for these
  rating_votes int,
  completed_pct real,
  completed_votes int,
  difficulty_desc varchar(255),
  difficulty_pct real,
  difficulty_votes int,
  playtime_hours real,
  playtime_votes int,

  UNIQUE(url)
);

create table game_recommendation (
  id serial not null primary key,
  game_id int not null references game(id) ON DELETE CASCADE,
  typ varchar(63) not null, -- own, related, playing, love
  name varchar(1023) not null, --maybe make this a reference
  url varchar(1023) not null,

  UNIQUE(game_id, typ, url)
);

create table game_compilation (
  id serial not null primary key,
  game_id int not null references game(id) ON DELETE CASCADE,
  typ varchar(31) not null, -- within, contains
  name varchar(1023) not null,
  platform varchar(256) not null,
  url varchar(1023) not null,

  UNIQUE(game_id, url)
);


CREATE OR REPLACE FUNCTION update_modified()
RETURNS TRIGGER AS $$
BEGIN
    NEW.date_modified = clock_timestamp();
    NEW.date_created = OLD.date_created;
    return NEW;
END
$$ language plpgsql;


DO $$
DECLARE
    r record;
BEGIN
    FOR r in SELECT table_name from information_schema.tables WHERE table_schema = 'public'
    LOOP
        EXECUTE format('
            ALTER TABLE %I
                ADD COLUMN date_created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
                ADD COLUMN date_modified TIMESTAMP WITH TIME ZONE
        ', r.table_name);

        EXECUTE format('CREATE TRIGGER update_modified_%s BEFORE UPDATE ON %I FOR EACH ROW EXECUTE PROCEDURE update_modified();', r.table_name, r.table_name);
    END LOOP;
END
$$ language plpgsql;


insert into esrb_content (name, description) values
 ('Alcohol Reference', 'Reference to and/or images of alcoholic beverages'),
 ('Animated Blood', 'Discolored and/or unrealistic depictions of blood'),
 ('Blood', 'Depictions of blood'),
 ('Blood and Gore', 'Depictions of blood or the mutilation of body parts'),
 ('Cartoon Violence', 'Violent actions involving cartoon-like situations and characters. May include violence where a character is unharmed after the action has been inflicted'),
 ('Comic Mischief', 'Depictions or dialogue involving slapstick or suggestive humor'),
 ('Crude Humor', 'Depictions or dialogue involving vulgar antics, including “bathroom” humor'),
 ('Drug Reference', 'Reference to and/or images of illegal drugs'),
 ('Edutainment', 'Content of product provides user with specific skills development or reinforcement learning within an entertainment setting. Skill development is an integral part of product'),
 ('Fantasy Violence', 'Violent actions of a fantasy nature, involving human or non-human characters in situations easily distinguishable from real life'),
 ('Informational', 'Overall content of product contains data, facts, resource information, reference materials or instructional text'),
 ('Intense Violence', 'Graphic and realistic-looking depictions of physical conflict. May involve extreme and/or realistic blood, gore, weapons and depictions of human injury and death'),
 ('Language', 'Mild to moderate use of profanity'),
 ('Lyrics', 'Mild references to profanity, sexuality, violence, alcohol or drug use in music'),
 ('Mature Humor', 'Depictions or dialogue involving "adult" humor, including sexual references'),
 ('Mild Violence', 'Mild scenes depicting characters in unsafe and/or violent situations'),
 ('Nudity', 'Graphic or prolonged depictions of nudity'),
 ('Partial Nudity', 'Brief and/or mild depictions of nudity'),
 ('Real Gambling', 'Player can gamble, including betting or wagering real cash or currency'),
 ('Sexual Themes', 'Mild to moderate sexual references and/or depictions. May include partial nudity'),
 ('Sexual Violence', 'Depictions of rape or other violent sexual acts'),
 ('Simulated Gambling', 'Player can gamble without betting or wagering real cash or currency'),
 ('Some Adult Assistance May Be Needed', 'Intended for very young ages'),
 ('Strong Language', 'Explicit and/or frequent use of profanity'),
 ('Strong Lyrics', 'Explicit and/or frequent references to profanity, sex, violence, alcohol or drug use in music'),
 ('Strong Sexual Content', 'Graphic references to and/or depictions of sexual behavior, possibly including nudity'),
 ('Suggestive Themes', 'Mild provocative references or materials'),
 ('Tobacco Reference', 'Reference to and/or images of tobacco products'),
 ('Use of Drugs', 'The consumption or use of illegal drugs'),
 ('Use of Alcohol', 'The consumption of alcoholic beverages'),
 ('Use of Tobacco', 'The consumption of tobacco products'),
 ('Violence', 'Scenes involving aggressive conflict');


commit;