import inflect
import warnings

from sqlalchemy.ext.automap import automap_base, generate_relationship
from sqlalchemy.orm import Session, relationship, configure_mappers, interfaces
from sqlalchemy import (
    create_engine,
    exc as sa_exc
)


Base = automap_base()


class Game_Developer(Base):
    __tablename__ = 'game_developer'


class Game_Esrb_Content(Base):
    __tablename__ = 'game_esrb_content'


class Game(Base):
    __tablename__ = 'game'

    developers = relationship("developer", secondary='game_developer', lazy='dynamic', backref="games")
    esrb_contents = relationship("esrb_content", secondary="game_esrb_content", lazy='dynamic', backref="games")


engine = create_engine("postgresql://brett:@localhost/gamefaqs")

_pluralizer = inflect.engine()


# noinspection PyUnusedLocal
def pluralize_collection(base, local_cls, referred_cls, constraint):
    return _pluralizer.plural(referred_cls.__name__).lower()


def _gen_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw):
    if direction not in (interfaces.MANYTOONE,):
        kw['lazy'] = 'dynamic'
    return generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw)


with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=sa_exc.SAWarning)
    Base.prepare(engine, reflect=True, name_for_collection_relationship=pluralize_collection, generate_relationship=_gen_relationship)

configure_mappers()

session = Session(engine, autoflush=False)

classes = Base.classes

Developer = classes.get('developer')
Esrb_Content = classes.get('esrb_content')
Game_Expansion = classes.get('game_expansion')
Game_Stat = classes.get('game_stat')
Game_Release = classes.get('game_release')
Game_Recommendation = classes.get('game_recommendation')
Game_Compilation = classes.get('game_compilation')
