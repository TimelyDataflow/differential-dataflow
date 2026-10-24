-- The suite's 17 projected relations (snb/data.py SCHEMA).
CREATE TABLE person(id BIGINT, first TEXT, last TEXT, gender TEXT, birthday BIGINT, created BIGINT,
  ip TEXT, browser TEXT, city BIGINT, birthmonth BIGINT, birthdaynum BIGINT, creationmonth BIGINT);
CREATE TABLE message(id BIGINT, kind BIGINT, created BIGINT, creator BIGINT, country BIGINT, content TEXT,
  image TEXT, length BIGINT, language TEXT, forum BIGINT, parent BIGINT, year BIGINT, month BIGINT, day BIGINT);
CREATE TABLE forum(id BIGINT, title TEXT, created BIGINT, moderator BIGINT);
CREATE TABLE knows(src BIGINT, dst BIGINT, created BIGINT);
CREATE TABLE member(forum BIGINT, person BIGINT, created BIGINT);
CREATE TABLE tag(id BIGINT, name TEXT, class BIGINT);
CREATE TABLE tagclass(id BIGINT, name TEXT, parent BIGINT);
CREATE TABLE place(id BIGINT, name TEXT, type TEXT, parent BIGINT);
CREATE TABLE org(id BIGINT, name TEXT, type TEXT, place BIGINT);
CREATE TABLE interest(person BIGINT, tag BIGINT);
CREATE TABLE mtag(message BIGINT, tag BIGINT);
CREATE TABLE ftag(forum BIGINT, tag BIGINT);
CREATE TABLE likes(person BIGINT, message BIGINT, created BIGINT);
CREATE TABLE study(person BIGINT, org BIGINT, year BIGINT);
CREATE TABLE work(person BIGINT, org BIGINT, year BIGINT);
CREATE TABLE email(person BIGINT, value TEXT);
CREATE TABLE language(person BIGINT, value TEXT);
