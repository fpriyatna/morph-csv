DROP TABLE IF EXISTS MODIFIEDDATES;
CREATE TABLE MODIFIEDDATES (`ID` INT,`MODIFIEDDATES` DATE,PRIMARY KEY (id,modifiedDates));
DROP TABLE IF EXISTS COMMENTS;
CREATE TABLE COMMENTS (`DATE` DATE,`USERNAME` VARCHAR(200),`COMMENT` VARCHAR(200),`MODIFIEDDATES_J` INT,PRIMARY KEY (date,username,comment));
DROP TABLE IF EXISTS PEOPLE;
CREATE TABLE PEOPLE (`NAME` VARCHAR(200),`LN1` VARCHAR(200),`LN2` VARCHAR(200),`CNAME` VARCHAR(200),`NCOURSES` INT DEFAULT 0,PRIMARY KEY (name,ln1,ln2));
DROP TABLE IF EXISTS COUNTRY;
CREATE TABLE COUNTRY (`CNAME` VARCHAR(200),`COFFICIALNAME` VARCHAR(200),PRIMARY KEY (cName));
INSERT INTO MODIFIEDDATES VALUES('0','2018-10-01');
INSERT INTO MODIFIEDDATES VALUES('0','2018-11-01');
INSERT INTO MODIFIEDDATES VALUES('1','2018-10-02');
INSERT INTO MODIFIEDDATES VALUES('1','2018-12-04');
INSERT INTO MODIFIEDDATES VALUES('2','2018-11-30');
INSERT INTO MODIFIEDDATES VALUES('3','2018-11-28');
INSERT INTO MODIFIEDDATES VALUES('3','2019-01-01');
INSERT INTO COMMENTS VALUES('2018-10-01','fpriyatna','Hallo Dunia','0');
INSERT INTO COMMENTS VALUES('2018-10-02','dchaves','Hola Mundo','1');
INSERT INTO COMMENTS VALUES('2018-11-30','fpriyatna','Hello World','2');
INSERT INTO COMMENTS VALUES('2018-11-28','dchaves','Hello World','3');
INSERT INTO PEOPLE VALUES('Freddy','Priyatna','NULL','Indonesia','0');
INSERT INTO PEOPLE VALUES('David','Chaves','Fraga','Spain','3');
INSERT INTO PEOPLE VALUES('Ahmad','Alobaid','NULL','Kuwait','0');
INSERT INTO PEOPLE VALUES('Oscar','Corcho','Garcia','Spain','7');
INSERT INTO COUNTRY VALUES('Indonesia','Republic of Indonesia');
INSERT INTO COUNTRY VALUES('Spain','Kingdom of Spain');
INSERT INTO COUNTRY VALUES('Kuwait','State of Kuwait');

ALTER TABLE  PEOPLE  ADD FOREIGN  KEY (cName) REFERENCES COUNTRY (cName);
ALTER TABLE PEOPLE ADD email VARCHAR(200);
UPDATE PEOPLE SET email=LOWER(CONCAT(SUBSTRING(name,1,1),CONCAT(ln1,'@fi.upm.es')));
CREATE INDEX usernames ON COMMENTS (username);
ALTER TABLE PEOPLE ADD author VARCHAR(200);
UPDATE PEOPLE SET author=CONCAT(SUBSTRING(name,1,1),ln1);
CREATE INDEX authors ON PEOPLE (author);