CREATE SCHEMA IF NOT EXISTS varasto;

CREATE TABLE IF NOT EXISTS varasto.asiakas (
id SERIAL PRIMARY KEY,
nimi VARCHAR(50) NOT NULL CHECK(nimi <> ''),
osoite VARCHAR(150) NOT NULL CHECK(osoite <> '')
);

CREATE TABLE IF NOT EXISTS varasto.lasku (
id SERIAL PRIMARY KEY,
asiakasId BIGINT UNSIGNED NOT NULL,
tila INTEGER NOT NULL, -- 0 = keskeneräinen, 1 = valmis, 2 = lähetetty, 3 = maksettu
erapaiva DATE,
ykkososa BIGINT UNSIGNED NOT NULL,
edellinenlasku BIGINT UNSIGNED NOT NULL,
FOREIGN KEY (asiakasId) REFERENCES asiakas (id)
);

CREATE TABLE IF NOT EXISTS varasto.tyokohde (
id SERIAL PRIMARY KEY,
nimi VARCHAR(100) NOT NULL CHECK (nimi <> ''),
osoite VARCHAR(100) NOT NULL CHECK (osoite <> ''),
asiakasid BIGINT UNSIGNED NOT NULL,
FOREIGN KEY (asiakasid) REFERENCES asiakas (id)
);

CREATE TABLE IF NOT EXISTS varasto.suoritus (
id SERIAL PRIMARY KEY,
tyyppi INT NOT NULL,
urakkahinta NUMERIC(65,2),
laskuId BIGINT UNSIGNED NOT NULL,
kohdeId BIGINT UNSIGNED NOT NULL,
FOREIGN KEY (laskuId) REFERENCES lasku (id),
FOREIGN KEY (kohdeId) REFERENCES tyokohde (id)
);

CREATE TABLE IF NOT EXISTS varasto.varastotarvike (
id SERIAL PRIMARY KEY,
nimi VARCHAR(100) NOT NULL CHECK(nimi <> ''),
varastosaldo INT NOT NULL,
yksikko VARCHAR(10) NOT NULL CHECK(yksikko <> ''),
sisaanostohinta NUMERIC(65,2) NOT NULL,
alv NUMERIC(65,2) NOT NULL,
poistettu BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS varasto.kaytettytarvike (
lukumaara INT CHECK (lukumaara > 0),
alennus NUMERIC(65,2),
suoritusId BIGINT UNSIGNED NOT NULL,
varastotarvikeId BIGINT UNSIGNED NOT NULL,
FOREIGN KEY (suoritusId) REFERENCES suoritus (id),
FOREIGN KEY (varastotarvikeId) REFERENCES varastotarvike (id),
CONSTRAINT tarvike_pk PRIMARY KEY (suoritusId, varastotarvikeId)
);

CREATE TABLE IF NOT EXISTS varasto.tyotyyppi (
id SERIAL PRIMARY KEY,
nimi VARCHAR(20) NOT NULL CHECK (nimi <> ''),
hinta NUMERIC (65,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS varasto.tyotunnit (
tyotyyppiId BIGINT UNSIGNED NOT NULL,
tuntimaara INT NOT NULL,
alennus NUMERIC(65,2),
suoritusId BIGINT UNSIGNED NOT NULL,
FOREIGN KEY (suoritusId) REFERENCES suoritus (id),
FOREIGN KEY (tyotyyppiId) REFERENCES tyotyyppi (id),
CONSTRAINT tyotunnit_pk PRIMARY KEY (suoritusId, tyotyyppiId)
);