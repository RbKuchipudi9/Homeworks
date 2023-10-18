CREATE TABLE animaldim (
    animalkey INT PRIMARY KEY,
    animal_id VARCHAR, 
    animal_name VARCHAR,
    dofb DATE,
    animal_type VARCHAR,
    breed VARCHAR,
    color VARCHAR,
    repro_status VARCHAR,
    gender VARCHAR
);

CREATE TABLE timedim (
    timekey INT PRIMARY KEY,
    date_recorded TIMESTAMP,
    month VARCHAR,
    year INT
);

CREATE TABLE outcomedim (
    outcomekey INT PRIMARY KEY, 
    outcome_type VARCHAR,
    outcome_subtype VARCHAR
);

CREATE TABLE animalfact (
    main_pk SERIAL PRIMARY KEY,
    age VARCHAR,
    animalkey INT REFERENCES animaldim(animalkey),
    outcomekey INT REFERENCES outcomedim(outcomekey),
    timekey INT REFERENCES timedim(timekey)
);
