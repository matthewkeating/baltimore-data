CREATE TABLE victim_based_crime (
    
    id INT AUTO_INCREMENT,
    crime_date_time DATETIME,
    crime_code VARCHAR(12),
    street VARCHAR(50),
    description VARCHAR(50),
    weapon VARCHAR(50),
    post VARCHAR(12),
    district VARCHAR(50),
    neighborhood VARCHAR(50),
    geo GEOGRAPHYPOINT,
    latitude VARCHAR(20),
    longitude VARCHAR(20),
    num_incidents SMALLINT,

    PRIMARY KEY (id)
)
