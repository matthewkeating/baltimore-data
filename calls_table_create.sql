CREATE TABLE calls_for_service (

    id INT AUTO_INCREMENT,
    call_date_time DATETIME,
    priority VARCHAR(15),
    district VARCHAR(2),
    description VARCHAR(30),
    call_number VARCHAR(10),
    incident_location VARCHAR(40),
    latitude VARCHAR(20),
    longitude VARCHAR(20),

    PRIMARY KEY (id)
)
