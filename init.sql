-- Criação da tabela de passageiros
CREATE TABLE passengers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(15),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criação da tabela de motoristas
CREATE TABLE drivers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    available BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criação da tabela de corridas
CREATE TABLE rides (
    ride_id UUID PRIMARY KEY,
    driver_id INT NOT NULL,
    passenger_location JSONB NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (driver_id) REFERENCES drivers (id)
);

INSERT INTO drivers (name, latitude, longitude, available) VALUES
('Motorista 1', -23.550520, -46.633308, TRUE),
('Motorista 2', -22.906847, -43.172897, TRUE),
('Motorista 3', -19.916681, -43.934493, TRUE),
('Motorista 4', -15.794229, -47.882166, TRUE),
('Motorista 5', -3.717221, -38.543369, TRUE),
('Motorista 6', -30.034647, -51.217658, TRUE),
('Motorista 7', -25.428356, -49.273251, TRUE),
('Motorista 8', -12.971399, -38.501389, TRUE),
('Motorista 9', -1.455754, -48.490179, TRUE),
('Motorista 10', -16.686891, -49.264794, TRUE);
