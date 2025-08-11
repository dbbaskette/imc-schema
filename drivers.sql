-- =============================================================================
-- Drivers table definition and sample data
-- =============================================================================

-- Create drivers table
CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    policy_id INTEGER NOT NULL REFERENCES policies(policy_id),
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    date_of_birth DATE NOT NULL,
    license_number VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data for drivers table
-- This data corresponds to the 50 policies for customers in Georgia.
-- Includes primary drivers and additional drivers on some policies.

INSERT INTO drivers (driver_id, policy_id, first_name, last_name, date_of_birth, license_number) VALUES
(400001, 200001, 'Sarah', 'Chen', '1990-05-15', 'C12345678'),
(400002, 200002, 'Emily', 'Carter', '1988-11-22', 'C23456789'),
(400003, 200003, 'Benjamin', 'Rivera', '1995-02-10', 'R34567890'),
(400004, 200004, 'Michael', 'Harris', '1992-08-30', 'H45678901'),
(400005, 200004, 'Jessica', 'Harris', '1991-07-25', 'H45678902'), -- Additional Driver (spouse)
(400006, 200005, 'David', 'Lee', '1985-12-01', 'L56789012'),
(400007, 200006, 'Jessica', 'Thompson', '2000-01-20', 'T67890123'),
(400008, 200007, 'Andrew', 'Martinez', '1978-06-05', 'M78901234'),
(400009, 200008, 'Ashley', 'Wilson', '1998-09-18', 'W89012345'),
(400010, 200009, 'Christopher', 'Garcia', '1982-03-12', 'G90123456'),
(400011, 200010, 'Amanda', 'Rodriguez', '1999-04-08', 'R01234567'),
(400012, 200010, 'Carlos', 'Rodriguez', '1998-10-14', 'R01234568'), -- Additional Driver (spouse)
(400013, 200011, 'Daniel', 'Johnson', '1991-07-19', 'J12345679'),
(400014, 200012, 'Lauren', 'Brown', '1993-10-28', 'B23456780'),
(400015, 200013, 'Matthew', 'Davis', '1989-01-03', 'D34567891'),
(400016, 200014, 'Stephanie', 'Miller', '1996-05-21', 'M45678902'),
(400017, 200015, 'Ryan', 'Anderson', '1994-02-14', 'A56789013'),
(400018, 200015, 'Sarah', 'Anderson', '1995-03-16', 'A56789014'), -- Additional Driver (spouse)
(400019, 200016, 'Mia', 'Thomas', '2001-06-09', 'T67890124'),
(400020, 200017, 'Henry', 'Taylor', '1980-08-17', 'T78901235'),
(400021, 200018, 'Evelyn', 'Moore', '1997-11-02', 'M89012346'),
(400022, 200018, 'Jack', 'Moore', '1996-09-01', 'M89012347'), -- Additional Driver
(400023, 200019, 'Alexander', 'Jackson', '1984-04-25', 'J90123457'),
(400024, 200020, 'Harper', 'Martin', '2002-07-30', 'M01234568'),
(400025, 200021, 'Michael', 'Lee', '1976-09-03', 'L12345679'),
(400026, 200022, 'Abigail', 'Perez', '1999-12-12', 'P23456780'),
(400027, 200023, 'Ethan', 'Thompson', '1990-02-28', 'T34567891'),
(400028, 200024, 'Emily', 'White', '1995-08-08', 'W45678902'),
(400029, 200025, 'Daniel', 'Harris', '1987-05-14', 'H56789013'),
(400030, 200025, 'Chloe', 'Harris', '1988-06-11', 'H56789014'), -- Additional Driver
(400031, 200026, 'Elizabeth', 'Sanchez', '1994-10-01', 'S67890124'),
(400032, 200027, 'Matthew', 'Clark', '1983-01-11', 'C78901235'),
(400033, 200028, 'Mila', 'Ramirez', '2000-03-03', 'R89012346'),
(400034, 200029, 'Aiden', 'Lewis', '1998-06-20', 'L90123457'),
(400035, 200030, 'Ella', 'Robinson', '1996-07-07', 'R01234568'),
(400036, 200031, 'Joseph', 'Walker', '1979-11-16', 'W12345679'),
(400037, 200032, 'Avery', 'Young', '1992-09-23', 'Y23456780'),
(400038, 200033, 'David', 'Allen', '1981-02-02', 'A34567891'),
(400039, 200034, 'Sofia', 'King', '1997-04-19', 'K45678902'),
(400040, 200035, 'Jackson', 'Wright', '1993-03-25', 'W56789013'),
(400041, 200035, 'Lily', 'Wright', '1994-04-26', 'W56789014'), -- Additional Driver
(400042, 200036, 'Scarlett', 'Scott', '1999-01-01', 'S67890124'),
(400043, 200037, 'Samuel', 'Torres', '1986-10-10', 'T78901235'),
(400044, 200038, 'Victoria', 'Nguyen', '1991-06-15', 'N89012346'),
(400045, 200039, 'Sebastian', 'Hill', '1995-09-05', 'H90123457'),
(400046, 200040, 'Grace', 'Flores', '1998-12-24', 'F01234568'),
(400047, 200041, 'Carter', 'Green', '1988-07-14', 'G12345679'),
(400048, 200042, 'Chloe', 'Adams', '1996-02-07', 'A23456780'),
(400049, 200043, 'Wyatt', 'Nelson', '1990-11-30', 'N34567891'),
(400050, 200044, 'Penelope', 'Baker', '1993-08-21', 'B45678902'),
(400051, 200044, 'Leo', 'Baker', '1992-07-20', 'B45678903'), -- Additional Driver
(400052, 200045, 'Jayden', 'Hall', '2003-05-05', 'H56789013'),
(400053, 200046, 'Layla', 'Rivera', '2001-10-18', 'R67890124'),
(400054, 200047, 'John', 'Campbell', '1975-04-01', 'C78901235'),
(400055, 200047, 'Susan', 'Campbell', '1977-05-02', 'C78901236'), -- Additional Driver
(400056, 200048, 'Riley', 'Mitchell', '1994-06-06', 'M89012346'),
(400057, 200049, 'Lincoln', 'Carter', '1989-09-09', 'C90123457'),
(400058, 200050, 'Zoey', 'Phillips', '1997-07-17', 'P01234568'),
(400059, 200050, 'Zane', 'Phillips', '1996-06-16', 'P01234569'); -- Additional Driver