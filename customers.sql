-- =============================================================================
-- Customers table definition and sample data
-- =============================================================================

-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data for customers table
INSERT INTO customers (customer_id, first_name, last_name, email, phone_number, address, city, state, zip_code) VALUES
(100001, 'Sarah', 'Chen', 'sarah.chen@email.com', '555-0101', '789 Maple Drive', 'Springfield', 'IL', '62704'),
(100002, 'Emily', 'Carter', 'emily.carter@email.com', '555-0102', '456 Oak Ave', 'Someplace', 'TX', '67890'),
(100003, 'Benjamin', 'Rivera', 'benjamin.rivera@email.com', '555-0103', '789 Pine Ln', 'Elsewhere', 'FL', '13579'),
(100004, 'Michael', 'Harris', 'michael.harris@email.com', '555-0104', '452 Oakwood Ave', 'Columbus', 'OH', '43215'),
(100005, 'David', 'Lee', 'david.lee@email.com', '555-0105', '123 Main St', 'Anytown', 'CA', '12345'),
(100006, 'Jessica', 'Thompson', 'jessica.thompson@email.com', '555-0106', '901 Cedar St', 'Austin', 'TX', '73301'),
(100007, 'Andrew', 'Martinez', 'andrew.martinez@email.com', '555-0107', '567 Birch Ave', 'Denver', 'CO', '80202'),
(100008, 'Ashley', 'Wilson', 'ashley.wilson@email.com', '555-0108', '234 Elm Dr', 'Phoenix', 'AZ', '85001'),
(100009, 'Christopher', 'Garcia', 'christopher.garcia@email.com', '555-0109', '678 Walnut Blvd', 'Seattle', 'WA', '98101'),
(100010, 'Amanda', 'Rodriguez', 'amanda.rodriguez@email.com', '555-0110', '345 Cherry Ln', 'Miami', 'FL', '33101'),
(100011, 'Daniel', 'Johnson', 'daniel.johnson@email.com', '555-0111', '890 Spruce Way', 'Portland', 'OR', '97201'),
(100012, 'Lauren', 'Brown', 'lauren.brown@email.com', '555-0112', '123 Aspen Ct', 'Nashville', 'TN', '37201'),
(100013, 'Matthew', 'Davis', 'matthew.davis@email.com', '555-0113', '456 Poplar St', 'Atlanta', 'GA', '30301'),
(100014, 'Stephanie', 'Miller', 'stephanie.miller@email.com', '555-0114', '789 Hickory Ave', 'Boston', 'MA', '02101'),
(100015, 'Ryan', 'Anderson', 'ryan.anderson@email.com', '555-0115', '012 Magnolia Dr', 'San Diego', 'CA', '92101');