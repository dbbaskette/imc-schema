-- =============================================================================
-- Policies table definition and sample data
-- =============================================================================

-- Create policies table
CREATE TABLE policies (
    policy_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    policy_number VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'EXPIRED', 'CANCELLED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data for policies table
INSERT INTO policies (policy_id, customer_id, policy_number, start_date, end_date, status) VALUES
(200001, 100001, 'IMC-200001', '2023-01-15', '2024-01-14', 'ACTIVE'),
(200002, 100002, 'IMC-200002', '2023-02-20', '2024-02-19', 'ACTIVE'),
(200003, 100003, 'IMC-200003', '2022-11-10', '2023-11-09', 'EXPIRED'),
(200004, 100004, 'IMC-200004', '2023-03-01', '2024-02-29', 'ACTIVE'),
(200005, 100005, 'IMC-200005', '2023-04-05', '2024-04-04', 'ACTIVE'),
(200006, 100006, 'IMC-200006', '2023-05-12', '2024-05-11', 'ACTIVE'),
(200007, 100007, 'IMC-200007', '2023-06-18', '2024-06-17', 'ACTIVE'),
(200008, 100008, 'IMC-200008', '2023-07-22', '2024-07-21', 'ACTIVE'),
(200009, 100009, 'IMC-200009', '2023-08-15', '2024-08-14', 'ACTIVE'),
(200010, 100010, 'IMC-200010', '2023-09-01', '2024-08-31', 'ACTIVE'),
(200011, 100011, 'IMC-200011', '2023-10-07', '2024-10-06', 'ACTIVE'),
(200012, 100012, 'IMC-200012', '2023-11-14', '2024-11-13', 'ACTIVE'),
(200013, 100013, 'IMC-200013', '2022-12-20', '2023-12-19', 'EXPIRED'),
(200014, 100014, 'IMC-200014', '2024-01-08', '2025-01-07', 'ACTIVE'),
(200015, 100015, 'IMC-200015', '2024-02-15', '2025-02-14', 'ACTIVE');