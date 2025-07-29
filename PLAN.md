# Plan: Insurance Company Database Schema

This plan outlines the steps to create a comprehensive Greenplum database schema and sample data for the Insurance MegaCorp demo application, as requested.

### Step 1: Research and Schema Design

-   **Objective:** Define the necessary tables, columns, and relationships for the insurance database.
-   **Actions:**
    1.  Analyze the `imc-telematics-gen` application to understand the existing data structures, particularly the format of `policy_id` (`IMC-AUTO-XXXXX`) and the data captured during an accident (VIN, location, timestamp, g-force).
    2.  Design a relational schema with the following tables:
        -   `customers`: Stores information about the policyholders.
        -   `policies`: Core policy information, linking customers to vehicles and drivers.
        -   `vehicles`: Details of insured vehicles (VIN, make, model).
        -   `drivers`: Information on all named drivers on a policy.
        -   `accidents`: Records of reported accidents, capturing telematics-like data.
        -   `claims`: Tracks insurance claims filed in relation to accidents.
        -   `driver_safety_scores`: A table to hold calculated safety scores for drivers (to be populated later).
    3.  Define primary and foreign key relationships to ensure data integrity.

### Step 2: Write DDL for Schema Creation

-   **Objective:** Create the SQL `CREATE TABLE` statements based on the design.
-   **Actions:**
    1.  Write the DDL for each table using Greenplum/PostgreSQL compatible syntax.
    2.  Use appropriate data types (e.g., `VARCHAR`, `TIMESTAMP`, `NUMERIC`, `SERIAL` for IDs).
    3.  Establish primary keys, foreign keys, and `NOT NULL` constraints.
    4.  Add comments to the SQL script to explain the purpose of each table and key columns.

### Step 3: Generate Realistic Sample Data (DML)

-   **Objective:** Populate the created tables with a rich, interconnected set of sample data.
-   **Actions:**
    1.  Write `INSERT` statements for the `customers`, `policies`, `vehicles`, and `drivers` tables, ensuring `policy_number` matches the `IMC-AUTO-XXXXX` format.
    2.  Create a set of `accidents`, linking them to the policies, vehicles, and drivers.
    3.  Generate `claims` for a subset of the accidents, with varying statuses (`PENDING`, `APPROVED`, `DENIED`).
    4.  **Crucially**, the `driver_safety_scores` table will be created but left empty, as per the request.

### Step 4: Consolidate and Finalize the SQL Script

-   **Objective:** Combine all SQL into a single, executable file.
-   **Actions:**
    1.  Assemble the DDL (`CREATE TABLE`) and DML (`INSERT`) statements into one file named `greenplum_schema.sql`.
    2.  Order the statements correctly to respect foreign key constraints (i.e., create parent tables before child tables, and insert into them in the same order).
    3.  Review the entire script for syntax errors and logical consistency.