-- Sample data for accidents table
-- This data corresponds to the new policies, vehicles, and drivers in Georgia.

INSERT INTO accidents (accident_id, policy_id, vehicle_id, driver_id, accident_timestamp, latitude, longitude, g_force, description)
VALUES
-- Accident 1: Liam Smith in Atlanta
(500001, 200001, 300001, 400001, '2023-10-15 08:45:00-04', 33.7537, -84.3863, 3.1, 'Rear-ended at a stoplight on Peachtree St.'),

-- Accident 2: Emma Brown in Macon
(500002, 200004, 300005, 400004, '2023-11-20 17:30:00-05', 32.8407, -83.6324, 1.9, 'Minor collision in a parking lot on Cherry St.'),

-- Accident 3: Mateo Martinez (addtl driver) in Macon
(500003, 200010, 300011, 400012, '2023-12-01 12:10:00-05', 32.8353, -83.6362, 4.5, 'Side-impact collision at intersection of Mulberry and 2nd St.'),

-- Accident 4: Evelyn Moore in Marietta
(500004, 200018, 300021, 400021, '2024-01-10 18:05:00-05', 33.9526, -84.5499, 2.2, 'Lost traction on wet road, hit curb on Powers Ferry Rd.'),

-- Accident 5: Daniel Harris in Atlanta
(500005, 200025, 300028, 400029, '2024-02-05 07:50:00-05', 33.8224, -84.3712, 5.8, 'High-speed collision on GA-400 North.'),

-- Accident 6: Zoey Phillips in Alpharetta
(500006, 200050, 300056, 400058, '2024-02-28 14:00:00-05', 34.0754, -84.2941, 7.2, 'Rollover accident on Windward Pkwy.'),

-- Accident 7: William Rodriguez in Savannah
(500007, 200009, 300010, 400010, '2024-03-02 11:25:00-05', 32.0809, -81.0912, 1.2, 'Scraped against a pole in a historic district parking garage.'),

-- Accident 8: Jack Moore (addtl driver) in Marietta
(500008, 200018, 300020, 400022, '2024-03-10 20:15:00-04', 33.9580, -84.5510, 3.5, 'T-bone collision at a four-way stop.'),

-- Accident 9: Liam Smith (second incident)
(500009, 200001, 300001, 400001, '2024-03-12 09:00:00-04', 33.7629, -84.4226, 0.8, 'Backed into a parked car at low speed.'),

-- Accident 10: John Campbell in Martinez
(500010, 200047, 300053, 400054, '2024-03-15 16:50:00-04', 33.5126, -82.0782, 4.1, 'Failed to yield, causing a collision on Columbia Rd.');