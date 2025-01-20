-- Copyright 2024 OceanBase.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  inventory
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our products using a single insert with many rows
CREATE TABLE products
(
  id          INTEGER      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(255) NOT NULL DEFAULT 'flink',
  description VARCHAR(512),
  weight      FLOAT
);
ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default, "scooter", "Small 2-wheel scooter", 3.14),
       (default, "car battery", "12V car battery", 8.1),
       (default, "12-pack drill bits", "12-pack of drill bits with sizes ranging from #40 to #3", 0.8),
       (default, "hammer", "12oz carpenter's hammer", 0.75),
       (default, "hammer", "14oz carpenter's hammer", 0.875),
       (default, "hammer", "16oz carpenter's hammer", 1.0),
       (default, "rocks", "box of assorted rocks", 5.3),
       (default, "jacket", "water resistent black wind breaker", 0.1),
       (default, "spare tire", "24 inch spare tire", 22.2);

-- Create some customers ...
CREATE TABLE customers
(
  id         INTEGER      NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name  VARCHAR(255) NOT NULL,
  email      VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT = 1001;


INSERT INTO customers
VALUES (default, "Sally", "Thomas", "sally.thomas@acme.com"),
       (default, "George", "Bailey", "gbailey@foobar.com"),
       (default, "Edward", "Walker", "ed@walker.com"),
       (default, "Anne", "Kretchmar", "annek@noanswer.org");
