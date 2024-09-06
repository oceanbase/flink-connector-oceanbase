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

CREATE TABLE `htable$family1`
(
  `K` varbinary(1024) NOT NULL,
  `Q` varbinary(256)  NOT NULL,
  `T` bigint(20)      NOT NULL,
  `V` varbinary(1024) DEFAULT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);

CREATE TABLE `htable$family2`
(
  `K` varbinary(1024) NOT NULL,
  `Q` varbinary(256)  NOT NULL,
  `T` bigint(20)      NOT NULL,
  `V` varbinary(1024) DEFAULT NULL,
  PRIMARY KEY (`K`, `Q`, `T`)
);
