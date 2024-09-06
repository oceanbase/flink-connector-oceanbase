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

CREATE TABLE gis_types
(
  id                   INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  point_c              POINT,
  geometry_c           GEOMETRY,
  linestring_c         LINESTRING,
  polygon_c            POLYGON,
  multipoint_c         MULTIPOINT,
  multiline_c          MULTILINESTRING,
  multipolygon_c       MULTIPOLYGON,
  geometrycollection_c GEOMETRYCOLLECTION
)
