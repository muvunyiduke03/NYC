-- MySQL dump 10.13  Distrib 8.0.43, for Linux (x86_64)
--
-- Host: localhost    Database: nyc_trip
-- ------------------------------------------------------
-- Server version	8.0.43-0ubuntu0.24.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `excluded_records`
--

DROP TABLE IF EXISTS `excluded_records`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `excluded_records` (
  `id` int NOT NULL AUTO_INCREMENT,
  `original_index` int NOT NULL,
  `exclusion_reason` varchar(100) NOT NULL,
  `exclusion_timestamp` datetime NOT NULL,
  `details` json DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_reason` (`exclusion_reason`),
  KEY `idx_timestamp` (`exclusion_timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Log of excluded records during data cleaning';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `excluded_records`
--

LOCK TABLES `excluded_records` WRITE;
/*!40000 ALTER TABLE `excluded_records` DISABLE KEYS */;
/*!40000 ALTER TABLE `excluded_records` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `spatial_grid_cells`
--

DROP TABLE IF EXISTS `spatial_grid_cells`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `spatial_grid_cells` (
  `cell_x` int NOT NULL,
  `cell_y` int NOT NULL,
  `lat_min` decimal(10,8) NOT NULL,
  `lat_max` decimal(10,8) NOT NULL,
  `lon_min` decimal(11,8) NOT NULL,
  `lon_max` decimal(11,8) NOT NULL,
  `total_passengers` int DEFAULT '0',
  `avg_trip_duration` decimal(10,2) DEFAULT NULL,
  `avg_trip_distance` decimal(8,3) DEFAULT NULL,
  `peak_hour` tinyint DEFAULT NULL COMMENT 'Hour with most activity',
  `weekend_ratio` decimal(5,4) DEFAULT NULL COMMENT 'Ratio of weekend trips',
  PRIMARY KEY (`cell_x`,`cell_y`),
  KEY `idx_lat_range` (`lat_min`,`lat_max`),
  KEY `idx_lon_range` (`lon_min`,`lon_max`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Aggregated statistics per spatial grid cell';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `spatial_grid_cells`
--

LOCK TABLES `spatial_grid_cells` WRITE;
/*!40000 ALTER TABLE `spatial_grid_cells` DISABLE KEYS */;
/*!40000 ALTER TABLE `spatial_grid_cells` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `trips`
--

DROP TABLE IF EXISTS `trips`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `trips` (
  `id` varchar(50) NOT NULL,
  `vendor_id` int NOT NULL,
  `pickup_datetime` datetime NOT NULL,
  `dropoff_datetime` datetime NOT NULL,
  `hour_of_day` tinyint NOT NULL,
  `day_of_week` tinyint NOT NULL,
  `is_weekend` tinyint(1) NOT NULL,
  `month` tinyint NOT NULL,
  `passenger_count` tinyint NOT NULL,
  `pickup_latitude` decimal(10,8) NOT NULL,
  `pickup_longitude` decimal(11,8) NOT NULL,
  `dropoff_latitude` decimal(10,8) NOT NULL,
  `dropoff_longitude` decimal(11,8) NOT NULL,
  `trip_duration` int NOT NULL COMMENT 'Duration in seconds',
  `calculated_duration` int DEFAULT NULL COMMENT 'Calculated duration in seconds',
  `trip_distance_km` decimal(8,3) NOT NULL COMMENT 'Haversine distance',
  `trip_speed_kmh` decimal(6,2) NOT NULL COMMENT 'Average speed',
  `distance_category` enum('short','medium','long','very_long') NOT NULL,
  `expected_duration_min` decimal(8,2) DEFAULT NULL COMMENT 'Expected duration based on 20 km/h',
  `actual_duration_min` decimal(8,2) DEFAULT NULL COMMENT 'Actual duration in minutes',
  `efficiency_ratio` decimal(6,4) DEFAULT NULL COMMENT 'Expected vs actual ratio',
  `store_and_fwd_flag` char(1) DEFAULT 'N',
  PRIMARY KEY (`id`),
  KEY `idx_pickup_datetime` (`pickup_datetime`),
  KEY `idx_hour_of_day` (`hour_of_day`),
  KEY `idx_passenger_count` (`passenger_count`),
  KEY `idx_distance_category` (`distance_category`),
  KEY `idx_vendor_datetime` (`vendor_id`,`pickup_datetime`),
  KEY `idx_pickup_coords` (`pickup_latitude`,`pickup_longitude`),
  KEY `idx_dropoff_coords` (`dropoff_latitude`,`dropoff_longitude`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Cleaned NYC taxi trip data with derived features';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `trips`
--

LOCK TABLES `trips` WRITE;
/*!40000 ALTER TABLE `trips` DISABLE KEYS */;
/*!40000 ALTER TABLE `trips` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-10-14 20:31:45